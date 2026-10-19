"""Deployment settings shared by every flow: run in the toolbox image on the stack network."""
import os

WORK_POOL = "aws-ecs" if os.environ.get("LOADER_PLATFORM") == "aws" else "local-docker"
IMAGE = os.environ.get("LOADER_TOOLBOX_IMAGE", "lakehouse-loader/toolbox:dev")  # ECR on AWS
NETWORK = "lakehouse_lake"
STATE_VOLUME = "lakehouse_state"
# Passed to every flow-run container; secrets come from the same .env the stack uses.
ENV_KEYS = ("PREFECT_API_AUTH_STRING", "LOADER_FETCH_ALLOW_PRIVATE", "POLARIS_URI", "POLARIS_MANAGEMENT_URI", "S3_ENDPOINT", "DYNAMODB_ENDPOINT",
            "LOADER_STATE_DIR", "PREFECT_LOGGING_EXTRA_LOGGERS",
            # Encryption at Bronze needs the master key (KMS on AWS) and the identity pepper.
            "LOADER_MASTER_KEY", "LOADER_IDENTITY_PEPPER",
            # The Gold flow refreshes the Doris copies as serving_refresher, never root.
            "DORIS_HOST", "DORIS_REFRESHER_PASSWORD",
            # The maintenance flow drives Spark Connect.
            "SPARK_REMOTE")


def job_variables() -> dict:
    if os.environ.get("LOADER_PLATFORM") == "aws":
        # Each flow run is a toolbox task: its task definition already carries the environment,
        # secrets and task role; only the network placement is given here.
        return {
            "cluster": os.environ["LOADER_ECS_CLUSTER"],
            "task_definition_arn": os.environ["LOADER_TOOLBOX_TASK_FAMILY"],
            "container_name": "toolbox",
            "launch_type": "FARGATE",
            "vpc_id": os.environ["LOADER_VPC_ID"],  # the ECS worker requires it with network_configuration
            "network_configuration": {"subnets": os.environ["LOADER_SUBNETS"].split(","),
                                      "securityGroups": [os.environ["LOADER_SECURITY_GROUP"]],
                                      "assignPublicIp": "DISABLED"},
            "stream_output": False,
        }
    return {
        "image": IMAGE,
        "image_pull_policy": "Never",
        "networks": [NETWORK],
        "env": {k: v for k, v in os.environ.items() if k in ENV_KEYS or k.startswith("LOADER_SOURCE_SECRET_")},
        "volumes": [f"{STATE_VOLUME}:/state"],
        "auto_remove": True,
    }


def paused() -> bool:
    """CI registers schedules paused (LOADER_PAUSE_SCHEDULES=1): tests trigger deployments
    themselves, and a scheduled run must not touch a test's tables between its steps."""
    return os.environ.get("LOADER_PAUSE_SCHEDULES", "0") == "1"


def deploy(flow, name: str) -> str:
    """Register `flow` on this platform's work pool; returns the deployment id."""
    return str(flow.deploy(name=name, work_pool_name=WORK_POOL, image=IMAGE, build=False, push=False,
                           job_variables=job_variables(), print_next_steps=False))


def deploy_sources(contracts_dir: str = "contracts") -> list[str]:
    """One `ingest/<source>` deployment per contract, polling on the contract's interval. A
    concurrency limit of 1 keeps one run per source at a time (one writer per table)."""
    from datetime import timedelta
    from pathlib import Path

    from flows.ingest import ingest
    from loader import contracts

    ids = []
    for contract in contracts.load_all(Path(contracts_dir)).values():
        ids.append(str(ingest.deploy(
            name=contract.source, work_pool_name=WORK_POOL, image=IMAGE, build=False, push=False,
            job_variables=job_variables(), parameters={"source": contract.source},
            interval=timedelta(seconds=contract.poll_seconds), concurrency_limit=1, paused=paused(),
            tags=[f"source:{contract.source}"], print_next_steps=False)))
    return ids


def deploy_gold(every_seconds: int = 600) -> str:
    """`gold/refresh`: rebuild Gold and the Doris copies on a schedule, one run at a time."""
    from datetime import timedelta

    from flows.gold import refresh

    return str(refresh.deploy(name="refresh", work_pool_name=WORK_POOL, image=IMAGE, build=False, push=False,
                              job_variables=job_variables(), interval=timedelta(seconds=every_seconds),
                              concurrency_limit=1, paused=paused(), print_next_steps=False))


def deploy_maintenance(every_seconds: int = 1800) -> str:
    """`maintenance/policies`: apply every table's maintenance policies, one run at a time."""
    from datetime import timedelta

    from flows.maintenance import maintain

    return str(maintain.deploy(name="policies", work_pool_name=WORK_POOL, image=IMAGE, build=False, push=False,
                               job_variables=job_variables(), interval=timedelta(seconds=every_seconds),
                               concurrency_limit=1, paused=paused(), print_next_steps=False))
