"""Deploy, test and destroy the AWS sandbox. Run through make:

    make sandbox-plan      terraform plan (read-only), saved to infra/sandbox/tfplan
    make sandbox-apply     apply that plan, push images, initialize, start services, bootstrap
    scripts/sandbox.py initialize   redo everything after Terraform (resumes a failed apply)
    scripts/sandbox.py push         rebuild and push the images only
    scripts/sandbox.py schedule-key-deletion <partner> [--days 30]   after an approved delete (operator only)
    scripts/sandbox.py cancel-key-deletion <partner>                 within the window
    make sandbox-test      run the suites as an ECS task in the VPC; logs stream here
    make sandbox-destroy   scale down, terraform destroy, then list anything left behind; first
                           schedule-key-deletion for each partner (only that role may delete their keys)
    scripts/sandbox.py run <command...>   run one command as a toolbox task (logs stream here)
    scripts/sandbox.py bench <scenario> [--max N]   a bench/run.py scenario, each step a toolbox task

Uses the AWS CLI profile in AWS_PROFILE, or `profile` in infra/sandbox/terraform.tfvars (copy
terraform.tfvars.example). Terraform refuses any account but
the sandbox (allowed_account_ids). Synthetic data only.
"""
import base64
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[1]
TF = ROOT / "infra" / "sandbox"


def _profile() -> str:
    """AWS_PROFILE, else `profile` from terraform.tfvars."""
    if os.environ.get("AWS_PROFILE"):
        return os.environ["AWS_PROFILE"]
    tfvars = TF / "terraform.tfvars"
    for line in tfvars.read_text().splitlines() if tfvars.exists() else []:
        key, _, value = line.partition("=")
        if key.strip() == "profile":
            return value.strip().strip('"')
    sys.exit("set AWS_PROFILE, or `profile` in infra/sandbox/terraform.tfvars (see terraform.tfvars.example)")


PROFILE = os.environ.setdefault("AWS_PROFILE", _profile())
TAG = "sandbox"
AWS_SUITES = ["tests/conformance", "tests/access", "tests/unit", "tests/identity", "tests/pipeline",
              "tests/serving", "tests/maintenance", "tests/smoke", "tests/aws"]


def sh(*cmd: str, cwd: Path = ROOT, capture: bool = False) -> str:
    print("+", " ".join(cmd), flush=True)
    result = subprocess.run(cmd, cwd=cwd, check=True, text=True, capture_output=capture)
    return result.stdout if capture else ""


def outputs() -> dict:
    raw = json.loads(sh("terraform", "output", "-json", cwd=TF, capture=True))
    return {k: v["value"] for k, v in raw.items()}


def session():
    return boto3.Session(profile_name=PROFILE, region_name="us-east-1")


# ---------------------------------------------------------------- plan / apply

def plan() -> None:
    sh("terraform", "init", "-input=false", cwd=TF)
    sh("terraform", "plan", "-input=false", "-out=tfplan", cwd=TF)


def push_images(out: dict) -> None:
    ecr = session().client("ecr")
    token = ecr.get_authorization_token()["authorizationData"][0]
    user, password = base64.b64decode(token["authorizationToken"]).decode().split(":", 1)
    subprocess.run(["docker", "login", "--username", user, "--password-stdin", token["proxyEndpoint"]],
                   input=password, text=True, check=True, capture_output=True)
    images = {"toolbox": ("stack/toolbox/Dockerfile", "."), "prefect-worker": ("stack/prefect-worker-ecs/Dockerfile", "stack/prefect-worker-ecs")}
    for name, (dockerfile, context) in images.items():
        uri = f"{out['ecr'][name]}:{TAG}"
        sh("docker", "build", "--platform", "linux/arm64", "-f", dockerfile, "-t", uri, context)
        sh("docker", "push", uri)


def run_task(out: dict, family_arn: str, command: list[str] | None = None, container: str | None = None,
             timeout: int = 3600, stream: bool = True, lines: list[str] | None = None) -> int:
    """Run a one-off Fargate task in the private subnets; stream its logs (also into `lines`);
    return its exit code (137 when ECS stopped it for memory)."""
    ecs, logs = session().client("ecs"), session().client("logs")
    overrides = {"containerOverrides": [{"name": container, "command": command}]} if command else {}
    task = ecs.run_task(cluster=out["cluster"], taskDefinition=family_arn, launchType="FARGATE",
                        networkConfiguration={"awsvpcConfiguration": {
                            "subnets": out["subnets"], "securityGroups": [out["security_group"]],
                            "assignPublicIp": "DISABLED"}}, overrides=overrides)["tasks"][0]
    arn, task_id = task["taskArn"], task["taskArn"].rsplit("/", 1)[1]
    name = container or task["containers"][0]["name"]
    stream_name, token, deadline = f"{name}/{name}/{task_id}", None, time.time() + timeout
    print(f"task {task_id} ({name}) started", flush=True)
    while True:
        desc = ecs.describe_tasks(cluster=out["cluster"], tasks=[arn])["tasks"][0]
        if desc["lastStatus"] == "STOPPED":
            time.sleep(5)  # the last log lines arrive shortly after the container exits
        if stream:
            try:
                kwargs = {"logGroupName": out["log_group"], "logStreamName": stream_name, "startFromHead": True}
                if token:
                    kwargs["nextToken"] = token
                page = logs.get_log_events(**kwargs)
                for event in page["events"]:
                    print(f"  | {event['message']}", flush=True)
                    if lines is not None:
                        lines.append(event["message"])
                token = page["nextForwardToken"]
            except logs.exceptions.ResourceNotFoundException:
                pass
        if desc["lastStatus"] == "STOPPED":
            code = desc["containers"][0].get("exitCode")
            reason = f"{desc.get('stoppedReason', '')} {desc['containers'][0].get('reason', '')}"
            print(f"task {task_id} stopped: exit {code} ({reason.strip()})", flush=True)
            if "OutOfMemory" in reason:
                return 137
            return 1 if code is None else code
        if time.time() > deadline:
            ecs.stop_task(cluster=out["cluster"], task=arn, reason="sandbox.py timeout")
            raise TimeoutError(f"task {task_id} did not finish in {timeout} s")
        time.sleep(10)


def scale(out: dict, service: str, count: int, wait: bool = True) -> None:
    ecs = session().client("ecs")
    ecs.update_service(cluster=out["cluster"], service=service, desiredCount=count)
    if wait:
        print(f"waiting for {service} to be stable at {count}", flush=True)
        ecs.get_waiter("services_stable").wait(cluster=out["cluster"], services=[service],
                                               WaiterConfig={"Delay": 15, "MaxAttempts": 60})


def apply() -> None:
    if not (TF / "tfplan").exists():
        sys.exit("run `make sandbox-plan` first and review the plan")
    sh("terraform", "apply", "-input=false", "tfplan", cwd=TF)
    (TF / "tfplan").unlink()
    initialize()


def initialize() -> None:
    """Everything after Terraform; idempotent, so `sandbox.py initialize` resumes a failed apply."""
    out = outputs()
    push_images(out)
    td = out["task_definitions"]
    for step, family in (("database", td["db_init"]), ("polaris realm", td["polaris_admin"])):
        if run_task(out, family, timeout=900) != 0:
            sys.exit(f"{step} initialization failed")
    scale(out, "polaris", 1)
    scale(out, "prefect", 1)
    scale(out, "prefect-worker", 1)
    scale(out, "partner-api", 1)
    start_cdc(out)
    if run_task(out, td["toolbox_admin"], ["python", "-m", "stack.bootstrap"], "toolbox", timeout=1800) != 0:
        sys.exit("bootstrap failed")
    if run_task(out, td["toolbox_admin"], ["python", "-m", "stack.bootstrap.doris"], "toolbox", timeout=2400) != 0:
        sys.exit("doris setup failed")
    print("sandbox ready")


def start_cdc(out: dict) -> None:
    """DMS captures the clinic database once db-init has created its table."""
    dms = session().client("dms")
    task = dms.describe_replication_tasks(Filters=[{"Name": "replication-task-arn", "Values": [out["dms_task"]]}])["ReplicationTasks"][0]
    if task["Status"] != "running":
        # DMS starts a task only after a successful connection test of both endpoints.
        for endpoint in (task["SourceEndpointArn"], task["TargetEndpointArn"]):
            dms.test_connection(ReplicationInstanceArn=task["ReplicationInstanceArn"], EndpointArn=endpoint)
            dms.get_waiter("test_connection_succeeds").wait(Filters=[
                {"Name": "endpoint-arn", "Values": [endpoint]},
                {"Name": "replication-instance-arn", "Values": [task["ReplicationInstanceArn"]]}])
        print("DMS endpoints reachable", flush=True)
        kind = "start-replication" if task["Status"] == "ready" else "resume-processing"
        dms.start_replication_task(ReplicationTaskArn=out["dms_task"], StartReplicationTaskType=kind)
        dms.get_waiter("replication_task_running").wait(
            Filters=[{"Name": "replication-task-arn", "Values": [out["dms_task"]]}])
    print("DMS capturing the clinic database", flush=True)


def test(args: list[str]) -> int:
    out = outputs()
    command = ["pytest", "-q", *(args or AWS_SUITES)]
    return run_task(out, out["task_definitions"]["toolbox_admin"], command, "toolbox", timeout=7200)


def bench(args: list[str]) -> None:
    """A benchmark scenario (bench/run.py) with every step a toolbox task: the same 2 vCPU and 8 GB
    cap as locally, against real S3, DynamoDB, KMS and Polaris on RDS. Results: bench/results/aws/."""
    os.environ["BENCH_RESULTS"] = "aws"
    sys.path.insert(0, str(ROOT))
    from bench import run as bench_run

    out = outputs()

    def ecs_step(step: list[str], timeout: int) -> tuple[int, str, str]:
        lines: list[str] = []
        try:
            code = run_task(out, out["task_definitions"]["toolbox_admin"], ["python", "-m", *step], "toolbox",
                            timeout=timeout, lines=lines)
        except TimeoutError as err:
            raise bench_run.Broke(str(err))
        text = "\n".join(lines)  # CloudWatch has stdout and stderr in one stream
        return code, text, text

    bench_run.STEP = ecs_step
    bench_run.main(args)


def key_deletion(partner: str, schedule: bool, days: int = 30) -> None:
    """The operator's step of an approved partner delete (loader/offboarding.py): as the
    partner-deletion role, the only identity the key policy lets schedule or cancel deletion of a
    partner's master key. Runs with the operator's own SSO profile, never in a task."""
    out = outputs()
    alias = out["partner_keys"][partner]
    creds = session().client("sts").assume_role(RoleArn=out["partner_deletion_role"],
                                                RoleSessionName=f"delete-{partner}"[:64])["Credentials"]
    kms = boto3.client("kms", region_name="us-east-1", aws_access_key_id=creds["AccessKeyId"],
                       aws_secret_access_key=creds["SecretAccessKey"], aws_session_token=creds["SessionToken"])
    key = kms.describe_key(KeyId=alias)["KeyMetadata"]["KeyId"]
    if schedule:
        when = kms.schedule_key_deletion(KeyId=key, PendingWindowInDays=days)["DeletionDate"]
        print(f"{alias}: deletion scheduled for {when:%Y-%m-%d}; `cancel-key-deletion {partner}` until then")
    else:
        kms.cancel_key_deletion(KeyId=key)
        print(f"{alias}: deletion cancelled; the key stays disabled until the partner is unfrozen")


# ---------------------------------------------------------------- destroy

def destroy() -> None:
    try:
        out = outputs()
        for service in ("partner-api", "prefect-worker", "prefect", "polaris"):
            scale(out, service, 0, wait=False)
        dms = session().client("dms")
        tasks = dms.describe_replication_tasks(Filters=[{"Name": "replication-task-arn", "Values": [out["dms_task"]]}])
        if tasks["ReplicationTasks"][0]["Status"] == "running":
            dms.stop_replication_task(ReplicationTaskArn=out["dms_task"])
            dms.get_waiter("replication_task_stopped").wait(
                Filters=[{"Name": "replication-task-arn", "Values": [out["dms_task"]]}])
            print("DMS task stopped", flush=True)
        # EMR Serverless refuses to delete a started application (it auto-stops only when idle).
        emr = session().client("emr-serverless")
        app = out["emr_application"]
        if emr.get_application(applicationId=app)["application"]["state"] not in ("STOPPED", "CREATED"):
            emr.stop_application(applicationId=app)
            while emr.get_application(applicationId=app)["application"]["state"] != "STOPPED":
                time.sleep(10)
            print("EMR application stopped", flush=True)
    except (subprocess.CalledProcessError, KeyError, IndexError):
        pass
    # Firehose streams are created at run time by the bootstrap and the streaming tests.
    firehose = session().client("firehose")
    for name in firehose.list_delivery_streams(Limit=100)["DeliveryStreamNames"]:
        if name.startswith("lakehouse-"):
            firehose.delete_delivery_stream(DeliveryStreamName=name, AllowForceDelete=True)
            print(f"deleted delivery stream {name}")
    # Principal secrets are created at run time by the bootstrap and tests, not by Terraform.
    sm = session().client("secretsmanager")
    for page in sm.get_paginator("list_secrets").paginate(Filters=[{"Key": "name", "Values": ["lakehouse/principals/"]}]):
        for secret in page["SecretList"]:
            sm.delete_secret(SecretId=secret["ARN"], ForceDeleteWithoutRecovery=True)
            print(f"deleted secret {secret['Name']}")
    sh("terraform", "destroy", "-input=false", "-auto-approve", cwd=TF)
    leftovers()


def leftovers() -> None:
    """Anything tagged for this project that still exists (should print nothing)."""
    tagging = session().client("resourcegroupstaggingapi")
    found = [r["ResourceARN"] for page in tagging.get_paginator("get_resources").paginate(
        TagFilters=[{"Key": "Project", "Values": ["lakehouse-loader"]}]) for r in page["ResourceTagMappingList"]]
    print("left behind:" if found else "nothing tagged lakehouse-loader is left", *found, sep="\n  ")


if __name__ == "__main__":
    command, rest = sys.argv[1], sys.argv[2:]
    if command == "plan":
        plan()
    elif command == "apply":
        apply()
    elif command == "initialize":
        initialize()
    elif command == "push":  # rebuild and push the images only (code changes between runs)
        push_images(outputs())
    elif command == "test":
        sys.exit(test(rest))
    elif command == "bench":
        bench(rest)
    elif command in ("schedule-key-deletion", "cancel-key-deletion"):
        days = int(rest[rest.index("--days") + 1]) if "--days" in rest else 30
        key_deletion(rest[0], command == "schedule-key-deletion", days)
    elif command == "destroy":
        destroy()
    elif command == "leftovers":
        leftovers()
    elif command == "run":  # any command as a toolbox task, e.g. run python -m stack.bootstrap.doris
        out = outputs()
        sys.exit(run_task(out, out["task_definitions"]["toolbox_admin"], rest, "toolbox", timeout=3600))
    else:
        sys.exit(__doc__)
