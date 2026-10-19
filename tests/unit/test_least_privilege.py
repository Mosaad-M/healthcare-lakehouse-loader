"""Flow runs (scheduled ingests, Gold, maintenance) never carry administrator credentials: only the
bootstrap and tests do."""
import re
from pathlib import Path


from flows import deploy

ADMIN = ("POLARIS_ROOT_SECRET", "DORIS_ROOT_PASSWORD", "RUSTFS_ACCESS_KEY", "RUSTFS_SECRET_KEY", "POSTGRES_PASSWORD")


def test_local_flow_runs_get_no_admin_credentials(monkeypatch):
    for name in ADMIN:
        monkeypatch.setenv(name, "admin-secret")
    monkeypatch.delenv("LOADER_PLATFORM", raising=False)
    env = deploy.job_variables()["env"]
    assert not set(ADMIN) & set(env), set(ADMIN) & set(env)


def test_the_aws_flow_task_definition_gets_no_admin_secrets():
    """infra/sandbox: flow runs use the `toolbox` task definition; admin secrets only in `toolbox_admin`."""
    tf = (Path(__file__).parents[2] / "infra" / "sandbox" / "ecs.tf").read_text()
    flow_secrets = re.search(r"flow_secrets = \[(.*?)\]", tf, re.S).group(1)
    assert not [name for name in ADMIN if name in flow_secrets]
    flow_task = re.search(r'resource "aws_ecs_task_definition" "toolbox" \{(.*?)\n\}', tf, re.S).group(1)
    assert "local.flow_secrets" in flow_task and "admin_secrets" not in flow_task
