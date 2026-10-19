"""The scheduled path: the maintenance deployment runs in a worker container and records its runs."""
from datetime import datetime, timezone

from prefect.deployments import run_deployment

from loader.maintenance import RUNS
from tests.conftest import scan_all


def test_maintenance_deployment_runs_and_records(s):
    started = datetime.now(timezone.utc).isoformat()
    table = "phi.silver.provider_directory__providers"
    # On AWS the run waits for an ECS task and then EMR capacity: 10 minutes was observed under load.
    run = run_deployment("maintenance/policies", parameters={"only": [table]}, timeout=1500)
    assert run.state.is_completed(), f"{run.state.type}: {run.state.message}"
    recent = [i for i in scan_all(s, RUNS)
              if i["table_name"]["S"] == table and i["run_id"]["S"].split("@", 1)[1] >= started]
    # Every table has the default policies, so at least orphan removal runs and is recorded.
    assert recent, "the deployment did not maintain the table"
