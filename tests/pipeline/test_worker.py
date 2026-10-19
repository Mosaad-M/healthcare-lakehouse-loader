"""The scheduled path: a file dropped for a configured source is loaded by its Prefect deployment,
in a flow-run container started by the worker (no admin credentials inside)."""
import hashlib
import time

from prefect.deployments import run_deployment

from loader import contracts, raw, synthetic
from loader.state import SILVER_COMMITTED, Manifest
from tests.pipeline.conftest import drop


def loaded(s, contract, digest: str) -> bool:
    return any(b.content_sha256 == digest and b.state == SILVER_COMMITTED
               for b in Manifest(s).for_source(contract.source))


def test_deployment_loads_a_drop(s, run_id):
    providers = contracts.load_all()["provider_directory"]
    body = synthetic.providers_csv(60, seed=int(run_id[:6], 16))  # unique bytes per run
    drop(s, providers, f"directory_{run_id}.csv", body)
    run = run_deployment(f"ingest/{providers.source}", timeout=300)
    assert run.state.is_completed(), f"{run.state.type}: {run.state.message}"
    # The schedule may have taken the file first; either run leaves it loaded and the drop empty.
    digest, deadline = hashlib.sha256(body).hexdigest(), time.time() + 120
    while not loaded(s, providers, digest) and time.time() < deadline:
        time.sleep(2)
    assert loaded(s, providers, digest)
    assert not raw.RawZone(s).pending_drops(providers)
