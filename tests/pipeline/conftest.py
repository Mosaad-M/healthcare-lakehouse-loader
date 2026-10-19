"""Per-run copies of the example contracts, deployed under run-specific source names."""
from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml

from loader import contracts, deploy, raw, tables
from loader.polaris import Polaris
from loader.state import MANIFEST, Manifest

EXAMPLES = Path("contracts")


@pytest.fixture(scope="session")
def started_at() -> str:
    """Taken before any source is deployed (the `sources` fixture requests it first)."""
    return datetime.now(timezone.utc).isoformat()


def deploy_copy(s, contracts_dir: Path, example: str, source: str, **changes) -> contracts.Contract:
    """Write a copy of an example contract under a new source name and deploy it (no code change)."""
    data = yaml.safe_load((EXAMPLES / f"{example}.yaml").read_text())
    data.update(source=source, **changes)
    path = contracts_dir / f"{source}.yaml"
    path.write_text(yaml.safe_dump(data, sort_keys=False))
    contract = contracts.load(path)
    deploy.deploy_contract(Polaris(s), s, contract)
    return contract


def remove_source(s, contract: contracts.Contract) -> None:
    root = Polaris(s)
    # Purging deletes data, which Polaris allows content managers (`loader`), not root's admin role.
    writer = Polaris(s, "loader")
    for layer in (tables.BRONZE, tables.SILVER):
        status, _ = writer.rest("DELETE", writer.table_url(tables.CATALOG, layer, contract.table) + "?purgeRequested=true")
        assert status in (204, 404), f"dropping {layer}.{contract.table}: HTTP {status}"
    root.mgmt("DELETE", f"/principals/{contract.principal}")
    root.mgmt("DELETE", f"/principal-roles/{contract.principal}_role")
    root.mgmt("DELETE", f"/catalogs/{tables.CATALOG}/catalog-roles/{contract.principal}")
    s.delete_principal(contract.principal)
    ddb = s.dynamodb()
    for batch in Manifest(s).for_source(contract.source):
        ddb.delete_item(TableName=MANIFEST, Key={"batch_id": {"S": batch.batch_id}})
    zone = raw.RawZone(s)
    for prefix in (f"drops/{contract.source}/", f"landed/{contract.source}/", f"quarantine/{contract.source}/"):
        for key in zone.keys(prefix):
            zone.delete(key)


@pytest.fixture(scope="session")
def sources(s, run_id, contracts_dir, started_at):
    """{example name: deployed per-run contract}"""
    deployed = {name: deploy_copy(s, contracts_dir, name, f"{name}_{run_id}")
                for name in ("acme_claims", "provider_directory", "lab_results")}
    yield deployed
    errors = []
    for contract in deployed.values():  # try every source, so one failure does not leak the rest
        try:
            remove_source(s, contract)
        except Exception as err:  # noqa: BLE001
            errors.append(f"{contract.source}: {err!r}")
    assert not errors, "teardown failed: " + "; ".join(errors)


def drop(s, contract: contracts.Contract, name: str, body: bytes) -> None:
    raw.RawZone(s).drop(contract, name, body)
