"""Silver layouts for scale: append and cdc tables are partitioned by pk_bucket (new ones at creation,
existing ones by partition evolution), and snapshot and window tables keep one day of snapshots
instead of a week."""
import uuid
from datetime import date

import pytest

from loader import deploy, duck, maintenance, pipeline, synthetic, tables
from loader.polaris import Polaris
from loader.state import SILVER_COMMITTED, Manifest
from tests.pipeline.conftest import deploy_copy, drop, remove_source


def default_spec(s, contract) -> list[str]:
    meta = Polaris(s).load_table(tables.CATALOG, tables.SILVER, contract.table)["metadata"]
    spec = next(sp for sp in meta["partition-specs"] if sp["spec-id"] == meta["default-spec-id"])
    return [f["name"] for f in spec["fields"]]


def snapshot_hours(s, contract) -> int:
    effective = maintenance.policies(Polaris(s, "maintenance"), tables.CATALOG, tables.SILVER, contract.table)
    return effective["system.snapshot-expiry"]["config"]["max-snapshot-age-hours"]


def silver_partitions(s, contract) -> set:
    con = duck.connect(s, tables.CATALOG, principal="maintenance")
    rows = con.execute(f"SELECT DISTINCT file_path FROM iceberg_metadata('{tables.qualified(contract, tables.SILVER)}') "
                       "WHERE status <> 'DELETED' AND manifest_content = 'DATA'").fetchall()
    return {path.split("pk_bucket=")[1].split("/")[0] for (path,) in rows if "pk_bucket=" in path}


@pytest.fixture
def made(s):
    created = []
    yield created
    for contract in created:
        remove_source(s, contract)


def test_an_append_table_is_partitioned_by_bucket_and_loads_into_its_buckets(s, contracts_dir, canaries, made):
    labs = deploy_copy(s, contracts_dir, "lab_results", f"layout_append_{uuid.uuid4().hex[:6]}")
    made.append(labs)
    assert default_spec(s, labs) == ["pk_bucket"]
    drop(s, labs, "labs.ndjson", synthetic.lab_ndjson(canaries, 60, members=12))
    pipeline.ingest(s, labs)
    assert {b.state for b in Manifest(s).for_source(labs.source)} == {SILVER_COMMITTED}
    assert len(silver_partitions(s, labs)) > 1, "rows land in several pk_bucket partitions"
    assert snapshot_hours(s, labs) == 168  # append keeps the catalog default


def test_an_existing_unpartitioned_table_is_evolved_and_keeps_loading(s, contracts_dir, canaries, made):
    labs = deploy_copy(s, contracts_dir, "lab_results", f"layout_evolve_{uuid.uuid4().hex[:6]}")
    made.append(labs)
    root = Polaris(s)
    # Recreate Silver as the old layout (unpartitioned) with one batch in it.
    writer = Polaris(s, "loader")  # purging is a content manager's right, not root's
    status, _ = writer.rest("DELETE", writer.table_url(tables.CATALOG, tables.SILVER, labs.table) + "?purgeRequested=true")
    assert status == 204
    deploy.ensure_table_columns(root, tables.CATALOG, tables.SILVER, labs.table, tables.columns(labs, tables.SILVER),
                                tables.properties(labs, tables.SILVER), partition=None)
    deploy.ensure_source_principal(root, s, labs)  # grants belong to the table entity: grant the new one
    assert default_spec(s, labs) == []
    drop(s, labs, "first.ndjson", synthetic.lab_ndjson(canaries, 30, members=6))
    pipeline.ingest(s, labs)

    deploy.ensure_table(root, labs, tables.SILVER)  # what a redeploy does
    assert default_spec(s, labs) == ["pk_bucket"]
    drop(s, labs, "second.ndjson", synthetic.lab_ndjson(canaries, 30, members=6, seed=5, id_offset=1000))
    pipeline.ingest(s, labs)
    assert {b.state for b in Manifest(s).for_source(labs.source)} == {SILVER_COMMITTED}
    con = duck.connect(s, tables.CATALOG, principal=labs.principal)
    bronze, silver = (con.execute(f"SELECT count(*) FROM {tables.qualified(labs, layer)} WHERE length(_batch_id) > 0")
                      .fetchone()[0] for layer in (tables.BRONZE, tables.SILVER))
    assert silver == bronze  # append: every row once, across both layouts
    assert silver_partitions(s, labs), "new data is written in the new layout"


@pytest.mark.parametrize("example", ["provider_directory", "acme_claims"])  # snapshot, window
def test_tables_that_rewrite_themselves_keep_one_day_of_snapshots(s, contracts_dir, made, example):
    contract = deploy_copy(s, contracts_dir, example, f"layout_short_{uuid.uuid4().hex[:6]}")
    made.append(contract)
    assert contract.silver.semantics in deploy.SHORT_SNAPSHOT_SEMANTICS
    assert snapshot_hours(s, contract) == 24
    assert default_spec(s, contract) == []  # snapshot and window are not bucketed
