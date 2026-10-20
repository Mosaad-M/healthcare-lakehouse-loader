"""Under frequent merges, policy-driven Spark maintenance keeps tables within policy, removes
orphans left by retried commits, never touches referenced files, keeps PHI out of metadata,
and leaves the data reading identically in DuckDB and Doris."""
import json
import threading
from datetime import date, datetime, timedelta, timezone

import pytest

from flows.ingest import ingest
from loader import duck, maintenance, synthetic, tables
from loader.polaris import Polaris, expect
from loader.state import table_lock
from tests.conformance.test_phi_metadata import manifest_bounds
from tests.pipeline.conftest import deploy_copy, drop, remove_source
from tests.pipeline.test_file_drops import plain_rows

STRICT = {
    "system.data-compaction": {"version": "2025-02-03", "enable": True, "config": {
        "target-file-size-bytes": 134217728, "min-input-files": 2, "delete-file-threshold": 1}},
    "system.snapshot-expiry": {"version": "2025-02-03", "enable": True, "config": {
        "max-snapshot-age-hours": 0, "min-snapshots-to-keep": 3}},
    "system.metadata-compaction": {"version": "2025-02-03", "enable": True, "config": {"max-manifests": 5}},
    "system.orphan-file-removal": {"version": "2025-02-03", "enable": True, "max_orphan_file_age_in_days": 1},
}


def attach_strict_policies(s, run_id: str, layer: str, table: str) -> list[str]:
    root, names = Polaris(s), []
    base = root.policy_url(tables.CATALOG, "/namespaces/ops/policies")
    # A table holds one table-level policy per type: drop the short snapshot expiry that deploy gives
    # snapshot and window tables, as an operator overriding it would.
    root.rest("POST", f"{base}/short_snapshot_expiry/mappings", {"target": {"type": "table-like", "path": [layer, table]}})
    for ptype, content in STRICT.items():
        name = f"t{run_id}_{layer}_{ptype.split('.')[-1].replace('-', '_')}"
        expect(root.rest("POST", base, {"name": name, "type": ptype, "content": json.dumps(content)}), 200)
        expect(root.rest("PUT", f"{base}/{name}/mappings",
                         {"target": {"type": "table-like", "path": [layer, table]}}), 200, 204)
        names.append(name)
    return names


def detach_policies(s, layer: str, table: str, names: list[str]) -> None:
    root = Polaris(s)
    base = root.policy_url(tables.CATALOG, "/namespaces/ops/policies")
    for name in names:
        root.rest("POST", f"{base}/{name}/mappings", {"target": {"type": "table-like", "path": [layer, table]}})
        root.rest("DELETE", f"{base}/{name}?detach-all=true")


def race(s, contract, writers: int = 4) -> list[str]:
    """Commits released together without retries: every loser leaves files behind (orphans)."""
    table = tables.qualified(contract, tables.BRONZE)
    cons = [duck.connect(s, tables.CATALOG, principal=contract.principal) for _ in range(writers)]
    for i, con in enumerate(cons):
        con.execute("BEGIN")
        con.execute(f"INSERT INTO {table} (claim_id, _batch_id) SELECT 'race' || x, 'race{i}' FROM range(20) r(x)")
    out, barrier = [], threading.Barrier(writers)

    def commit(con):
        barrier.wait()
        try:
            con.execute("COMMIT")
            out.append("ok")
        except Exception as err:  # noqa: BLE001
            out.append("conflict" if duck.is_conflict(err) else duck.redact(err))

    threads = [threading.Thread(target=commit, args=(c,)) for c in cons]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return out


def aged(listing):
    """Test seam: pretend every file is a month old so the one-day window covers fresh orphans."""
    old = datetime.now(timezone.utc) - timedelta(days=30)
    return [(path, old) for path, _ in listing]


def orphan_count(executor, s, contract, layer) -> int:
    """Orphans Iceberg would remove (dry run), with the test's aged listing."""
    listing = aged(maintenance.list_table_files(s, Polaris(s, "maintenance"), tables.CATALOG, layer, contract.table))
    spec = maintenance.orphan_spec(datetime.now(timezone.utc) - timedelta(days=1), listing, dry_run=True)
    return executor.run(f"{tables.CATALOG}.{layer}.{contract.table}", [spec])[0]["orphan_files"]


def doris_count(contract, layer) -> int | None:
    """Rows Doris reads from the lake table; None when the stack runs without Doris (catalog CI job)."""
    from flows.gold import doris_running
    from loader import doris

    if not doris_running():
        return None
    con = doris.connect()
    doris.query(con, f"REFRESH TABLE lake_phi.{layer}.{contract.table}")
    return doris.query(con, f"SELECT count(*) FROM lake_phi.{layer}.{contract.table}")[0][0]


@pytest.fixture(scope="module")
def churned(s, run_id, contracts_dir, canaries):
    contract = deploy_copy(s, contracts_dir, "acme_claims", f"maint_{run_id}")
    for i in range(15):  # overlapping restatements: many small files, deletes, snapshots
        drop(s, contract, f"claims_{i:02d}.csv", synthetic.claims_csv(
            canaries, date(2026, 9, 1) + timedelta(days=i % 5), days=3, per_day=12, seed=i, members=9))
        ingest(source=contract.source, contracts_dir=str(contracts_dir))
    outcomes = race(s, contract)
    attached = {layer: attach_strict_policies(s, run_id, layer, contract.table) for layer in (tables.BRONZE, tables.SILVER)}
    yield contract, outcomes
    for layer, names in attached.items():
        detach_policies(s, layer, contract.table, names)
    remove_source(s, contract)


def test_maintenance_brings_churned_tables_within_policy(s, churned):
    contract, outcomes = churned
    assert outcomes.count("conflict") >= 1, f"the race left no orphans to remove: {outcomes}"
    executor = maintenance.executor(s)
    snapshot = {layer: plain_rows(s, contract, layer, "_batch_id, claim_id" + (", _row_number" if layer == "bronze" else ""))
                for layer in (tables.BRONZE, tables.SILVER)}
    counts = {layer: doris_count(contract, layer) for layer in (tables.BRONZE, tables.SILVER)}
    orphans_before = orphan_count(executor, s, contract, tables.BRONZE)
    silver = maintenance.health(Polaris(s, "maintenance"), executor, tables.CATALOG, tables.SILVER, contract.table)
    assert silver.delete_files > 0 and silver.data_files > 5 and silver.snapshots > 3 and orphans_before > 0

    for layer in (tables.BRONZE, tables.SILVER):
        run = maintenance.maintain_table(s, executor, tables.CATALOG, layer, contract.table, _listing_hook=aged)
        assert not run.skipped
        assert {"compact", "expire_snapshots", "remove_orphans"} <= {a.kind for a in run.actions}
        after = run.after
        assert after.delete_files == 0, f"{layer}: delete files remain"
        assert after.snapshots <= 3, f"{layer}: {after.snapshots} snapshots"
        assert after.manifests <= 5, f"{layer}: {after.manifests} manifests"
        assert after.data_files < run.before.data_files

    assert orphan_count(executor, s, contract, tables.BRONZE) == 0
    for layer in (tables.BRONZE, tables.SILVER):
        assert plain_rows(s, contract, layer, "_batch_id, claim_id" + (", _row_number" if layer == "bronze" else "")) \
            == snapshot[layer], f"{layer}: data changed"
        assert doris_count(contract, layer) == counts[layer]


def test_compaction_keeps_phi_out_of_metadata(s, churned):
    """Files rewritten by Spark still have no bounds for PHI columns (service_date is PHI);
    non-PHI columns keep theirs (positive control)."""
    contract, _ = churned
    meta = Polaris(s).load_table(tables.CATALOG, tables.SILVER, contract.table)["metadata"]
    ids = {f["name"]: f["id"] for f in meta["schemas"][-1]["fields"]}
    con = duck.connect(s, tables.CATALOG, principal=contract.principal)
    bounds = manifest_bounds(s, con, tables.qualified(contract, tables.SILVER))
    assert not bounds.get(ids["service_date"], {}).get("lower"), "PHI bound written by compaction"
    assert not bounds.get(ids["member_name"], {}).get("lower")
    assert bounds.get(ids["claim_id"], {}).get("lower"), "no bounds at all: the check proves nothing"


def test_a_locked_table_is_skipped(s, churned):
    contract, _ = churned
    name = f"{tables.CATALOG}.{tables.SILVER}.{contract.table}"
    with table_lock(s, name):
        run = maintenance.maintain_table(s, maintenance.executor(s), tables.CATALOG, tables.SILVER, contract.table)
    assert run.skipped and "locked" in run.skipped


def test_an_orphan_window_under_a_day_is_refused(s, churned, run_id):
    contract, _ = churned
    root = Polaris(s)
    base = root.policy_url(tables.CATALOG, "/namespaces/ops/policies")
    name = f"t{run_id}_unsafe_orphans"
    expect(root.rest("POST", base, {"name": name, "type": "system.orphan-file-removal", "content": json.dumps(
        {"version": "2025-02-03", "enable": True, "max_orphan_file_age_in_days": 0.25})}), 200)
    target = {"target": {"type": "table-like", "path": [tables.BRONZE, contract.table]}}
    # Replace the strict bronze orphan policy with the unsafe one for this test.
    strict = f"t{run_id}_bronze_orphan_file_removal"
    root.rest("POST", f"{base}/{strict}/mappings", target)
    expect(root.rest("PUT", f"{base}/{name}/mappings", target), 200, 204)
    try:
        run = maintenance.maintain_table(s, maintenance.executor(s), tables.CATALOG, tables.BRONZE, contract.table)
        assert run.results["remove_orphans"] == {"refused": "orphan window shorter than one day"}
    finally:
        root.rest("POST", f"{base}/{name}/mappings", target)
        root.rest("DELETE", f"{base}/{name}?detach-all=true")
        expect(root.rest("PUT", f"{base}/{strict}/mappings", target), 200, 204)
