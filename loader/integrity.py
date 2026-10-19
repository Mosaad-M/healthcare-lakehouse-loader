"""Table integrity: every file a table's current snapshot references must exist in storage.

Twice a committed data file went missing (a read then failed with HTTP 404). The cause is DuckDB
1.5.6: when a commit's answer is lost (a timeout, or a proxy error), it resends the commit, gets 409,
and deletes the files of the commit Polaris applied (README, DuckDB). This turns such a loss into an
alert and a repair instead of a table that cannot be read:

- check: the files the current snapshot references (manifest list, manifests, data and delete files)
  against a listing of the table's location, made with credentials vended for that table.
- repair, under the table's writer lock: first restore each missing file from its previous
  version (the lake buckets keep deleted objects for a few days: infra/sandbox/s3.tf). Iceberg files
  are written once, so the previous version is the file the snapshot references, and the table is
  whole again with nothing to redo. Only if that fails, roll the table back to the newest snapshot
  whose files all exist, then send what the rollback took out back through the pipeline:
  - Bronze: every batch now missing from Bronze, or with fewer rows than it loaded, goes back to
    LANDED; the next ingest loads it again from the raw zone (Silver re-applies it idempotently).
  - Silver: every batch whose Silver commit came after that snapshot goes back to BRONZE_COMMITTED;
    the next ingest re-applies them in landing order (Silver is deterministic given Bronze).
  - Gold and other tables: the rollback is enough; their next run rebuilds them.
  - A batch whose raw file was erased cannot be loaded again and is reported.
- If no whole snapshot exists (the oldest data file is the one lost), automation stops and alerts:
  `python -m loader.cli repair --recreate <table>` (an operator, with root) drops and redeploys the
  table, which needs catalog administration for the source's grants, and replays every batch.

Writers heal their own table (heal, through duck.run_txn): a write that hits a missing file, or whose
commit succeeded only on a retry, checks and repairs the table under the lock it holds, so readers see
the loss for seconds instead of until the next maintenance run.

Two DuckDB 1.5.6 behaviours shape the rollback: deleting every row still reads every file (so a table with a
missing file cannot be emptied), and a table whose main reference was removed is still read at its
last snapshot. Rollback to a whole snapshot is the operation that works.
"""
import logging
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import datetime, timezone

import duckdb

from loader import duck, tables
from loader.polaris import Polaris
from loader.settings import Settings
from loader.state import (BRONZE_COMMITTED, LANDED, QUARANTINED, SILVER_COMMITTED, Manifest, table_lock)

log = logging.getLogger(__name__)
PRINCIPAL = "maintenance"  # reads every table's metadata and may move its snapshot reference


@dataclass
class Report:
    table: str
    snapshot_id: int | None
    referenced: int = 0
    missing: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.missing


def _listing(s: Settings, polaris: Polaris, catalog: str, namespace: str, table: str) -> set[str]:
    from loader.maintenance import list_table_files

    return {path for path, _ in list_table_files(s, polaris, catalog, namespace, table)}


def _referenced(con: duckdb.DuckDBPyConnection, qualified: str, snapshot_id: int, manifest_list: str) -> set[str]:
    rows = con.execute(f"SELECT DISTINCT file_path, manifest_path FROM iceberg_metadata('{qualified}', "
                       f"snapshot_from_id => {snapshot_id}) WHERE status <> 'DELETED'").fetchall()
    return {manifest_list} | {r[0] for r in rows} | {r[1] for r in rows}


def _snapshots(con: duckdb.DuckDBPyConnection, qualified: str) -> list[tuple[int, int, str]]:
    """(snapshot_id, sequence_number, manifest_list), newest first."""
    return con.execute(f"SELECT snapshot_id, sequence_number, manifest_list FROM iceberg_snapshots('{qualified}') "
                       "ORDER BY sequence_number DESC").fetchall()


def check(s: Settings, catalog: str, namespace: str, table: str) -> Report:
    polaris = Polaris(s, PRINCIPAL)
    name = f"{catalog}.{namespace}.{table}"
    current = polaris.load_table(catalog, namespace, table)["metadata"].get("current-snapshot-id")
    if current in (None, -1):
        return Report(name, None)  # no data yet
    listing = _listing(s, polaris, catalog, namespace, table)
    con = duck.connect(s, catalog, principal=PRINCIPAL)
    qualified = f"ice.{namespace}.{table}"
    snaps = {sid: manifest_list for sid, _, manifest_list in _snapshots(con, qualified)}
    try:
        referenced = _referenced(con, qualified, current, snaps[current])
    except duckdb.Error as err:  # the metadata itself is missing
        return Report(name, current, missing=[f"metadata unreadable: {duck.redact(err)[:200]}"])
    return Report(name, current, len(referenced), sorted(referenced - listing))


class NeedsOperator(RuntimeError):
    """No snapshot of the table is whole: `loader.cli repair --recreate` (root) is needed."""


class TableRepaired(RuntimeError):
    """A writer found its table broken, and the repair rolled it back and reset batches: the run
    starts over, in landing order (loader/pipeline.py)."""


def restore(s: Settings, missing: list[str]) -> list[str]:
    """Copy each missing file back from its newest earlier version. Returns the paths restored.

    Runs as `lake-restore`, which may list versions, read them, and copy an object within its own
    bucket, nothing else (infra/sandbox/iam.tf and s3.tf).
    """
    s3, restored = s.s3("lake-restore"), []
    for path in missing:
        if not path.startswith("s3://"):
            continue  # unreadable metadata, reported as such
        bucket, key = path.removeprefix("s3://").split("/", 1)
        versions = [v for page in s3.get_paginator("list_object_versions").paginate(Bucket=bucket, Prefix=key)
                    for v in page.get("Versions", []) if v["Key"] == key]
        if not versions:
            continue  # kept no longer, or the bucket is not versioned
        newest = max(versions, key=lambda v: v["LastModified"])
        s3.copy_object(Bucket=bucket, Key=key, CopySource={"Bucket": bucket, "Key": key, "VersionId": newest["VersionId"]})
        restored.append(path)
    return restored


def repair(s: Settings, catalog: str, namespace: str, table: str, locked: bool = False) -> dict:
    """Bring a table whose current snapshot lost files back to a readable state (module docstring).
    `locked`: the caller already holds the table's writer lock."""
    name = f"{catalog}.{namespace}.{table}"
    with nullcontext() if locked else table_lock(s, name):
        report = check(s, catalog, namespace, table)  # again, under the lock
        if report.ok:
            return {"repaired": False, "reason": "healthy on recheck"}
        polaris = Polaris(s, PRINCIPAL)
        props = polaris.load_table(catalog, namespace, table)["metadata"].get("properties", {})
        layer, source = props.get("loader.layer"), props.get("loader.source")
        restored = restore(s, report.missing)
        if restored:
            after = check(s, catalog, namespace, table)
            if after.ok:
                log.error("integrity repair of %s: %d missing files restored from their previous version", name, len(restored))
                return {"repaired": True, "layer": layer, "missing": len(report.missing), "restored": len(restored),
                        "rolled_back_to": None, "batches_reset": 0, "unrecoverable_batches": []}
            report = after
        target = _newest_whole_snapshot(s, polaris, catalog, namespace, table, report.snapshot_id)
        if target is None:
            log.error("integrity: %s has no whole snapshot; run loader.cli repair --recreate", name)
            raise NeedsOperator(f"{name}: no snapshot has all its files; recreate the table (loader.cli repair --recreate)")
        target_id, target_ms = target
        polaris.update_table(catalog, namespace, table,
                             [{"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": target_id}],
                             requirements=[{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": report.snapshot_id}])
        reset, unrecoverable = _reset_batches(s, catalog, namespace, table, layer, source, target_ms)
    log.error("integrity repair of %s: %d missing files; rolled back to snapshot %s; %d batches to redo; "
              "%d unrecoverable", name, len(report.missing), target_id, reset, len(unrecoverable))
    return {"repaired": True, "layer": layer, "missing": len(report.missing), "restored": len(restored),
            "rolled_back_to": target_id, "batches_reset": reset, "unrecoverable_batches": unrecoverable}


def heal(s: Settings, catalog: str, namespace: str, table: str) -> bool:
    """duck.run_txn's `heal` for a writer that holds the table's lock: False if the table is whole,
    True if its files were restored (the write is retried as is). Raises TableRepaired after a
    rollback (the run starts over), NeedsOperator if no snapshot is whole."""
    result = repair(s, catalog, namespace, table, locked=True)
    if not result["repaired"]:
        return False
    if result["rolled_back_to"] is None:
        return True
    raise TableRepaired(f"{catalog}.{namespace}.{table} lost files and was rolled back to snapshot "
                        f"{result['rolled_back_to']}; {result['batches_reset']} batches to redo")


def _newest_whole_snapshot(s, polaris, catalog, namespace, table, current) -> tuple[int, int] | None:
    """(snapshot_id, timestamp_ms) of the newest snapshot before `current` whose files all exist."""
    listing = _listing(s, polaris, catalog, namespace, table)
    con = duck.connect(s, catalog, principal=PRINCIPAL)
    qualified = f"ice.{namespace}.{table}"
    rows = con.execute(f"SELECT snapshot_id, sequence_number, manifest_list, timestamp_ms FROM iceberg_snapshots('{qualified}') "
                       "ORDER BY sequence_number DESC").fetchall()
    current_seq = next(seq for sid, seq, _, _ in rows if sid == current)
    for sid, seq, manifest_list, ts in rows:
        if seq >= current_seq:
            continue
        try:
            if _referenced(con, qualified, sid, manifest_list) <= listing:
                if hasattr(ts, "timestamp"):  # DuckDB returns a naive datetime in UTC
                    ts = ts.replace(tzinfo=timezone.utc).timestamp() * 1000
                return sid, int(ts)
        except duckdb.Error:
            continue  # this snapshot's metadata is gone too
    return None


def _reset_batches(s, catalog, namespace, table, layer, source, since_ms: int) -> tuple[int, list[str]]:
    """Send the batches the rollback took out of the table back through the pipeline."""
    if not source or layer not in (tables.BRONZE, tables.SILVER):
        return 0, []
    manifest, reset, unrecoverable = Manifest(s), 0, []
    if layer == tables.SILVER:
        since = datetime.fromtimestamp(since_ms / 1000, timezone.utc).isoformat(timespec="microseconds")
        for item in _manifest_items(s, source):
            # A batch's last update is its Silver commit; redoing one the snapshot already has is harmless.
            if item["state"]["S"] == SILVER_COMMITTED and item["updated_at"]["S"] >= since:
                manifest.advance(item["batch_id"]["S"], BRONZE_COMMITTED, reason="re-applied after integrity repair")
                reset += 1
        return reset, unrecoverable
    con = duck.connect(s, catalog, principal=PRINCIPAL)
    present = dict(con.execute(f"SELECT _batch_id, count(*) FROM ice.{namespace}.{table} GROUP BY ALL").fetchall())
    for batch in manifest.for_source(source):
        if batch.state in (BRONZE_COMMITTED, SILVER_COMMITTED) and present.get(batch.batch_id, 0) < batch.rows_bronze:
            if batch.raw_purged:
                manifest.advance(batch.batch_id, QUARANTINED, reason="lost in storage and its raw file was erased")
                unrecoverable.append(batch.batch_id)
            else:
                manifest.advance(batch.batch_id, LANDED, reason="reloaded after integrity repair")
                reset += 1
    return reset, unrecoverable


def _manifest_items(s: Settings, source: str) -> list[dict]:
    from loader.state import MANIFEST

    ddb, items = s.dynamodb(), []
    kwargs = {"TableName": MANIFEST, "FilterExpression": "#src = :src", "ExpressionAttributeNames": {"#src": "source"},
              "ExpressionAttributeValues": {":src": {"S": source}}, "ConsistentRead": True}
    while True:
        page = ddb.scan(**kwargs)
        items += page.get("Items", [])
        if "LastEvaluatedKey" not in page:
            return items
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def recreate(s: Settings, contract, layer: str) -> dict:
    """Operator path (root): drop the table without purging, redeploy it empty with its grants, and
    send every batch back through the pipeline. Batches whose raw file is gone cannot come back."""
    from loader import deploy

    root = Polaris(s)
    with table_lock(s, f"{tables.CATALOG}.{layer}.{contract.table}"):
        root.rest("DELETE", root.table_url(tables.CATALOG, layer, contract.table))
        deploy.deploy_contract(root, s, contract)
        manifest, reset, unrecoverable = Manifest(s), 0, []
        for batch in manifest.for_source(contract.source):
            if layer == tables.SILVER and batch.state == SILVER_COMMITTED:
                manifest.advance(batch.batch_id, BRONZE_COMMITTED, reason="Silver recreated by an operator")
                reset += 1
            elif layer == tables.BRONZE and batch.state in (BRONZE_COMMITTED, SILVER_COMMITTED):
                if batch.raw_purged:
                    manifest.advance(batch.batch_id, QUARANTINED, reason="lost in storage and its raw file was erased")
                    unrecoverable.append(batch.batch_id)
                else:
                    manifest.advance(batch.batch_id, LANDED, reason="Bronze recreated by an operator")
                    reset += 1
    log.error("integrity: %s.%s recreated by an operator; %d batches to redo; %d unrecoverable",
              layer, contract.table, reset, len(unrecoverable))
    return {"recreated": True, "batches_reset": reset, "unrecoverable_batches": unrecoverable}
