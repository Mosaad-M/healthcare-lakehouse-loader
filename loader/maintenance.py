"""Policy-driven table maintenance, executed by Spark.

For each table: read its health (Polaris metadata, plus the manifest count), read the maintenance
policies that apply to it (Polaris resolves catalog, namespace and table levels), plan the actions
those policies call for, and run them through an executor while holding the table's writer lock
(maintenance is a writer too). Results go to the `maintenance_runs` table.

Orphan removal lists the table's files itself, with credentials Polaris vends for that table, and
hands the listing to Iceberg (`file_list_view`). Real runs refuse a window shorter than a day
(the safety window must exceed the longest write-to-commit delay); holding the lock also
keeps in-flight writes of this table out of reach.
"""
import json
import logging
import os
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Protocol

from loader import maintenance_ops
from loader.polaris import Polaris, expect, vended_s3
from loader.settings import Settings
from loader.state import LockHeld, table_lock

RUNS = "maintenance_runs"
PRINCIPAL = "maintenance"
LAYOUT = {"phi": ["bronze", "silver", "gold"], "deid": ["gold"]}
MIN_ORPHAN_AGE = timedelta(days=1)

log = logging.getLogger(__name__)


@dataclass
class Health:
    data_files: int
    delete_files: int
    records: int
    total_bytes: int
    snapshots: int
    oldest_snapshot_hours: float
    manifests: int

    @property
    def avg_file_bytes(self) -> float:
        return self.total_bytes / self.data_files if self.data_files else 0.0


@dataclass
class Action:
    kind: str  # compact | rewrite_manifests | expire_snapshots | remove_orphans
    options: dict = field(default_factory=dict)


@dataclass
class TableRun:
    table: str
    actions: list[Action]
    before: Health
    after: Health | None = None
    results: dict = field(default_factory=dict)
    skipped: str = ""


class Executor(Protocol):
    """Runs Iceberg maintenance on Spark (loader/maintenance_ops.py). Spark Connect locally;
    EMR Serverless on AWS. `table` is `catalog.namespace.name`; actions are maintenance_ops dicts."""

    def stats(self, table: str) -> dict: ...

    def run(self, table: str, actions: list[dict]) -> list[dict]: ...


class SparkExecutor:
    """Spark Connect, as the `maintenance` principal (the server's catalog credentials)."""

    def __init__(self, s: Settings):
        from pyspark.sql import SparkSession

        self.spark = SparkSession.builder.remote(s.spark_remote).getOrCreate()

    def stats(self, table: str) -> dict:
        return maintenance_ops.stats(self.spark, table)

    def run(self, table: str, actions: list[dict]) -> list[dict]:
        return [maintenance_ops.run(self.spark, table, a) for a in actions]


class EmrExecutor:
    """EMR Serverless: one job per call, running loader/maintenance_ops.py with a spec in S3.

    The job reads the `maintenance` principal's credential from Secrets Manager itself; the spec
    holds table names, actions and (for orphan removal) the file listing, never credentials.
    """

    def __init__(self, s: Settings, timeout_seconds: int = 1800):
        import boto3

        self.s = s
        self.app = os.environ["LOADER_EMR_APPLICATION_ID"]
        self.role = os.environ["LOADER_EMR_JOB_ROLE"]
        self.bucket = os.environ["LOADER_ARTIFACTS_BUCKET"]
        self.emr = boto3.client("emr-serverless", region_name=s.region)
        self.s3 = boto3.client("s3", region_name=s.region)
        self.timeout = timeout_seconds

    def _job(self, operations: list[dict]) -> list[dict]:
        import time
        from pathlib import Path

        prefix = f"maintenance/{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"
        script = f"maintenance/maintenance_ops.py"
        self.s3.put_object(Bucket=self.bucket, Key=script,
                           Body=(Path(__file__).parent / "maintenance_ops.py").read_bytes())
        spec = {"operations": operations, "catalogs": list(LAYOUT), "polaris_uri": self.s.polaris_uri,
                "region": self.s.region, "principals_secret": "lakehouse/principals/maintenance"}
        self.s3.put_object(Bucket=self.bucket, Key=f"{prefix}/spec.json", Body=json.dumps(spec, default=str).encode())
        run = self.emr.start_job_run(
            applicationId=self.app, executionRoleArn=self.role, name=prefix.rsplit("/", 1)[1],
            jobDriver={"sparkSubmit": {
                "entryPoint": f"s3://{self.bucket}/{script}",
                "entryPointArguments": [f"s3://{self.bucket}/{prefix}/spec.json"],
                "sparkSubmitParameters": (
                    "--conf spark.jars=/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar "
                    "--conf spark.executor.cores=1 --conf spark.executor.memory=2g "
                    "--conf spark.driver.cores=1 --conf spark.driver.memory=2g")}},
            configurationOverrides={"monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": f"s3://{self.bucket}/maintenance-logs/"}}})
        deadline = time.time() + self.timeout
        while True:
            state = self.emr.get_job_run(applicationId=self.app, jobRunId=run["jobRunId"])["jobRun"]
            if state["state"] == "SUCCESS":
                break
            if state["state"] in ("FAILED", "CANCELLED") or time.time() > deadline:
                raise RuntimeError(f"EMR maintenance job {run['jobRunId']} ended {state['state']}")
            time.sleep(10)
        return json.loads(self.s3.get_object(Bucket=self.bucket, Key=f"{prefix}/results.json")["Body"].read())

    def stats(self, table: str) -> dict:
        return self._job([{"op": "stats", "table": table}])[0]

    def run(self, table: str, actions: list[dict]) -> list[dict]:
        return self._job([{"op": "run", "table": table, "action": a} for a in actions])


def executor(s: Settings) -> Executor:
    return EmrExecutor(s) if os.environ.get("LOADER_MAINTENANCE_EXECUTOR") == "emr" else SparkExecutor(s)


# ---------------------------------------------------------------- health and policies

def health(polaris: Polaris, executor: Executor, catalog: str, namespace: str, table: str) -> Health:
    meta = polaris.load_table(catalog, namespace, table)["metadata"]
    snapshots = meta.get("snapshots") or []
    oldest = min((sn["timestamp-ms"] for sn in snapshots), default=None)
    now_ms = datetime.now(timezone.utc).timestamp() * 1000
    stats = executor.stats(f"{catalog}.{namespace}.{table}") if meta.get("current-snapshot-id") not in (None, -1) else {}
    return Health(
        data_files=stats.get("data_files", 0), delete_files=stats.get("delete_files", 0),
        records=stats.get("records", 0), total_bytes=stats.get("data_bytes", 0),
        snapshots=len(snapshots), oldest_snapshot_hours=(now_ms - oldest) / 3_600_000 if oldest else 0.0,
        manifests=stats.get("manifests", 0))


def policies(polaris: Polaris, catalog: str, namespace: str, table: str) -> dict[str, dict]:
    """The effective policy per type (Polaris already applies table > namespace > catalog)."""
    url = polaris.policy_url(catalog, f"/applicable-policies?namespace={namespace}&target-name={table}")
    found = expect(polaris.rest("GET", url), 200)["applicable-policies"]
    return {p["policy-type"]: json.loads(p["content"]) for p in found}


def plan(h: Health, effective: dict[str, dict]) -> list[Action]:
    actions = []
    compaction = effective.get("system.data-compaction", {})
    if compaction.get("enable") and h.data_files:
        cfg = compaction.get("config", {})
        target = int(cfg.get("target-file-size-bytes", 128 * 1024 * 1024))
        min_files = int(cfg.get("min-input-files", 5))
        delete_threshold = int(cfg.get("delete-file-threshold", 1))
        small_files = h.data_files >= min_files and h.avg_file_bytes < target / 2
        if small_files or h.delete_files >= delete_threshold:
            actions.append(Action("compact", {"target-file-size-bytes": target, "min-input-files": min_files,
                                              "delete-file-threshold": delete_threshold}))
    metadata = effective.get("system.metadata-compaction", {})
    if metadata.get("enable") and h.manifests > int(metadata.get("config", {}).get("max-manifests", 100)):
        actions.append(Action("rewrite_manifests"))
    expiry = effective.get("system.snapshot-expiry", {})
    if expiry.get("enable"):
        cfg = expiry.get("config", {})
        keep = int(cfg.get("min-snapshots-to-keep", 5))
        max_age = float(cfg.get("max-snapshot-age-hours", 168))
        if h.snapshots > keep and h.oldest_snapshot_hours > max_age:
            actions.append(Action("expire_snapshots", {"max_age_hours": max_age, "retain_last": keep}))
    orphans = effective.get("system.orphan-file-removal", {})
    if orphans.get("enable"):
        actions.append(Action("remove_orphans", {"max_age_days": float(orphans.get("max_orphan_file_age_in_days", 3))}))
    return actions


def list_table_files(s: Settings, polaris: Polaris, catalog: str, namespace: str, table: str) -> list[tuple[str, datetime]]:
    """Every object under the table's location, listed with credentials vended for this table."""
    body = polaris.load_table(catalog, namespace, table, vended=True)
    s3 = s.s3(vended_s3(body))
    bucket, prefix = body["metadata"]["location"].removeprefix("s3://").split("/", 1)
    files = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix.rstrip("/") + "/"):
        files += [(f"s3://{bucket}/{o['Key']}", o["LastModified"]) for o in page.get("Contents", [])]
    return files


# ---------------------------------------------------------------- running

def maintain_table(s: Settings, executor: Executor, catalog: str, namespace: str, table: str,
                   _listing_hook=None) -> TableRun:
    """Plan and run maintenance for one table under its writer lock.

    `_listing_hook` (tests only) may rewrite the orphan listing, for example to age fresh files.
    """
    polaris = Polaris(s, PRINCIPAL)
    name = f"{catalog}.{namespace}.{table}"
    # First, every file the table references must exist (loader/integrity.py). A table that lost one
    # is repaired this run instead of maintained; the next run maintains it.
    from loader import integrity

    report = integrity.check(s, catalog, namespace, table)
    if not report.ok:
        log.error("integrity: %s references %d missing files", name, len(report.missing))
        try:
            repaired = integrity.repair(s, catalog, namespace, table)
            run = TableRun(name, [], health(polaris, executor, catalog, namespace, table))
            run.skipped = "integrity repair this run; maintenance on the next"
        except integrity.NeedsOperator as err:
            repaired = {"repaired": False, "needs_operator": str(err)}
            run = TableRun(name, [], Health(0, 0, 0, 0, 0, 0.0, 0))
            run.skipped = "integrity: needs an operator (loader.cli repair --recreate)"
        run.results["integrity"] = {**repaired, "missing_files": report.missing[:20]}
        record(s, run)
        return run
    before = health(polaris, executor, catalog, namespace, table)
    run = TableRun(name, plan(before, policies(polaris, catalog, namespace, table)), before)
    if not run.actions:
        return run
    try:
        with table_lock(s, name, wait_seconds=0):  # never queue behind a writer; next run retries
            specs = action_specs(s, polaris, catalog, namespace, table, run, _listing_hook)
            results = executor.run(name, [spec for _, spec in specs]) if specs else []
            for (kind, _), result in zip(specs, results):
                run.results[kind] = result
    except LockHeld:
        run.skipped = "table locked by a writer; retried next run"
        record(s, run)
        return run
    run.after = health(polaris, executor, catalog, namespace, table)
    record(s, run)
    return run


def action_specs(s: Settings, polaris: Polaris, catalog: str, namespace: str, table: str, run: TableRun,
                 listing_hook=None) -> list[tuple[str, dict]]:
    """The planned actions as maintenance_ops specs. Unsafe orphan windows are refused here."""
    now, specs = datetime.now(timezone.utc), []
    for action in run.actions:
        if action.kind == "compact":
            specs.append(("compact", {"kind": "compact", "options": action.options}))
        elif action.kind == "rewrite_manifests":
            specs.append(("rewrite_manifests", {"kind": "rewrite_manifests"}))
        elif action.kind == "expire_snapshots":
            older = now - timedelta(hours=action.options["max_age_hours"])
            specs.append(("expire_snapshots", {"kind": "expire_snapshots", "older_than": f"{older:%Y-%m-%d %H:%M:%S}",
                                               "retain_last": action.options["retain_last"]}))
        elif action.kind == "remove_orphans":
            window = timedelta(days=action.options["max_age_days"])
            if window < MIN_ORPHAN_AGE:
                run.results["remove_orphans"] = {"refused": "orphan window shorter than one day"}
                continue
            listing = list_table_files(s, polaris, catalog, namespace, table)
            if listing_hook:
                listing = listing_hook(listing)
            specs.append(("remove_orphans", orphan_spec(now - window, listing)))
    return specs


def orphan_spec(older_than: datetime, listing: list[tuple[str, datetime]], dry_run: bool = False) -> dict:
    return {"kind": "remove_orphans", "older_than": f"{older_than:%Y-%m-%d %H:%M:%S}", "dry_run": dry_run,
            "listing": [[path, when.isoformat()] for path, when in listing]}


def record(s: Settings, run: TableRun) -> None:
    s.dynamodb().put_item(TableName=RUNS, Item={
        "run_id": {"S": f"{run.table}@{datetime.now(timezone.utc).isoformat()}"}, "table_name": {"S": run.table},
        "actions": {"S": json.dumps([asdict(a) for a in run.actions])},
        "before": {"S": json.dumps(asdict(run.before))},
        "after": {"S": json.dumps(asdict(run.after)) if run.after else "null"},
        "results": {"S": json.dumps(run.results, default=str)}, "skipped": {"S": run.skipped}})


def tables(s: Settings) -> list[tuple[str, str, str]]:
    polaris, out = Polaris(s, PRINCIPAL), []
    for catalog, namespaces in LAYOUT.items():
        for namespace in namespaces:
            status, body = polaris.rest("GET", polaris.table_url(catalog, namespace) + "/tables")
            if status == 200:
                out += [(catalog, namespace, i["name"]) for i in body.get("identifiers", [])]
    return out


def maintain_all(s: Settings, chosen_executor: Executor | None = None, only: list[str] | None = None) -> list[TableRun]:
    """Maintain every table, or only those named `catalog.namespace.table` in `only`."""
    run_with = chosen_executor or executor(s)
    chosen = [t for t in tables(s) if not only or ".".join(t) in only]
    return [maintain_table(s, run_with, *t) for t in chosen]
