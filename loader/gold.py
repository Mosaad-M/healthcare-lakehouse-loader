"""Gold: dbt models on DuckDB, written through Polaris as the `gold` principal.

deploy() (as root) creates each model's Iceberg table before dbt first writes it: the column types
come from DESCRIBE of the model's compiled SQL, and the properties from the layer rules (Gold
is v2; the PHI catalog keeps metrics at `counts`). run() then lets dbt replace each
table's contents in one transaction (materialization iceberg_replace).
"""
import hashlib
import os
import re
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

import yaml

from loader import admin, duck
from loader.polaris import Polaris, PolarisError, expect
from loader.settings import Settings

PROJECT = Path(__file__).parents[1] / "dbt"
NAMESPACE = "gold"
PRINCIPAL = "gold"
CATALOGS = ("phi", "deid")
WRITE = ["TABLE_LIST", "TABLE_READ_DATA", "TABLE_WRITE_DATA", "TABLE_READ_PROPERTIES"]


class GoldFailed(RuntimeError):
    pass


@dataclass
class Model:
    name: str
    catalog: str
    sql: str
    meta: dict
    materialized: str = ""

    @property
    def qualified(self) -> str:
        return f"{self.catalog}.{NAMESPACE}.{self.name}"


def source_tables() -> set[tuple[str, str, str]]:
    """(catalog, namespace, table) of every dbt source: the only lake tables Gold reads."""
    return {(n["database"], n["schema"], n["name"]) for n in manifest()["sources"].values()}


def ensure_access(root: Polaris, s: Settings) -> None:
    """The gold principal writes only the gold namespaces and reads only its dbt sources, table by
    table (`gold_reader`): not the whole PHI catalog, every partner's Bronze included. A source
    table that does not exist fails here, and grants for tables no source uses any more are
    revoked."""
    for catalog in CATALOGS:
        expect(root.rest("POST", f"{s.polaris_uri}/v1/{catalog}/namespaces", {"namespace": [NAMESPACE]}), 200, 409)
        admin.ensure_catalog_role(root, catalog, "gold_writer", [
            {"type": "namespace", "namespace": [NAMESPACE], "privilege": p} for p in WRITE])
    for catalog in CATALOGS:
        wanted = [{"type": "catalog", "privilege": p} for p in ("NAMESPACE_LIST", "TABLE_LIST")] + [
            {"type": "table", "namespace": [namespace], "tableName": table, "privilege": "TABLE_READ_DATA"}
            for cat, namespace, table in sorted(source_tables()) if cat == catalog]
        admin.ensure_catalog_role(root, catalog, "gold_reader", wanted)
        for grant in expect(root.mgmt("GET", f"/catalogs/{catalog}/catalog-roles/gold_reader/grants"), 200)["grants"]:
            if grant not in wanted:
                expect(root.mgmt("POST", f"/catalogs/{catalog}/catalog-roles/gold_reader/grants", {"grant": grant}), 201)
    admin.ensure_principal(root, s, PRINCIPAL, {c: ["gold_reader", "gold_writer"] for c in CATALOGS})
    # Earlier deploys gave the whole-catalog reader role (Polaris answers 400 for one not held).
    held = expect(root.mgmt("GET", f"/principal-roles/{PRINCIPAL}_role/catalog-roles/phi"), 200)["roles"]
    if "reader" in {r["name"] for r in held}:
        expect(root.mgmt("DELETE", f"/principal-roles/{PRINCIPAL}_role/catalog-roles/phi/reader"), 204)


def extension_options(s: Settings) -> dict:
    """Load DuckDB extensions only from the directory baked into the image (never download code)."""
    return {"autoinstall_known_extensions": False, "autoload_known_extensions": False,
            **({"extension_directory": s.duckdb_extension_dir} if s.duckdb_extension_dir else {}),
            **duck.strict_settings()}


def write_profile(s: Settings, directory: Path) -> None:
    client_id, secret = s.principal(PRINCIPAL)
    host, ssl = duck.unscoped_endpoint(s)
    profile = {"lakehouse_gold": {"target": "local", "outputs": {"local": {
        "type": "duckdb", "path": ":memory:", "threads": 1,
        "extensions": ["httpfs", "aws", "iceberg", "avro"],
        # At connect time (config_options), not per cursor (settings): dbt-duckdb installs the
        # extensions right after connecting, so with `settings` alone it downloaded them from the
        # internet on every run, ignoring the baked directory (found on AWS once egress allowed HTTPS only).
        "config_options": extension_options(s),
        "settings": extension_options(s),
        "secrets": [
            {"type": "iceberg", "name": "polaris_gold", "client_id": client_id, "client_secret": secret,
             "oauth2_server_uri": s.token_uri, "oauth2_scope": "PRINCIPAL_ROLE:ALL"},
            # Fail closed, as in loader/duck.py: storage requests without a vended secret are refused.
            {"type": "s3", "name": "deny_unscoped", "scope": "s3://", "key_id": "deny", "secret": "deny",
             "endpoint": host, "url_style": "path", "use_ssl": ssl, "region": "us-east-1"},
        ],
        "attach": [{"path": c, "alias": c, "type": "iceberg", "options": {
            duck.catalog_url_option(): s.polaris_uri, "secret": "polaris_gold", "disable_multi_table_commit": True}}
            for c in CATALOGS],
    }}}}
    (directory / "profiles.yml").write_text(yaml.safe_dump(profile))


def _dbt(args: list[str], profiles: Path):
    from dbt.cli.main import dbtRunner

    result = dbtRunner().invoke([*args, "--project-dir", str(PROJECT), "--profiles-dir", str(profiles),
                                 # Build output and logs in the run's temp directory: the project
                                 # directory is read-only to the (non-root) loader in its image.
                                 "--target-path", str(profiles / "target"), "--log-path", str(profiles / "logs"),
                                 "--log-level", "warn", "--quiet"])
    if not result.success:
        failed = [r.node.name for r in (result.result or []) if getattr(r, "status", "") in ("error", "fail")]
        raise GoldFailed(f"dbt {args[0]} failed" + (f" for {', '.join(failed)}" if failed else ""))
    return result.result


def models(s: Settings, profiles: Path, deploying: bool = False, vars: dict | None = None) -> list[Model]:
    """Every model, compiled, in dependency order: a model comes after the models it reads, so deploy
    can create each table before describing a model that reads it. `deploying` compiles with the
    `gold_deploy` var: a model that reads its own table (the incremental dimensions) then reads an
    empty, typed stand-in, since the table may not exist yet. `vars` are other dbt vars."""
    import json

    given = {**(vars or {}), **({"gold_deploy": True} if deploying else {})}
    _dbt(["compile", *(["--vars", json.dumps(given)] if given else [])], profiles)
    nodes = json.loads((profiles / "target" / "manifest.json").read_text())["nodes"]
    found = {n["unique_id"]: n for n in nodes.values() if n["resource_type"] == "model"}
    ordered, done = [], set()

    def visit(uid: str, path: tuple = ()) -> None:
        if uid in done:
            return
        if uid in path:
            raise GoldFailed(f"Gold models depend on each other in a cycle: {', '.join(path)}")
        parents = [p for p in found[uid]["depends_on"]["nodes"] if p in found]
        for parent in sorted(parents):
            visit(parent, (*path, uid))
        done.add(uid)
        n = found[uid]
        ordered.append(Model(n["name"], n["database"], n["compiled_code"], n["config"].get("meta", {}),
                             n["config"].get("materialized", "")))

    for uid in sorted(found):
        visit(uid)
    return ordered


def manifest() -> dict:
    """dbt's parsed manifest (models, sources and the dependency graph), without connecting to
    anything: the throwaway profile only satisfies dbt's need for one."""
    import json

    with tempfile.TemporaryDirectory() as tmp:
        profile = {"lakehouse_gold": {"target": "parse", "outputs": {"parse": {"type": "duckdb", "path": ":memory:"}}}}
        (Path(tmp) / "profiles.yml").write_text(yaml.safe_dump(profile))
        _dbt(["parse"], Path(tmp))
        return json.loads((Path(tmp) / "target" / "manifest.json").read_text())


ICEBERG = {"VARCHAR": "string", "BIGINT": "long", "INTEGER": "int", "DOUBLE": "double", "FLOAT": "float",
           "DATE": "date", "TIMESTAMP": "timestamp", "TIMESTAMP WITH TIME ZONE": "timestamptz",
           "BOOLEAN": "boolean", "BLOB": "binary"}


def iceberg_type(duckdb_type: str) -> str:
    match = re.fullmatch(r"DECIMAL\((\d+),\s*(\d+)\)", duckdb_type)
    if match:
        return f"decimal({match[1]}, {match[2]})"
    if duckdb_type not in ICEBERG:
        raise GoldFailed(f"type {duckdb_type} has no Iceberg mapping; cast it in the model")
    return ICEBERG[duckdb_type]


def properties(model: Model) -> dict[str, str]:
    props = {"format-version": "2", "write.parquet.compression-codec": "zstd", "loader.layer": "gold"}
    # De-identified tables may keep column statistics; PHI marts keep values out of metadata.
    props["write.metadata.metrics.default"] = "truncate(16)" if model.catalog == "deid" else "counts"
    return props


def deploy(s: Settings) -> list[Model]:
    """Create missing Gold tables (and add new columns). Runs as root."""
    from loader.deploy import attach_table_policy, ensure_table_columns

    root = Polaris(s)
    ensure_access(root, s)
    with tempfile.TemporaryDirectory() as tmp:
        write_profile(s, Path(tmp))
        found = models(s, Path(tmp), deploying=True)
    con = duck.connect(s, "phi", principal=PRINCIPAL, alias="phi")
    duck.attach_polaris(con, s, "deid", principal=PRINCIPAL, alias="deid")
    for model in found:
        cols = [(name, iceberg_type(typ)) for name, typ, *_ in con.execute(f"DESCRIBE ({model.sql})").fetchall()]
        try:
            ensure_table_columns(root, model.catalog, NAMESPACE, model.name, cols, properties(model), partition=None,
                                 drop_missing=True)
        except PolarisError as err:
            raise GoldFailed(f"deploy of {model.qualified} failed (HTTP {err.status})") from None
        if model.materialized == "iceberg_replace":
            # Rewritten whole on every changed run: a day of snapshots, not the 7-day default.
            attach_table_policy(root, model.catalog, NAMESPACE, model.name, "short_snapshot_expiry")
    return found


# A batch is visible to Gold only once it is in the manifest, a moment after its landing time is
# taken: a dimension's history never goes nearer to now than this, so a batch landing at that moment
# cannot be passed by a later one. LOADER_GOLD_SETTLE_SECONDS changes it (the tests set 0).
SETTLE_SECONDS = 300


class LateBatch(GoldFailed):
    """A batch of a history source reached the pipeline behind the history Gold already applied."""


def _when(value):
    """A landing time (manifest string or datetime) as an aware UTC datetime."""
    from datetime import datetime, timezone

    moment = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return moment.astimezone(timezone.utc)


def history_horizons(s: Settings, found: list[Model], now=None) -> dict[str, str]:
    """For each incremental dimension's source (meta.history_source): the landing time its run may
    not reach. That is the earliest batch still on its way to Bronze (landed, not yet loaded or
    quarantined), or now less SETTLE_SECONDS. A dimension applies batches in landing order and
    never again reads one before its watermark, so a batch still in flight must not be passed by a
    later one: it would never be applied.

    Fails loudly (LateBatch) if a batch in flight already landed at or before the history the
    dimension has applied (a clock behind another worker's, say): it can no longer be applied in order.
    """
    import os
    from datetime import datetime, timedelta, timezone

    from loader.state import LANDED, Manifest

    now = now or datetime.now(timezone.utc)
    settle = float(os.environ.get("LOADER_GOLD_SETTLE_SECONDS", SETTLE_SECONDS))
    horizons, con = {}, None
    for model in found:
        source = model.meta.get("history_source")
        if not source:
            continue
        pending = [b for b in Manifest(s).for_source(source) if b.state == LANDED]
        if pending:
            con = con or duck.connect(s, model.catalog, principal=PRINCIPAL, alias=model.catalog)
            applied = con.execute(f"""SELECT greatest(max(first_seen_at), max(valid_to))
                FROM {model.qualified} WHERE first_seen_at IS NOT NULL""").fetchone()[0]
            behind = [b for b in pending if applied is not None and _when(b.landed_at) <= _when(applied)]
            if behind:
                raise LateBatch(f"{model.qualified}: {len(behind)} batch(es) of {source} landed at or before the "
                                "history already applied and are not in Bronze yet; they would be skipped. Load "
                                "them, then rebuild the dimension (empty its table) if they change its history.")
        # Parsed and re-printed, so only a timestamp reaches the model's SQL.
        horizons[source] = min([_when(b.landed_at) for b in pending] + [now - timedelta(seconds=settle)]).isoformat()
    return horizons


RUNS = "gold_runs"  # DynamoDB: the inputs of the last completed run


def inputs(s: Settings, found: list[Model]) -> tuple[str, float]:
    """A fingerprint of everything a run reads (the compiled models and each source table's current
    snapshot), and when the newest of those snapshots was committed (epoch seconds)."""
    reader, parts, newest = Polaris(s, PRINCIPAL), [m.sql for m in found], 0.0
    for catalog, namespace, table in sorted(source_tables()):
        meta = reader.load_table(catalog, namespace, table)["metadata"]
        current = meta.get("current-snapshot-id")
        parts.append(f"{catalog}.{namespace}.{table}={current}")
        newest = max([newest] + [snap["timestamp-ms"] / 1000 for snap in meta.get("snapshots", [])
                                 if snap["snapshot-id"] == current])
    return hashlib.sha256("\n".join(parts).encode()).hexdigest(), newest


def run(s: Settings, force: bool = False) -> list[Model]:
    """Rebuild every Gold table, holding each table's writer lock (maintenance and Gold
    never write the same table at once). Returns the models in run order, or [] when nothing Gold
    reads has changed since the last run (unless `force`): the rewritten tables are not copied again.
    A source snapshot newer than the last run's start less the settling time could hold a batch the
    dimensions' horizon left out then, so it still runs."""
    import json
    from contextlib import ExitStack

    from loader.state import table_lock

    started = time.time()
    settle = float(os.environ.get("LOADER_GOLD_SETTLE_SECONDS", SETTLE_SECONDS))
    with tempfile.TemporaryDirectory() as tmp:
        write_profile(s, Path(tmp))
        found = models(s, Path(tmp))
        fingerprint, newest = inputs(s, found)
        last = s.dynamodb().get_item(TableName=RUNS, Key={"name": {"S": "last"}}, ConsistentRead=True).get("Item")
        if (not force and last and last["inputs"]["S"] == fingerprint
                and newest < float(last["started_at"]["N"]) - settle):
            return []
        with ExitStack() as locks:
            for model in sorted(found, key=lambda m: m.qualified):  # one order: no lock cycles
                locks.enter_context(table_lock(s, model.qualified))
            horizons = history_horizons(s, found)
            _dbt(["run", "--vars", json.dumps({"history_horizon": horizons})], Path(tmp))
    s.dynamodb().put_item(TableName=RUNS, Item={"name": {"S": "last"}, "inputs": {"S": fingerprint},
                                                "started_at": {"N": str(started)}})
    return found
