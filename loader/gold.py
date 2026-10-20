"""Gold: dbt models on DuckDB, written through Polaris as the `gold` principal.

deploy() (as root) creates each model's Iceberg table before dbt first writes it: the column types
come from DESCRIBE of the model's compiled SQL, and the properties from the layer rules (Gold
is v2; the PHI catalog keeps metrics at `counts`). run() then lets dbt replace each
table's contents in one transaction (materialization iceberg_replace).
"""
import re
import tempfile
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

    @property
    def qualified(self) -> str:
        return f"{self.catalog}.{NAMESPACE}.{self.name}"


def ensure_access(root: Polaris, s: Settings) -> None:
    """The gold principal reads the PHI catalog (Silver) and writes only the gold namespaces."""
    for catalog in CATALOGS:
        expect(root.rest("POST", f"{s.polaris_uri}/v1/{catalog}/namespaces", {"namespace": [NAMESPACE]}), 200, 409)
        admin.ensure_catalog_role(root, catalog, "gold_writer", [
            {"type": "namespace", "namespace": [NAMESPACE], "privilege": p} for p in WRITE])
    admin.ensure_principal(root, s, PRINCIPAL, {"phi": ["reader", "gold_writer"], "deid": ["gold_writer"]})


def extension_options(s: Settings) -> dict:
    """Load DuckDB extensions only from the directory baked into the image (never download code)."""
    return {"autoinstall_known_extensions": False, "autoload_known_extensions": False,
            **({"extension_directory": s.duckdb_extension_dir} if s.duckdb_extension_dir else {})}


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
            "endpoint": s.polaris_uri, "secret": "polaris_gold", "disable_multi_table_commit": True}}
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


def models(s: Settings, profiles: Path) -> list[Model]:
    """Every model, compiled, in dependency order: a model comes after the models it reads, so deploy
    can create each table before describing a model that reads it."""
    import json

    _dbt(["compile"], profiles)
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
        ordered.append(Model(n["name"], n["database"], n["compiled_code"], n["config"].get("meta", {})))

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
    from loader.deploy import ensure_table_columns

    root = Polaris(s)
    ensure_access(root, s)
    with tempfile.TemporaryDirectory() as tmp:
        write_profile(s, Path(tmp))
        found = models(s, Path(tmp))
    con = duck.connect(s, "phi", principal=PRINCIPAL, alias="phi")
    duck.attach_polaris(con, s, "deid", principal=PRINCIPAL, alias="deid")
    for model in found:
        cols = [(name, iceberg_type(typ)) for name, typ, *_ in con.execute(f"DESCRIBE ({model.sql})").fetchall()]
        try:
            ensure_table_columns(root, model.catalog, NAMESPACE, model.name, cols, properties(model), partition=None)
        except PolarisError as err:
            raise GoldFailed(f"deploy of {model.qualified} failed (HTTP {err.status})") from None
    return found


def run(s: Settings) -> list[Model]:
    """Rebuild every Gold table, holding each table's writer lock (maintenance and Gold
    never write the same table at once). Returns the models in run order."""
    from contextlib import ExitStack

    from loader.state import table_lock

    with tempfile.TemporaryDirectory() as tmp:
        write_profile(s, Path(tmp))
        found = models(s, Path(tmp))
        with ExitStack() as locks:
            for model in sorted(found, key=lambda m: m.qualified):  # one order: no lock cycles
                locks.enter_context(table_lock(s, model.qualified))
            _dbt(["run"], Path(tmp))
    return found
