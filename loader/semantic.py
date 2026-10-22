"""The semantic layer: metrics defined once in dbt (dbt/models/schema.yml, dbt's v2 YAML), turned
into SQL by MetricFlow and run on Gold with DuckDB as the `metrics` principal.

Results are NOT suppressed. Metrics are computed from person-level Gold (`phi.gold`), so a result
can describe one person (a group of one); it is PHI, for roles granted person-level data only.
De-identified users keep the suppressed marts in Doris. MetricFlow does not run on Doris.

Queries name metrics and group-bys only: MetricFlow resolves every name against the definitions and
refuses unknown ones, so no caller text reaches the SQL. (Filters, which MetricFlow takes as SQL,
are not offered.)
"""
import functools
import json
import tempfile
from pathlib import Path

import yaml

from loader import admin, duck
from loader.polaris import Polaris, expect
from loader.settings import Settings

PRINCIPAL = "metrics"
ROLE = "metrics_reader"


@functools.cache
def definitions() -> dict:
    """dbt's semantic manifest (semantic models, metrics, the time spine), from a `dbt parse` that
    connects to nothing."""
    from loader import gold

    with tempfile.TemporaryDirectory() as tmp:
        profile = {"lakehouse_gold": {"target": "parse", "outputs": {"parse": {"type": "duckdb", "path": ":memory:"}}}}
        (Path(tmp) / "profiles.yml").write_text(yaml.safe_dump(profile))
        gold._dbt(["parse"], Path(tmp))
        return json.loads((Path(tmp) / "target" / "semantic_manifest.json").read_text())


def tables(manifest: dict | None = None) -> set[tuple[str, str, str]]:
    """(catalog, namespace, table) of every table a metric query can read: the semantic models'
    tables and the time spine."""
    manifest = manifest or definitions()
    relations = [m["node_relation"] for m in manifest["semantic_models"]]
    relations += [t["node_relation"] for t in manifest["project_configuration"].get("time_spines", [])]
    return {(r["database"], r["schema_name"], r["alias"]) for r in relations}


def ensure_access(root: Polaris, s: Settings) -> None:
    """The metrics principal reads exactly the tables metric queries read, table by table, and
    nothing else: not Silver or Bronze, not the marts. Grants for tables no metric uses any more
    are revoked."""
    from loader.gold import CATALOGS

    for catalog in CATALOGS:
        wanted = [{"type": "catalog", "privilege": p} for p in ("NAMESPACE_LIST", "TABLE_LIST")] + [
            {"type": "table", "namespace": [namespace], "tableName": table, "privilege": "TABLE_READ_DATA"}
            for cat, namespace, table in sorted(tables()) if cat == catalog]
        admin.ensure_catalog_role(root, catalog, ROLE, wanted)
        for grant in expect(root.mgmt("GET", f"/catalogs/{catalog}/catalog-roles/{ROLE}/grants"), 200)["grants"]:
            if grant not in wanted:
                expect(root.mgmt("POST", f"/catalogs/{catalog}/catalog-roles/{ROLE}/grants", {"grant": grant}), 201)
    admin.ensure_principal(root, s, PRINCIPAL, {c: [ROLE] for c in CATALOGS})


class _DuckDb:
    """What MetricFlow needs to know about the engine to write SQL for it (it runs nothing)."""

    @property
    def sql_engine_type(self):
        from metricflow.protocols.sql_client import SqlEngine

        return SqlEngine.DUCKDB

    @functools.cached_property
    def sql_plan_renderer(self):
        from metricflow.sql.render.duckdb_renderer import DuckDbSqlPlanRenderer

        return DuckDbSqlPlanRenderer()


@functools.cache
def _engine():
    from metricflow.engine.metricflow_engine import MetricFlowEngine
    from metricflow_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
    from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup

    lookup = SemanticManifestLookup(PydanticSemanticManifest.parse_obj(definitions()))
    return MetricFlowEngine(semantic_manifest_lookup=lookup, sql_client=_DuckDb())


def sql(metrics: list[str], group_by: list[str] = (), order_by: list[str] = (), limit: int | None = None) -> str:
    """The SQL for `metrics` by `group_by` (names such as metric_time__month or
    provider_version__specialty). Unknown names raise. A `limit` needs an `order_by`: without one
    it would keep arbitrary groups, and DuckDB ignores it on Gold tables written whole anyway
    (README, DuckDB), returning every group."""
    from metricflow.engine.metricflow_engine import MetricFlowQueryRequest

    if limit is not None and not order_by:
        raise ValueError("a limit needs an order_by")

    request = MetricFlowQueryRequest.create(metric_names=list(metrics), group_by_names=list(group_by),
                                            order_by_names=list(order_by) or None, limit=limit)
    statement = _engine().explain(request).sql_statement
    if statement.bind_parameter_set.param_items:
        raise ValueError("metric SQL with bind parameters is not supported")
    return statement.sql


def query(s: Settings, metrics: list[str], group_by: list[str] = (), order_by: list[str] = (),
          limit: int | None = None) -> tuple[list[str], list[tuple]]:
    """Run a metric query as the metrics principal. Returns (column names, rows). PHI: see above."""
    text = sql(metrics, group_by, order_by, limit)
    con = duck.connect(s, "phi", principal=PRINCIPAL, alias="phi")
    duck.attach_polaris(con, s, "deid", principal=PRINCIPAL, alias="deid")
    cur = con.execute(text)
    return [d[0] for d in cur.description], cur.fetchall()
