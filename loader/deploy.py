"""Turn a contract into catalog objects: tables, a principal for the source, and its grants.

Idempotent. Runs as Polaris root (bootstrap, or the deploy step on AWS). New columns in a contract
are added to existing tables; any other schema change is refused and needs a migration.
"""
from loader import admin, tables
from loader.contracts import Contract, canonical_type
from loader.polaris import Polaris, PolarisError, expect
from loader.settings import Settings

# What a source principal may do on its own two tables, and nothing else (one principal per
# pipeline). Listing is limited to the layer namespaces. TABLE_WRITE_DATA covers
# single-table commits (and, in Polaris, property writes too). Polaris's multi-table transactions
# endpoint would also need namespace-wide TABLE_CREATE, so sources commit one table at a time.
TABLE_PRIVILEGES = ["TABLE_READ_DATA", "TABLE_WRITE_DATA", "TABLE_READ_PROPERTIES"]
NAMESPACE_PRIVILEGES = ["TABLE_LIST", "NAMESPACE_READ_PROPERTIES"]


def ensure_namespace(root: Polaris, namespace: str) -> None:
    expect(root.rest("POST", f"{root.settings.polaris_uri}/v1/{tables.CATALOG}/namespaces",
                     {"namespace": [namespace]}), 200, 409)


def ensure_table_columns(root: Polaris, catalog: str, namespace: str, table: str, cols: list[tuple[str, str]],
                         props: dict[str, str], partition: str | None) -> None:
    """Create the table, or add new columns and set properties on an existing one. A type change
    or the format version (fixed at creation) is never changed here.

    partition: None (unpartitioned) or a layer name understood by tables.partition_spec.
    """
    status, body = root.rest("GET", root.table_url(catalog, namespace, table))
    if status == 404:
        schema = tables.iceberg_schema(cols)
        spec = tables.partition_spec(schema, partition) if partition else {"spec-id": 0, "fields": []}
        expect(root.rest("POST", root.table_url(catalog, namespace) + "/tables", {
            "name": table, "schema": schema, "partition-spec": spec, "properties": props}), 200)
        return
    if status != 200:
        raise PolarisError(status, body)
    meta = body["metadata"]
    current = next(sc for sc in meta["schemas"] if sc["schema-id"] == meta["current-schema-id"])
    existing = {f["name"]: f for f in current["fields"]}
    for name, typ in cols:
        if name in existing and canonical_type(existing[name]["type"]) != canonical_type(typ):
            raise ValueError(f"{namespace}.{table}.{name}: type change {existing[name]['type']} -> {typ} "
                             "needs a migration")
    updates = []
    if any(name not in existing for name, _ in cols):
        keep = [(f["name"], f["type"]) for f in current["fields"]]
        added = [(n, t) for n, t in cols if n not in existing]
        schema = tables.iceberg_schema(keep + added, schema_id=max(sc["schema-id"] for sc in meta["schemas"]) + 1,
                                       existing={f["name"]: f["id"] for f in current["fields"]})
        updates += [{"action": "add-schema", "schema": schema,
                     "last-column-id": max(f["id"] for f in schema["fields"])},
                    {"action": "set-current-schema", "schema-id": -1}]
    wanted = {k: v for k, v in props.items() if k != "format-version"}
    if any(meta.get("properties", {}).get(k) != v for k, v in wanted.items()):
        updates.append({"action": "set-properties", "updates": wanted})
    if partition:
        default = next(sp for sp in meta["partition-specs"] if sp["spec-id"] == meta["default-spec-id"])
        if not default["fields"]:
            # Partition evolution: new data uses the new spec; compaction rewrites older files into it.
            next_id = max(sp["spec-id"] for sp in meta["partition-specs"]) + 1
            updates += [{"action": "add-spec", "spec": tables.partition_spec(current, partition, spec_id=next_id)},
                        {"action": "set-default-spec", "spec-id": -1}]
    if updates:
        root.update_table(catalog, namespace, table, updates)


def ensure_table(root: Polaris, contract: Contract, layer: str) -> None:
    ensure_table_columns(root, tables.CATALOG, layer, contract.table, tables.columns(contract, layer),
                         tables.properties(contract, layer), partition=tables.layout(contract, layer))
    if layer == tables.SILVER and contract.silver.semantics in SHORT_SNAPSHOT_SEMANTICS:
        attach_table_policy(root, tables.CATALOG, layer, contract.table, "short_snapshot_expiry")


# Silver semantics whose batches rewrite much of the table: a week of snapshots would keep up to a
# week of copies, so they keep one day (stack/bootstrap/model.json table_policies).
SHORT_SNAPSHOT_SEMANTICS = ("snapshot", "window")
POLICY_NAMESPACE = "ops"


def attach_table_policy(root: Polaris, catalog: str, namespace: str, table: str, policy: str) -> None:
    """Attach a maintenance policy from the policy namespace to one table; Polaris resolves table >
    namespace > catalog, so it overrides the catalog default of the same type. A table that already
    has a table-level policy of that type (this one from an earlier deploy, or an operator's own
    override) keeps it: Polaris answers 409."""
    url = root.policy_url(catalog, f"/namespaces/{POLICY_NAMESPACE}/policies/{policy}/mappings")
    expect(root.rest("PUT", url, {"target": {"type": "table-like", "path": [namespace, table]}}), 204, 200, 409)


def ensure_source_principal(root: Polaris, s: Settings, contract: Contract) -> None:
    role = contract.principal
    grants = [{"type": "namespace", "namespace": [layer], "privilege": p}
              for layer in (tables.BRONZE, tables.SILVER) for p in NAMESPACE_PRIVILEGES]
    grants += [{"type": "table", "namespace": [layer], "tableName": contract.table, "privilege": p}
               for layer in (tables.BRONZE, tables.SILVER) for p in TABLE_PRIVILEGES]
    admin.ensure_catalog_role(root, tables.CATALOG, role, grants)
    admin.ensure_principal(root, s, contract.principal, {tables.CATALOG: [role]})


def deploy_contract(root: Polaris, s: Settings, contract: Contract) -> None:
    from loader import offboarding

    s.partner_keys().ensure(contract.partner)  # locally made here, once; on AWS Terraform made it
    offboarding.ensure_inbound(s, contract)  # the partner's own put-only drop identity (local)
    for layer in (tables.BRONZE, tables.SILVER):
        ensure_namespace(root, layer)
        ensure_table(root, contract, layer)
    ensure_source_principal(root, s, contract)
