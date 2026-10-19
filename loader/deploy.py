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
    if updates:
        root.update_table(catalog, namespace, table, updates)


def ensure_table(root: Polaris, contract: Contract, layer: str) -> None:
    ensure_table_columns(root, tables.CATALOG, layer, contract.table, tables.columns(contract, layer),
                         tables.properties(contract, layer), partition=layer if layer == tables.BRONZE else None)


def ensure_source_principal(root: Polaris, s: Settings, contract: Contract) -> None:
    role = contract.principal
    grants = [{"type": "namespace", "namespace": [layer], "privilege": p}
              for layer in (tables.BRONZE, tables.SILVER) for p in NAMESPACE_PRIVILEGES]
    grants += [{"type": "table", "namespace": [layer], "tableName": contract.table, "privilege": p}
               for layer in (tables.BRONZE, tables.SILVER) for p in TABLE_PRIVILEGES]
    admin.ensure_catalog_role(root, tables.CATALOG, role, grants)
    admin.ensure_principal(root, s, contract.principal, {tables.CATALOG: [role]})


def deploy_contract(root: Polaris, s: Settings, contract: Contract) -> None:
    for layer in (tables.BRONZE, tables.SILVER):
        ensure_namespace(root, layer)
        ensure_table(root, contract, layer)
    ensure_source_principal(root, s, contract)
