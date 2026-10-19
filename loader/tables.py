"""Iceberg table layouts derived from a contract: columns, partitioning and properties per layer."""
from loader.contracts import Contract, Phi

CATALOG = "phi"  # every source lands in the PHI catalog; de-identified Gold goes to `deid`
BRONZE, SILVER = "bronze", "silver"
SILVER_BUCKETS = 16  # pk_bucket values; Silver is not partitioned until a lookup need is measured

# Bookkeeping columns: never PHI, so they keep full metrics.
BRONZE_META = [("_batch_id", "string"), ("_source_file", "string"), ("_row_number", "long"),
               ("_ingested_at", "timestamptz")]
SILVER_META = [("pk_bucket", "int"), ("_batch_id", "string"), ("_ingested_at", "timestamptz")]


def data_columns(contract: Contract) -> list[tuple[str, str]]:
    """Contract columns as stored: identifiers are ciphertext (binary), plus the row's subject ID."""
    cols = [(c.name, "binary" if c.phi is Phi.IDENTIFIER else c.type) for c in contract.kept]
    return cols + ([("subject_id", "string")] if contract.subject else [])


def columns(contract: Contract, layer: str) -> list[tuple[str, str]]:
    return data_columns(contract) + (BRONZE_META if layer == BRONZE else SILVER_META)


def properties(contract: Contract, layer: str) -> dict[str, str]:
    """Metrics `counts` by default so no value reaches a manifest; non-PHI columns opt back
    in. Bronze is format v2, Silver v3 (deletion vectors)."""
    props = {
        "format-version": "2" if layer == BRONZE else "3",
        "write.metadata.metrics.default": "counts",
        "write.parquet.compression-codec": "zstd",
        "loader.source": contract.source,
        "loader.layer": layer,
    }
    open_columns = [c.name for c in contract.kept if c.phi is Phi.NONE]
    open_columns += [name for name, _ in (BRONZE_META if layer == BRONZE else SILVER_META)]
    for name in open_columns:
        props[f"write.metadata.metrics.column.{name}"] = "truncate(16)"
    return props


def iceberg_schema(cols: list[tuple[str, str]], schema_id: int = 0, existing: dict | None = None) -> dict:
    """Iceberg schema JSON. Existing field ids are kept; new columns get the next free ids."""
    existing = existing or {}
    next_id = max(existing.values(), default=0) + 1
    fields = []
    for name, typ in cols:
        if name in existing:
            fid = existing[name]
        else:
            fid, next_id = next_id, next_id + 1
        fields.append({"id": fid, "name": name, "required": False, "type": typ})
    return {"type": "struct", "schema-id": schema_id, "fields": fields}


def partition_spec(schema: dict, layer: str) -> dict:
    """Bronze: day(_ingested_at), never a PHI value. Silver: unpartitioned."""
    if layer != BRONZE:
        return {"spec-id": 0, "fields": []}
    source_id = next(f["id"] for f in schema["fields"] if f["name"] == "_ingested_at")
    return {"spec-id": 0, "fields": [
        {"source-id": source_id, "field-id": 1000, "transform": "day", "name": "_ingested_at_day"}]}


def qualified(contract: Contract, layer: str, alias: str = "ice") -> str:
    return f"{alias}.{layer}.{contract.table}"
