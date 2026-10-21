"""Iceberg table layouts derived from a contract: columns, partitioning and properties per layer."""
from loader.contracts import Contract, Phi

CATALOG = "phi"  # every source lands in the PHI catalog; de-identified Gold goes to `deid`
BRONZE, SILVER = "bronze", "silver"
SILVER_BUCKETS = 16  # pk_bucket values (fixed in the data: changing it rewrites the table)
# Silver semantics whose merges look keys up across the table: partitioned by pk_bucket, so a batch's
# merge reads only the buckets its keys fall in (the benchmark: random-key appends grew 0.13 s per
# batch per million rows in an unpartitioned Silver). snapshot and window rewrite by date or whole.
BUCKETED_SEMANTICS = ("append", "cdc")
SILVER_BUCKETED = "silver_bucketed"  # partition_spec layout name

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


def iceberg_schema(cols: list[tuple[str, str]], schema_id: int = 0, existing: dict | None = None,
                   last_column_id: int = 0) -> dict:
    """Iceberg schema JSON. Existing field ids are kept; new columns get ids above both the existing
    ones and the table's last-column-id: Iceberg matches data files to columns by id, so the id of a
    dropped column must never be given to a new one (its old values would read as the new column's)."""
    existing = existing or {}
    next_id = max([last_column_id, *existing.values()], default=0) + 1
    fields = []
    for name, typ in cols:
        if name in existing:
            fid = existing[name]
        else:
            fid, next_id = next_id, next_id + 1
        fields.append({"id": fid, "name": name, "required": False, "type": typ})
    return {"type": "struct", "schema-id": schema_id, "fields": fields}


def partition_spec(schema: dict, layer: str, spec_id: int = 0) -> dict:
    """Bronze: day(_ingested_at), never a PHI value. Silver for append and cdc (SILVER_BUCKETED): the
    pk_bucket value, a hash bucket, never a PHI value. Other Silver: unpartitioned."""
    if layer == BRONZE:
        source_id = next(f["id"] for f in schema["fields"] if f["name"] == "_ingested_at")
        return {"spec-id": spec_id, "fields": [
            {"source-id": source_id, "field-id": 1000, "transform": "day", "name": "_ingested_at_day"}]}
    if layer == SILVER_BUCKETED:
        source_id = next(f["id"] for f in schema["fields"] if f["name"] == "pk_bucket")
        return {"spec-id": spec_id, "fields": [
            {"source-id": source_id, "field-id": 1000, "transform": "identity", "name": "pk_bucket"}]}
    return {"spec-id": spec_id, "fields": []}


def layout(contract: Contract, layer: str) -> str | None:
    """The partition layout of a contract's table in a layer (partition_spec's names)."""
    if layer == BRONZE:
        return BRONZE
    return SILVER_BUCKETED if contract.silver.semantics in BUCKETED_SEMANTICS else None


def qualified(contract: Contract, layer: str, alias: str = "ice") -> str:
    return f"{alias}.{layer}.{contract.table}"
