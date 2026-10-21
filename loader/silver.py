"""Silver: current state per contract semantics, one single-table transaction per batch.

All three semantics are idempotent: rerunning a batch leaves Silver as it was after the first run.
Window bounds come from a subquery over Bronze, so no data value is ever written into SQL text.
"""
from loader import duck, integrity, tables
from loader.contracts import Contract
from loader.settings import Settings
from loader.state import Batch


def pk_bucket(contract: Contract, alias: str = "") -> str:
    """Stable md5-based bucket, never DuckDB's hash(), which may change between versions."""
    parts = ", ".join(f"CAST({alias}{k} AS VARCHAR)" for k in contract.primary_key)
    return f"CAST(md5_number(concat_ws('|', {parts})) % {tables.SILVER_BUCKETS} AS INTEGER)"


def statements(contract: Contract, batch: Batch) -> list[str]:
    bronze = tables.qualified(contract, tables.BRONZE)
    silver = tables.qualified(contract, tables.SILVER)
    kept = [name for name, _ in tables.data_columns(contract)]
    cols = kept + ["pk_bucket", "_batch_id", "_ingested_at"]
    src = (f"(SELECT {', '.join(kept)}, {pk_bucket(contract)} AS pk_bucket, _batch_id, _ingested_at "
           f"FROM {bronze} WHERE _batch_id = {duck.sql_str(batch.batch_id)})")
    insert = f"INSERT INTO {silver} ({', '.join(cols)}) SELECT {', '.join(cols)} FROM {src}"
    semantics = contract.silver.semantics
    if semantics == "snapshot":
        return [f"DELETE FROM {silver}", insert]
    if semantics == "window":
        # Replace the batch's range, and every key the batch carries wherever it was: a correction
        # can move a row's date out of the range (a claim's service date fixed), and its old version
        # must not stay behind next to the new one (found by tests/unit/test_silver_properties.py).
        w = contract.silver.window_column
        key_tuple = ", ".join(["pk_bucket"] + contract.primary_key)
        return [f"DELETE FROM {silver} WHERE {w} BETWEEN (SELECT min({w}) FROM {src}) "
                f"AND (SELECT max({w}) FROM {src}) OR ({key_tuple}) IN (SELECT {key_tuple} FROM {src})", insert]
    if semantics == "cdc":
        # The latest change per key in this batch replaces Silver's row only if it is newer, so
        # replays and redelivered events are harmless. A DELETE then an INSERT in one transaction:
        # DuckDB 2.0 accepts this as one multi-action MERGE, but that was 15% slower per change
        # batch (bench/run.py silver_cdc: 6.6 s against 5.7 s, 10 batches each).
        # Both compare log positions, so neither depends on seeing the other's effect.
        op, order = contract.silver.op_column, contract.silver.order_column
        keys = contract.primary_key
        latest = (f"(SELECT * EXCLUDE (_rank) FROM (SELECT *, row_number() OVER (PARTITION BY {', '.join(keys)} "
                  f"ORDER BY {order} DESC) AS _rank FROM {src}) WHERE _rank = 1)")
        match = " AND ".join(["c.pk_bucket = t.pk_bucket"] + [f"c.{k} = t.{k}" for k in keys])
        key_tuple = ", ".join(["pk_bucket"] + keys)
        return [
            f"DELETE FROM {silver} WHERE ({key_tuple}) IN (SELECT {', '.join('t.' + k for k in ['pk_bucket'] + keys)} "
            f"FROM {silver} t JOIN {latest} c ON {match} WHERE c.{order} > t.{order})",
            f"INSERT INTO {silver} ({', '.join(cols)}) SELECT {', '.join('c.' + col for col in cols)} FROM {latest} c "
            f"WHERE c.{op} <> 'D' AND NOT EXISTS (SELECT 1 FROM {silver} t WHERE {match} AND t.{order} >= c.{order})",
        ]
    # append: insert keys Silver does not have yet; pk_bucket in ON prunes the target.
    on = " AND ".join(["t.pk_bucket = s.pk_bucket"] + [f"t.{k} = s.{k}" for k in contract.primary_key])
    return [f"MERGE INTO {silver} t USING {src} s ON {on} WHEN NOT MATCHED THEN "
            f"INSERT ({', '.join(cols)}) VALUES ({', '.join('s.' + c for c in cols)})"]


def apply(s: Settings, contract: Contract, batch: Batch) -> int:
    """Apply one Bronze batch to Silver. Returns Silver's row count afterwards."""
    connect = lambda: duck.connect(s, tables.CATALOG, principal=contract.principal)  # noqa: E731
    duck.run_txn(connect, statements(contract, batch),
                 heal=lambda: integrity.heal(s, tables.CATALOG, tables.SILVER, contract.table))
    return connect().execute(f"SELECT count(*) FROM {tables.qualified(contract, tables.SILVER)}").fetchone()[0]
