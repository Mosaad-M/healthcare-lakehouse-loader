"""Bronze: typed, append-only records per contract, one batch per landed file.

Every check that can reject a file runs before anything is written, so a bad file is quarantined
with nothing committed. Identifier columns are encrypted per subject before the batch is staged. Rejection reasons name columns and counts, never values: DuckDB's own parse
errors echo the offending line, so they are reduced to their error class.
"""
import tempfile
from pathlib import Path

import duckdb
import pyarrow as pa

from loader import crypto, duck, integrity, raw, tables
from loader.contracts import Contract, Stream
from loader.identity import Identity
from loader.settings import Settings
from loader.shred import record_batch_subjects
from loader.state import Batch
from loader.vault import KeyVault


ENCRYPT_CHUNK_ROWS = 100_000


class Quarantine(Exception):
    """The file breaks its contract. The message is safe to log and store (no data values)."""


def encrypt_identifiers(con: duckdb.DuckDBPyConnection, s: Settings, contract: Contract, batch: Batch) -> list[str]:
    """Replace every identifier in `typed` with ciphertext under its subject's key, and add
    the row's subject ID. Returns the subject IDs the batch mentions.

    Plaintext identifiers exist only in this process's memory; the staged file holds ciphertext.
    """
    if not contract.subject:
        return []
    key = contract.subject.key
    empty = con.execute(f"SELECT count(*) FROM typed WHERE {' OR '.join(f'{k} IS NULL' for k in key)}").fetchone()[0]
    if empty:
        raise Quarantine(f"subject key ({', '.join(key)}) is empty in {empty} rows")
    as_text = ", ".join(f"CAST({k} AS VARCHAR)" for k in key)
    subjects = Identity(s).resolve(contract.source, [tuple(r) for r in con.execute(f"SELECT DISTINCT {as_text} FROM typed").fetchall()])
    keys = KeyVault(s).get_or_create(list(subjects.values()), contract.source, contract.partner)

    idents = [c.name for c in contract.identifiers]
    # Read the batch as Arrow (columnar, compact) and encrypt it a chunk at a time, so Python holds
    # per-value objects for one chunk only: whole-batch Python rows took about 1.8 KB per row and
    # ran a 10M-row batch out of an 8 GB task (benchmark).
    table = con.execute(f"SELECT _row_number, {as_text}, {', '.join(f'CAST({c} AS VARCHAR)' for c in idents)} "
                        "FROM typed").fetch_arrow_table()
    sids = sorted(set(subjects.values()))
    position = {sid: i for i, sid in enumerate(sids)}
    ciphers = [crypto.SubjectCipher(keys[sid], sid) for sid in sids]
    owners, cipher = [], {c: [] for c in idents}
    for chunk in table.to_batches(ENCRYPT_CHUNK_ROWS):
        key_values = zip(*(chunk.column(1 + j).to_pylist() for j in range(len(key))))
        owner = [position[subjects[k]] for k in key_values]
        owners.append(pa.array(owner, pa.int32()))
        for j, col in enumerate(idents):
            values = chunk.column(1 + len(key) + j).to_pylist()
            cipher[col].append(pa.array([None if v is None else ciphers[o].encrypt(col, v) for o, v in zip(owner, values)],
                                        pa.binary()))
    subject_ids = pa.DictionaryArray.from_arrays(pa.concat_arrays(owners) if owners else pa.array([], pa.int32()),
                                                 pa.array(sids, pa.string()))
    encrypted = pa.table({"_row_number": table.column(0).combine_chunks().cast(pa.int64()), "subject_id": subject_ids,
                          **{f"enc_{c}": pa.concat_arrays(v) if v else pa.array([], pa.binary())
                             for c, v in cipher.items()}})
    del table
    con.register("encrypted", encrypted)
    replace = ", ".join(f"e.enc_{c} AS {c}" for c in idents)
    con.execute(f"CREATE OR REPLACE TEMP TABLE typed AS SELECT t.* REPLACE ({replace}), "
                "CAST(e.subject_id AS VARCHAR) AS subject_id "
                "FROM typed t JOIN encrypted e USING (_row_number)")
    con.unregister("encrypted")
    return sids


def raw_secret(con: duckdb.DuckDBPyConnection, s: Settings, contract: Contract) -> None:
    """Raw-zone credentials scoped to this source's prefixes (loader/partners.py)."""
    from loader import partners

    con.execute(s.duckdb_s3_secret("raw_zone", f"s3://{s.bucket(raw.LOGICAL)}/",
                                   s.scoped_storage(partners.raw_scope(contract), contract.source)))


def reader(contract: Contract, uri: str) -> str:
    """A DuckDB table function for the landed file. Hive partitioning is off: the landed path holds
    `ingest_date=` and `batch_id=` segments, which must not become columns."""
    fmt = contract.format
    if fmt.type == "csv":
        return (f"read_csv({duck.sql_str(uri)}, header = {str(fmt.header).lower()}, "
                f"delim = {duck.sql_str(fmt.delimiter)}, all_varchar = true, hive_partitioning = false)")
    if fmt.type == "ndjson":
        return f"read_json({duck.sql_str(uri)}, format = 'newline_delimited', hive_partitioning = false)"
    if fmt.type == "json_pages":
        # A JSON array of API response bodies; each record is an element of `records_path`.
        return (f"(SELECT unnest(page.{fmt.records_path}, recursive := true) FROM "
                f"read_json({duck.sql_str(uri)}, format = 'array', hive_partitioning = false) page)")
    return f"read_parquet({duck.sql_str(uri)}, hive_partitioning = false)"


def stage(con: duckdb.DuckDBPyConnection, s: Settings, contract: Contract, batch: Batch, out: Path) -> int:
    """Validate the landed file and write the typed Bronze rows to a local Parquet file.

    Returns the row count. Raises Quarantine when the file breaks the contract.
    """
    zone = raw.RawZone.for_contract(s, contract)
    src = reader(contract, zone.uri(batch.raw_key))
    # Size caps before anything is loaded into memory: an oversized file would run the loader out of
    # memory, and a batch that kills its process is retried on every run.
    if batch.size and batch.size > contract.max_file_bytes:
        raise Quarantine(f"file is {batch.size} bytes; the contract allows {contract.max_file_bytes} (max_file_bytes)")
    try:
        streamed = con.execute(f"SELECT count(*) FROM {src}").fetchone()[0]
    except duckdb.Error as err:
        raise Quarantine(f"file could not be parsed as {contract.format.type} ({type(err).__name__})") from None
    if streamed > contract.max_rows:
        raise Quarantine(f"file has {streamed} rows; the contract allows {contract.max_rows} (max_rows)")
    try:
        con.execute(f"CREATE OR REPLACE TEMP TABLE landed AS SELECT * FROM {src}")
    except duckdb.Error as err:
        raise Quarantine(f"file could not be parsed as {contract.format.type} ({type(err).__name__})") from None

    present = {r[0] for r in con.execute("DESCRIBE landed").fetchall()}
    declared = {c.file_field for c in contract.columns}
    missing = sorted(c.name for c in contract.kept if c.file_field not in present)
    extra = sorted(present - declared)
    if missing:
        raise Quarantine(f"missing columns: {', '.join(missing)}")
    if extra:
        # An undeclared column may be new PHI: nothing is loaded until the contract classifies it.
        raise Quarantine(f"undeclared columns: {', '.join(extra)}")

    def text(c, alias=""):
        field = '"' + c.file_field.replace('"', '""') + '"'
        return f"nullif(trim(CAST({alias}{field} AS VARCHAR)), '')"

    typed = ", ".join(f"TRY_CAST({text(c)} AS {c.duckdb_type}) AS {c.name}" for c in contract.kept)
    con.execute(f"CREATE OR REPLACE TEMP TABLE typed AS SELECT {typed}, "
                f"row_number() OVER () AS _row_number FROM landed")
    rows = con.execute("SELECT count(*) FROM landed").fetchone()[0]
    if rows == 0:
        raise Quarantine("file has no rows")

    # Cast failures: a value was present but is not of the declared type.
    checks = ", ".join(
        f"count(*) FILTER (WHERE {text(c, 'l.')} IS NOT NULL AND t.{c.name} IS NULL)" for c in contract.kept)
    failures = con.execute(
        f"SELECT {checks} FROM (SELECT *, row_number() OVER () AS _row_number FROM landed) l "
        "JOIN typed t USING (_row_number)").fetchone()
    bad = [f"{c.name} ({n} rows)" for c, n in zip(contract.kept, failures) if n]
    if bad:
        raise Quarantine(f"values not of the declared type in: {', '.join(bad)}")

    if isinstance(contract.fetch, Stream):
        # At-least-once delivery can repeat an event within one file: identical copies are one event.
        # Copies that differ still count as repeats below.
        cols = ", ".join(c.name for c in contract.kept)
        con.execute(f"CREATE OR REPLACE TEMP TABLE typed AS SELECT * FROM typed "
                    f"QUALIFY row_number() OVER (PARTITION BY {cols} ORDER BY _row_number) = 1")

    pk = ", ".join(contract.primary_key)
    nulls = con.execute(f"SELECT count(*) FROM typed WHERE {' OR '.join(f'{k} IS NULL' for k in contract.primary_key)}").fetchone()[0]
    if nulls:
        raise Quarantine(f"primary key ({pk}) is empty in {nulls} rows")
    # A CDC batch holds several changes per key; each change is unique by key and log position.
    unique = pk + (f", {contract.silver.order_column}" if contract.silver.semantics == "cdc" else "")
    dupes = con.execute(f"SELECT count(*) FROM (SELECT {unique} FROM typed GROUP BY ALL HAVING count(*) > 1)").fetchone()[0]
    if dupes:
        raise Quarantine(f"({unique}) repeats for {dupes} keys")

    subject_ids = encrypt_identifiers(con, s, contract, batch)
    record_batch_subjects(s, batch.batch_id, subject_ids)
    kept = ", ".join(name for name, _ in tables.data_columns(contract))
    con.execute(
        f"COPY (SELECT {kept}, {duck.sql_str(batch.batch_id)} AS _batch_id, "
        f"{duck.sql_str(zone.uri(batch.raw_key))} AS _source_file, _row_number, "
        f"{duck.sql_str(batch.landed_at)}::TIMESTAMPTZ AS _ingested_at FROM typed ORDER BY _row_number) "
        f"TO {duck.sql_str(str(out))} (FORMAT parquet)")
    con.execute("DROP TABLE landed; DROP TABLE typed")
    return rows


def write(s: Settings, contract: Contract, batch: Batch) -> int:
    """Validate, then replace the batch in Bronze in one single-table transaction.

    Rerunning a batch deletes its earlier rows first, so reruns never duplicate.
    """
    # The staged batch holds the rows Bronze will (PHI, identifiers encrypted): it is written in the
    # process's private spill directory (mode 0700, removed at exit), not the shared temp directory.
    with tempfile.TemporaryDirectory(dir=duck.spill_directory()) as tmp:
        staged = Path(tmp) / "batch.parquet"
        con = duckdb.connect()
        duck.load_extensions(con, s)
        duck.deny_unscoped(con, s)
        raw_secret(con, s, contract)
        rows = stage(con, s, contract, batch, staged)

        table = tables.qualified(contract, tables.BRONZE)
        cols = ", ".join(name for name, _ in tables.columns(contract, tables.BRONZE))
        duck.run_txn(lambda: duck.connect(s, tables.CATALOG, principal=contract.principal), [
            f"DELETE FROM {table} WHERE _batch_id = {duck.sql_str(batch.batch_id)}",
            f"INSERT INTO {table} ({cols}) SELECT {cols} FROM read_parquet({duck.sql_str(str(staged))})",
        ], heal=lambda: integrity.heal(s, tables.CATALOG, tables.BRONZE, contract.table))
    return rows
