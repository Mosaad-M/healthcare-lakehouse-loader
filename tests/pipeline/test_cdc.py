"""Change data capture end to end. Writes to the clinic's Postgres reach Silver through the
capture tool (AWS DMS on AWS; Debezium and cdc-sink locally), Parquet change files in the drop
prefix, and the `cdc` semantics (latest change per key wins; deletes apply; changes older than
Silver's row are ignored).

All captured changes go to the one `acme_clinic_patients` source, so each run uses its own range of
patient IDs and only looks at that range. Capture is asynchronous, so each test ingests until
Silver shows the expected state (or a timeout).
"""
import io
import os
import time

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from flows.ingest import ingest
from loader import contracts, duck, pipeline, raw, tables


@pytest.fixture(scope="module")
def clinic():
    return contracts.load_all()["acme_clinic_patients"]


@pytest.fixture(scope="module")
def ids(run_id) -> list[int]:
    base = int(run_id, 16) % 10**6 * 100
    return [base + i for i in range(5)]


def sql(statement: str, params=()) -> None:
    with psycopg.connect(host=os.environ.get("CLINIC_PG_HOST", "source-pg"), dbname="clinic",
                         user=os.environ.get("CLINIC_PG_USER", "postgres"),
                         password=os.environ["POSTGRES_PASSWORD"], autocommit=True) as con:
        con.execute(statement, params)


def silver_rows(s, clinic, ids) -> dict[int, tuple]:
    con = duck.connect(s, tables.CATALOG, principal=clinic.principal)
    rows = con.execute(f"SELECT patient_id, balance, plan_code, cdc_op, cdc_seq FROM "
                       f"{tables.qualified(clinic, tables.SILVER)} WHERE patient_id IN ({', '.join(map(str, ids))})").fetchall()
    return {r[0]: r[1:] for r in rows}


def ingest_until(s, clinic, ids, done, timeout: float = 300) -> dict[int, tuple]:
    """Ingest until `done(rows)` holds for Silver's rows of this run (capture lag is seconds locally,
    up to a minute or so with DMS)."""
    deadline = time.time() + timeout
    while True:
        ingest(source=clinic.source)
        rows = silver_rows(s, clinic, ids)
        if done(rows):
            return rows
        if time.time() > deadline:
            raise TimeoutError(f"Silver did not reach the expected state within {timeout} s: {rows}")
        time.sleep(5)


def test_inserts_arrive_in_silver(s, clinic, ids, canaries):
    for i, pid in enumerate(ids):
        name = canaries.name if i == 0 else f"Avery Fakeworth {i}"
        sql("INSERT INTO patients (patient_id, mrn, full_name, email, birth_year, zip3, plan_code, balance) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (pid, f"MRNCDC{pid}", name, f"p{pid}@example.invalid", 1950 + i, "021", "GOLD", 100 + i))
    rows = ingest_until(s, clinic, ids, lambda rows: sorted(rows) == ids)
    assert all(r[2] == "I" for r in rows.values())


def test_updates_and_deletes_apply_in_order(s, clinic, ids):
    sql("UPDATE patients SET balance = 1.00 WHERE patient_id = %s", (ids[1],))
    sql("UPDATE patients SET balance = 2.00 WHERE patient_id = %s", (ids[1],))  # the later one wins
    sql("UPDATE patients SET plan_code = 'SILVER' WHERE patient_id = %s", (ids[2],))
    sql("DELETE FROM patients WHERE patient_id = %s", (ids[3],))
    rows = ingest_until(s, clinic, ids, lambda rows: ids[3] not in rows and rows.get(ids[2], (0, ""))[1] == "SILVER"
                        and str(rows.get(ids[1], ("",))[0]) == "2.00")
    assert rows[ids[1]][2] == "U"
    assert len(rows) == 4


def test_an_older_change_does_not_overwrite_a_newer_row(s, clinic, ids):
    """A redelivered or out-of-order change (lower change sequence) is ignored."""
    before = silver_rows(s, clinic, ids)
    stale = {"Op": "U", "change_seq": str(before[ids[1]][3] - 1), "patient_id": ids[1], "mrn": f"MRNCDC{ids[1]}",
             "full_name": "Avery Fakeworth 1", "email": f"p{ids[1]}@example.invalid", "birth_year": 1951,
             "zip3": "021", "plan_code": "GOLD", "balance": "999.99", "updated_at": "2026-01-01T00:00:00Z"}
    out = io.BytesIO()
    pq.write_table(pa.Table.from_pylist([stale]), out)
    raw.RawZone(s).drop(clinic, f"stale-{ids[1]}.parquet", out.getvalue())
    ingest(source=clinic.source)
    assert silver_rows(s, clinic, ids) == before


def test_identifiers_are_ciphertext_in_bronze_and_silver(s, clinic, ids, canaries):
    con = duck.connect(s, tables.CATALOG, principal=clinic.principal)
    for layer in (tables.BRONZE, tables.SILVER):
        rows = con.execute(f"SELECT mrn, full_name, email FROM {tables.qualified(clinic, layer)} "
                           f"WHERE patient_id = {ids[0]}").fetchall()
        assert rows and all(isinstance(v, (bytes, bytearray)) for r in rows for v in r)
        assert canaries.name.encode() not in b"".join(bytes(v) for r in rows for v in r)


def test_replay_from_raw_rebuilds_the_same_state(s, clinic, ids):
    before = silver_rows(s, clinic, ids)
    pipeline.replay(s, clinic)
    assert silver_rows(s, clinic, ids) == before
