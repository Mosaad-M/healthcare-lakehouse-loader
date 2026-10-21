"""dim_patient (SCD type 2 from the clinic's change feed, incremental). Its compiled dbt SQL runs in
plain DuckDB over a hand-made Bronze table (no stack), the way the iceberg_upsert materialization
runs it. For any sequence of change batches (updates, deletes, re-inserts, redelivered and
out-of-order changes), loaded over any sequence of Gold runs, versions must match a few lines of
Python, and a key once written must never change.
"""
import functools
import tempfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from tests.unit.test_gold_star import compiled

EPOCH = datetime(1900, 1, 1, tzinfo=timezone.utc)
T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
SETTINGS = settings(max_examples=200, deadline=None, suppress_health_check=[HealthCheck.too_slow])
# One change: (patient, op, (birth_year, zip3, plan_code), kind). kind "new" is the next change in
# the log; ("redeliver", i) repeats one of the patient's earlier changes exactly, sequence number
# included (at-least-once delivery); "older" carries an older sequence number than any yet (a change
# that arrives after newer ones).
CHANGE = st.tuples(st.sampled_from([1, 2]), st.sampled_from(["I", "U", "U", "D"]),
                   st.tuples(st.sampled_from([1970, 1980]), st.sampled_from(["021", "100"]),
                             st.sampled_from(["GOLD", "SILVER"])),
                   st.one_of(st.just("new"), st.just("older"), st.tuples(st.just("redeliver"), st.integers(0, 3))))
BATCH = st.lists(CHANGE, min_size=1, max_size=4)


@functools.cache
def _compiled(deploying: bool) -> dict[str, str]:
    with tempfile.TemporaryDirectory() as tmp:
        return compiled(Path(tmp), deploying=deploying)


# Fixtures that return the cached SQL; the property test calls _compiled itself, so a failure
# report shows the generated changes, not the SQL of every model.
@pytest.fixture(scope="module")
def sql() -> dict[str, str]:
    return _compiled(False)


@pytest.fixture(scope="module")
def deploy_sql() -> dict[str, str]:
    return _compiled(True)


def lake(deploy_sql) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute("ATTACH ':memory:' AS phi")
    for schema in ("bronze", "gold"):
        con.execute(f"CREATE SCHEMA phi.{schema}")
    con.execute("""CREATE TABLE phi.bronze.acme_clinic_patients__patients (patient_id BIGINT, mrn BLOB,
        full_name BLOB, email BLOB, birth_year INTEGER, zip3 VARCHAR, plan_code VARCHAR, balance DECIMAL(10,2),
        updated_at TIMESTAMPTZ, cdc_op VARCHAR, cdc_seq DECIMAL(38,0), subject_id VARCHAR, _batch_id VARCHAR,
        _source_file VARCHAR, _row_number BIGINT, _ingested_at TIMESTAMPTZ)""")
    con.execute(f"CREATE TABLE phi.gold.dim_patient AS SELECT * FROM ({deploy_sql['dim_patient']}) WHERE false")
    return con


def land(con, batch_id: str, landed: datetime, changes: list[tuple]) -> None:
    """changes: (patient_id, op, (birth_year, zip3, plan_code), cdc_seq)"""
    for i, (pid, op, (year, zip3, plan), seq) in enumerate(changes):
        con.execute("""INSERT INTO phi.bronze.acme_clinic_patients__patients VALUES
            (?, NULL, NULL, NULL, ?, ?, ?, 1.00, ?, ?, ?, ?, ?, 'f', ?, ?)""",
                    [pid, year, zip3, plan, T0, op, Decimal(seq), f"subject-{pid}", batch_id, i, landed])


def run(con, sql) -> int:
    con.execute(f"CREATE OR REPLACE TEMP TABLE staged AS {sql['dim_patient']}")
    con.execute("DELETE FROM phi.gold.dim_patient WHERE patient_sk IN (SELECT patient_sk FROM staged)")
    con.execute("INSERT INTO phi.gold.dim_patient BY NAME SELECT * FROM staged")
    return con.execute("SELECT count(*) FROM staged").fetchone()[0]


def versions(con) -> list[tuple]:
    return con.execute("""SELECT patient_id, birth_year, zip3, plan_code, valid_from, valid_to, is_current, version,
        ended_by_delete, cast(last_cdc_seq AS BIGINT) FROM phi.gold.dim_patient ORDER BY patient_id, version""").fetchall()


def expected(batches: list[list[tuple]], landed: list[datetime]) -> list[tuple]:
    """Reference model, Silver's rule: in landing order, each patient's newest change per batch; ignored
    if the patient is present and the change is not newer than its current row. A delete ends the open
    version (and leaves nothing to compare with); a present change opens a version or, with other
    attributes, ends the open one and opens the next."""
    out, open_, count = [], {}, {}
    for changes, at in zip(batches, landed):
        newest = {}
        for pid, op, attrs, seq in changes:
            if pid not in newest or seq > newest[pid][2]:
                newest[pid] = (op, attrs, seq)
        for pid, (op, attrs, seq) in sorted(newest.items()):
            cur = open_.get(pid)
            if cur and seq <= cur["last"]:
                continue
            if op == "D":
                if cur:
                    out.append((pid, *cur["attrs"], cur["from"], at, False, cur["n"], True, cur["last"]))
                    del open_[pid]
            elif cur and cur["attrs"] == attrs:
                cur["last"] = seq
            else:
                if cur:
                    out.append((pid, *cur["attrs"], cur["from"], at, False, cur["n"], False, cur["last"]))
                count[pid] = count.get(pid, 0) + 1
                open_[pid] = {"attrs": attrs, "from": EPOCH if count[pid] == 1 else at, "n": count[pid], "last": seq}
    out += [(pid, *c["attrs"], c["from"], None, True, c["n"], None, c["last"]) for pid, c in open_.items()]
    return sorted(out, key=lambda r: (r[0], r[7]))


@given(st.lists(st.tuples(BATCH, st.booleans()), min_size=1, max_size=6))
@SETTINGS
def test_versions_match_the_reference_model_for_any_changes_over_any_runs(steps):
    sql, deploy_sql = _compiled(False), _compiled(True)
    con = lake(deploy_sql)
    batches, landed, meaning, seq, sent = [], [], {}, 100, {}
    for i, (changes, run_now) in enumerate(steps):
        numbered = []
        for pid, op, attrs, kind in changes:
            earlier = sent.get(pid, [])
            if isinstance(kind, tuple) and earlier:
                change = earlier[-1 - kind[1] % len(earlier)]  # an exact redelivery
            else:
                seq += 1
                change = (pid, op, attrs, seq - 50 if kind == "older" else seq)
                sent.setdefault(pid, []).append(change)
            numbered.append(change)
        batches.append(numbered)
        landed.append(T0 + timedelta(days=i + 1))
        land(con, f"b{i}", landed[-1], numbered)
        if run_now or i == len(steps) - 1:
            run(con, sql)
            assert versions(con) == expected(batches, landed)
            rows = con.execute("SELECT patient_sk, patient_id, version, valid_from FROM phi.gold.dim_patient").fetchall()
            assert len({r[0] for r in rows}) == len(rows), "two versions share a key"
            assert set(meaning) <= {r[0] for r in rows}, "a key written by an earlier run is gone"
            for key, *version in rows:
                assert meaning.setdefault(key, version) == version, "a key now names another version"


def test_an_update_a_delete_and_a_re_insert(sql, deploy_sql):
    con = lake(deploy_sql)
    land(con, "b1", T0, [(1, "I", (1970, "021", "GOLD"), 1)])
    land(con, "b2", T0 + timedelta(days=1), [(1, "U", (1970, "021", "SILVER"), 2)])
    run(con, sql)
    land(con, "b3", T0 + timedelta(days=2), [(1, "D", (1970, "021", "SILVER"), 3)])
    run(con, sql)
    land(con, "b4", T0 + timedelta(days=3), [(1, "I", (1970, "100", "SILVER"), 4)])
    run(con, sql)
    got = [(r[3], r[7], r[8], r[6]) for r in versions(con)]
    assert got == [("GOLD", 1, False, False), ("SILVER", 2, True, False), ("SILVER", 3, None, True)]


def test_a_change_to_an_untracked_column_moves_the_sequence_but_starts_no_version(sql, deploy_sql):
    con = lake(deploy_sql)
    land(con, "b1", T0, [(1, "I", (1970, "021", "GOLD"), 1)])
    run(con, sql)
    land(con, "b2", T0 + timedelta(days=1), [(1, "U", (1970, "021", "GOLD"), 2)])  # a balance change, say
    assert run(con, sql) == 1  # the open version, rewritten with the newer sequence
    assert [(r[7], r[9]) for r in versions(con)] == [(1, 2)]
    assert run(con, sql) == 0  # read again, changes nothing again


def test_the_dimension_holds_no_identifier_columns(deploy_sql):
    con = lake(deploy_sql)
    columns = {r[0] for r in con.execute("DESCRIBE phi.gold.dim_patient").fetchall()}
    assert not columns & {"mrn", "full_name", "email", "balance"}
    assert {"patient_id", "subject_id", "birth_year", "zip3", "plan_code"} <= columns


def test_run_and_deploy_sql_return_the_same_columns(sql, deploy_sql):
    con = lake(deploy_sql)
    columns = lambda q: sorted(r[:2] for r in con.execute(f"DESCRIBE ({q})").fetchall())  # noqa: E731
    assert columns(sql["dim_patient"]) == columns(deploy_sql["dim_patient"])


@pytest.mark.parametrize("runs", ["one run", "a run per batch"])
def test_an_insert_redelivered_after_a_delete_starts_a_version_of_its_own(sql, deploy_sql, runs):
    """Insert (seq 1), delete (seq 2), then the insert again, exactly (seq 1): Silver takes it back (a
    delete leaves nothing to compare with), so the patient has a second version. It must not take
    the first version's key: keys come from the batch that starts a version, not the sequence."""
    con = lake(deploy_sql)
    insert = (1, "I", (1970, "021", "GOLD"), 1)
    for i, changes in enumerate([[insert], [(1, "D", (1970, "021", "GOLD"), 2)], [insert]]):
        land(con, f"b{i}", T0 + timedelta(days=i + 1), changes)
        if runs == "a run per batch":
            run(con, sql)
    if runs == "one run":
        run(con, sql)
    rows = con.execute("SELECT patient_sk, version, is_current, ended_by_delete FROM phi.gold.dim_patient "
                       "ORDER BY version").fetchall()
    assert [r[1:] for r in rows] == [(1, False, True), (2, True, None)]
    assert len({r[0] for r in rows}) == 2
