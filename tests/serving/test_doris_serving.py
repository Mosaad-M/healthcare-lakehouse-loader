"""Gold and Doris serving: correct and fast, no plaintext identifiers, erasure and access control.

Loads enough synthetic people into the example sources that Gold's small-cell suppression keeps
rows, rebuilds Gold with dbt, refreshes the Doris copies through the Gold flow, then checks answers,
freshness, latency, permissions and that Doris never holds a plaintext identifier.
"""
import os
import random
import statistics
import time
from datetime import date, timedelta

import pymysql
import pytest

from flows.gold import refresh
from flows.ingest import ingest
from loader import contracts, crypto, doris, duck, journal, raw, synthetic, tables
from loader.identity import Identity
from loader.serving import REFRESHES
from loader.shred import shred
from loader.vault import KeyVault
from tests.conftest import scan_all

pytestmark = [pytest.mark.doris]
SERVED = ("claims_monthly", "provider_activity", "lab_monthly", "member_claims")


def drop(s, contract, name: str, body: bytes) -> None:
    raw.RawZone(s).drop(contract, name, body)


@pytest.fixture(scope="module")
def loaded(s, run_id, canaries):
    found = contracts.load_all()
    seed = int(run_id[:6], 16)
    # Unique ids per run (the example tables are shared across runs); 60 members, 50 providers.
    drop(s, found["provider_directory"], f"providers_{run_id}.csv", synthetic.providers_csv(50, seed=seed))
    drop(s, found["acme_claims"], f"claims_{run_id}.csv",
         synthetic.claims_csv(canaries, date(2026, 8, 1), days=10, per_day=100, seed=seed, members=60,
                              id_offset=seed % 10_000 * 100))
    drop(s, found["lab_results"], f"labs_{run_id}.ndjson",
         synthetic.lab_ndjson(canaries, 300, seed=seed, members=30, id_offset=seed % 1000 * 1000))
    for source in ("provider_directory", "acme_claims", "lab_results"):
        ingest(source=source)
    refresh()
    return found


def gold_rows(s, model: str) -> list[tuple]:
    catalog = "phi" if model == "member_claims" else "deid"
    con = duck.connect(s, catalog, principal="gold", alias=catalog)
    cur = con.execute(f"SELECT * FROM {catalog}.gold.{model}")
    names = [d[0] for d in cur.description]
    return [tuple(v.hex().upper() if isinstance(v, (bytes, bytearray)) else v for v in row)
            for row in cur.fetchall()], names


def doris_rows(model: str) -> list[tuple]:
    return doris.query(doris.connect(), f"SELECT * FROM serving.{model}")


def normalize(rows):
    """Compare across engines: decimals and floats as rounded strings, dates as ISO strings."""
    def cell(v):
        if isinstance(v, float):
            return f"{v:.6f}"
        return str(v)
    return sorted(tuple(cell(v) for v in row) for row in rows)


@pytest.mark.parametrize("model", SERVED)
def test_doris_copies_answer_like_gold(s, loaded, model):
    rows, _ = gold_rows(s, model)
    assert rows, f"{model} is empty: suppression removed everything or the load failed"
    assert normalize(doris_rows(model)) == normalize(rows)


def test_the_refresh_records_which_gold_snapshot_each_copy_serves(s, loaded):
    items = {i["copy"]["S"]: i for i in scan_all(s, REFRESHES)}
    for model in SERVED:
        assert items[f"serving.{model}"]["gold_snapshot"]["S"] not in ("", "None")


def test_a_gold_update_is_visible_in_doris_after_the_pipeline_refresh(s, loaded, run_id, canaries):
    def served_snapshot():
        item = s.dynamodb().get_item(TableName=REFRESHES, Key={"copy": {"S": "serving.claims_monthly"}})["Item"]
        return item["gold_snapshot"]["S"]

    before, snapshot = doris.query(doris.connect(), "SELECT sum(claims) FROM serving.claims_monthly")[0][0], served_snapshot()
    # A service date no run has used (claims use window semantics, and the example tables are shared
    # across runs, so a used date would be restated rather than added; picking one from the run ID
    # collided once the stack had seen a dozen runs).
    claims = loaded["acme_claims"]
    used = {r[0] for r in duck.connect(s, tables.CATALOG, principal=claims.principal).execute(
        f"SELECT DISTINCT service_date FROM {tables.qualified(claims, tables.SILVER)} "
        "WHERE service_date < DATE '2026-01-01'").fetchall()}
    day = next(d for d in (date(2025, 1, 1) + timedelta(days=n) for n in range(365)) if d not in used)
    drop(s, claims, f"late_{run_id}.csv",
         synthetic.claims_csv(canaries, day, days=1, per_day=60, seed=7, members=60))
    ingest(source="acme_claims")
    assert doris.query(doris.connect(), "SELECT sum(claims) FROM serving.claims_monthly")[0][0] == before
    refresh()
    assert served_snapshot() != snapshot
    after = doris.query(doris.connect(), "SELECT sum(claims) FROM serving.claims_monthly")[0][0]
    assert after > before
    assert normalize(doris_rows("claims_monthly")) == normalize(gold_rows(s, "claims_monthly")[0])


def test_point_lookups_meet_the_latency_target(loaded):
    """p95 under 100 ms for single-member lookups on the member mart, over 300 queries."""
    con = doris.connect()
    subjects = [r[0] for r in doris.query(con, "SELECT subject_id FROM serving.member_claims WHERE subject_id IS NOT NULL")]
    rnd, timings = random.Random(1), []
    for _ in range(20):  # warm up
        doris.query(con, "SELECT * FROM serving.member_claims WHERE subject_id = %s", (rnd.choice(subjects),))
    for _ in range(300):
        start = time.perf_counter()
        assert doris.query(con, "SELECT * FROM serving.member_claims WHERE subject_id = %s", (rnd.choice(subjects),))
        timings.append((time.perf_counter() - start) * 1000)
    p95 = statistics.quantiles(timings, n=20)[-1]
    print(f"point lookup p50 {statistics.median(timings):.1f} ms, p95 {p95:.1f} ms")
    assert p95 < 100


def user(name: str):
    password = {"analyst": "DORIS_ANALYST_PASSWORD", "analyst_ma": "DORIS_ANALYST_MA_PASSWORD",
                "care_team": "DORIS_CARE_TEAM_PASSWORD"}[name]
    return doris.connect(name, os.environ[password])


def refused(con, sql: str) -> bool:
    try:
        doris.query(con, sql)
        return False
    except pymysql.MySQLError as err:
        return "denied" in str(err).lower() or "permission" in str(err).lower() or "privilege" in str(err).lower()


def test_analysts_see_only_de_identified_copies(loaded):
    con = user("analyst")
    assert doris.query(con, "SELECT count(*) FROM serving.provider_activity")[0][0] > 0
    assert refused(con, "SELECT * FROM serving.member_claims")
    assert refused(con, "SELECT * FROM lake_phi.silver.acme_claims__claims LIMIT 1")
    assert refused(con, "SELECT * FROM lake_deid.gold.claims_monthly LIMIT 1")


def test_the_lab_copy_holds_no_single_patients_value(loaded):
    """min and max of a cell are each one patient's result: the analysts' copy must not have them."""
    columns = [r[0] for r in doris.query(user("analyst"), "DESCRIBE serving.lab_monthly")]
    assert "min_value" not in columns and "max_value" not in columns and "sum_value" in columns


def test_row_policy_limits_analyst_ma_to_massachusetts(loaded):
    everyone = doris.query(user("analyst"), "SELECT state, count(*) FROM serving.provider_activity GROUP BY state")
    ma_only = doris.query(user("analyst_ma"), "SELECT state, count(*) FROM serving.provider_activity GROUP BY state")
    assert len(everyone) > 1
    assert ma_only == [r for r in everyone if r[0] == "MA"]


def test_care_team_cannot_select_the_identifier_column(loaded):
    con = user("care_team")
    assert doris.query(con, "SELECT subject_id, claims FROM serving.member_claims LIMIT 5")
    assert refused(con, "SELECT member_name FROM serving.member_claims LIMIT 1")
    assert refused(con, "SELECT * FROM serving.member_claims LIMIT 1")
    assert refused(con, "SELECT * FROM serving.claims_monthly LIMIT 1")


def test_doris_holds_no_plaintext_identifiers(s, loaded, canaries):
    con = doris.connect()
    dump = []
    for model in SERVED:
        dump += [str(r) for r in doris.query(con, f"SELECT * FROM serving.{model}")]
    text = "\n".join(dump)
    assert not [c for c in canaries.all() if c in text]
    # Positive control: the canary member is in the mart, as ciphertext.
    sid = canary_subject(s, loaded, canaries)
    row = doris.query(con, "SELECT member_name FROM serving.member_claims WHERE subject_id = %s", (sid,))
    assert row and bytes.fromhex(row[0][0])[:1] == crypto.VERSION


def canary_subject(s, loaded, canaries) -> str:
    contract = loaded["acme_claims"]
    return Identity(s).resolve(contract.source, [(canaries.member_id,)])[(canaries.member_id,)]


def test_shredded_person_is_unreadable_in_the_doris_copy(s, loaded, canaries, run_id):
    """After a shred, Doris holds the person's ciphertext and nothing can read it; a later refresh
    keeps it that way."""
    sid = canary_subject(s, loaded, canaries)
    con = doris.connect()
    blob = bytes.fromhex(doris.query(con, "SELECT member_name FROM serving.member_claims WHERE subject_id = %s",
                                     (sid,))[0][0])
    key = KeyVault(s).get([sid])[sid]
    assert crypto.decrypt(key, sid, "member_name", blob) == canaries.name  # readable before

    shred(s, request_id=f"doris-{run_id}", actor="privacy-officer@example.org", reason="patient_request",
          subject_ids=[sid])
    refresh()
    rows = doris.query(doris.connect(), "SELECT member_name FROM serving.member_claims WHERE subject_id = %s", (sid,))
    assert rows, "the copy no longer holds the subject's row; the test proves nothing"
    assert KeyVault(s).get([sid]) == {}
    assert canaries.name not in str(rows)
    assert any(e["request_id"] == f"doris-{run_id}" for e in journal.entries(s))
    # The key is gone, so even the ciphertext Doris held before the shred stays unreadable.
    assert sid not in KeyVault(s).get([sid])


def test_the_refresher_reads_gold_only(loaded):
    """Flow runs refresh Doris as serving_refresher, never root: Gold and the serving copies,
    nothing else in the lake."""
    con = doris.connect("serving_refresher", os.environ["DORIS_REFRESHER_PASSWORD"])
    assert doris.query(con, "SELECT count(*) FROM lake_deid.gold.claims_monthly")[0][0] > 0
    with pytest.raises(pymysql.err.OperationalError, match="Access denied|denied"):
        doris.query(con, "SELECT count(*) FROM lake_phi.silver.acme_claims__claims")
    with pytest.raises(pymysql.err.OperationalError, match="Access denied|denied"):
        doris.query(con, "CREATE DATABASE refresher_probe")



def test_spreadsheet_formulas_are_neutralized_in_the_copies(loaded):
    """A partner value that a spreadsheet would run (=, +, -, @, tab, CR) reaches people as text."""
    from loader.serving import safe_text

    values = ['=HYPERLINK("http://x.example","y")', "+1+2", "-3+4", "@SUM(A1)", "\tTAB", "plain", "a=b"]
    union = " UNION ALL ".join("SELECT %s AS v" for _ in values)
    got = sorted(r[0] for r in doris.query(doris.connect(), f"SELECT {safe_text('v')} FROM ({union}) t", values))
    assert got == sorted(["'" + v for v in values[:5]] + ["plain", "a=b"])


def test_the_backend_does_not_log_vended_tokens():
    """At INFO the BE logs each vended S3 session token in full (s3_util.cpp); it runs at WARNING."""
    from loader import doris

    con = doris.connect()
    with con.cursor() as cur:
        cur.execute("ADMIN SHOW BACKEND CONFIG LIKE 'sys_log_level'")
        value = [d[0].lower() for d in cur.description].index("value")
        values = {row[value] for row in cur.fetchall()}
    assert values == {"WARNING"}, values


def test_every_care_team_query_is_recorded_and_only_root_reads_the_record(loaded, run_id):
    """Doris records each query (user, time, client, statement, tables, rows) in its audit table; the
    serving users cannot read it. It flushes in batches (up to a minute)."""
    tag = f"audit_{run_id}_{random.randrange(10**9)}"
    doris.query(user("care_team"), f"SELECT /* {tag} */ subject_id FROM serving.member_claims LIMIT 3")
    deadline, rows = time.time() + 120, []
    while not rows and time.time() < deadline:
        time.sleep(5)
        rows = doris.query(doris.connect(), "SELECT user, state, return_rows, queried_tables_and_views "
                           "FROM __internal_schema.audit_log WHERE stmt LIKE %s", (f"%{tag}%",))
    assert rows, "the query was not recorded"
    who, state, returned, tables_read = rows[0]
    assert (who, state, returned) == ("care_team", "EOF", 3)
    assert "serving.member_claims" in str(tables_read)
    for name in ("care_team", "analyst"):
        assert refused(user(name), "SELECT count(*) FROM __internal_schema.audit_log"), name
