"""The Gold star schema's core: dim_provider (SCD type 2, incremental) and fact_claim. Their compiled
dbt SQL runs in plain DuckDB over hand-made Bronze and Silver tables (no stack), dim_provider the way
the iceberg_upsert materialization runs it. For any sequence of directory snapshots, loaded over any
sequence of Gold runs, versions must match what a few lines of Python say they should, a key once
written must never change, and every claim must find the provider version valid on its service date.
"""
from datetime import date, datetime, timedelta, timezone

import duckdb
import pytest
import yaml
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from loader import contracts, gold

EPOCH = datetime(1900, 1, 1, tzinfo=timezone.utc)
T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
NPIS = st.sampled_from(["1000000001", "1000000002", "1000000003"])
ATTRS = st.tuples(st.sampled_from(["Clinic A", "Clinic B"]), st.sampled_from(["cardiology", "oncology"]),
                  st.sampled_from(["MA", "NY"]), st.booleans())
SNAPSHOT = st.dictionaries(NPIS, ATTRS, min_size=1, max_size=3)  # one directory file: npi -> attributes
SETTINGS = settings(max_examples=200, deadline=None, suppress_health_check=[HealthCheck.too_slow])
A = "1000000001"


def compiled(tmp, deploying: bool) -> dict[str, str]:
    profile = {"lakehouse_gold": {"target": "t", "outputs": {"t": {"type": "duckdb", "path": ":memory:"}}}}
    (tmp / "profiles.yml").write_text(yaml.safe_dump(profile))
    return {m.name: m.sql for m in gold.models(None, tmp, deploying=deploying)}


@pytest.fixture(scope="module")
def sql(tmp_path_factory) -> dict[str, str]:
    """Compiled SQL of every Gold model, as a Gold run compiles it."""
    return compiled(tmp_path_factory.mktemp("dbt"), deploying=False)


@pytest.fixture(scope="module")
def deploy_sql(tmp_path_factory) -> dict[str, str]:
    """As Gold deploy compiles it: what the tables are created from."""
    return compiled(tmp_path_factory.mktemp("dbt_deploy"), deploying=True)


def lake() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute("ATTACH ':memory:' AS phi")
    for schema in ("bronze", "silver", "gold"):
        con.execute(f"CREATE SCHEMA phi.{schema}")
    con.execute("""CREATE TABLE phi.bronze.provider_directory__providers (npi VARCHAR, provider_name VARCHAR,
        specialty VARCHAR, state VARCHAR, active BOOLEAN, _batch_id VARCHAR, _source_file VARCHAR,
        _row_number BIGINT, _ingested_at TIMESTAMPTZ)""")
    con.execute("""CREATE TABLE phi.silver.acme_claims__claims (claim_id VARCHAR, member_id BLOB, member_name BLOB,
        service_date DATE, diagnosis_code VARCHAR, procedure_code VARCHAR, provider_npi VARCHAR,
        billed_amount DECIMAL(12,2), paid_amount DECIMAL(12,2), subject_id VARCHAR, pk_bucket INTEGER,
        _batch_id VARCHAR, _ingested_at TIMESTAMPTZ)""")
    return con


def land(con, batch_id: str, landed: datetime, snapshot: dict) -> None:
    for i, (npi, (name, specialty, state, active)) in enumerate(sorted(snapshot.items())):
        con.execute("INSERT INTO phi.bronze.provider_directory__providers VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [npi, name, specialty, state, active, batch_id, "f", i, landed])


def create_dim(con, deploy_sql) -> None:
    """What Gold deploy does: an empty table with the model's columns."""
    con.execute(f"CREATE TABLE phi.gold.dim_provider AS SELECT * FROM ({deploy_sql['dim_provider']}) WHERE false")


def run_dim(con, sql) -> int:
    """What the iceberg_upsert materialization does. Returns the number of rows the run wrote."""
    con.execute(f"CREATE OR REPLACE TEMP TABLE staged AS {sql['dim_provider']}")
    con.execute("DELETE FROM phi.gold.dim_provider WHERE provider_sk IN (SELECT provider_sk FROM staged)")
    con.execute("INSERT INTO phi.gold.dim_provider BY NAME SELECT * FROM staged")
    return con.execute("SELECT count(*) FROM staged").fetchone()[0]


def versions(con) -> list[tuple]:
    return con.execute("""SELECT npi, provider_name, specialty, state, active, valid_from, valid_to, is_current, version
        FROM phi.gold.dim_provider ORDER BY npi, valid_from""").fetchall()


def expected_versions(snapshots: list[dict], landed: list[datetime]) -> list[tuple]:
    """Reference model: walk the snapshots in landing order and cut a version whenever a provider
    appears, changes, or drops out."""
    out, open_ = [], {}  # npi -> [attrs, valid_from, version]
    count: dict[str, int] = {}
    for snap, at in zip(snapshots, landed):
        for npi in list(open_):
            if npi not in snap or snap[npi] != open_[npi][0]:
                attrs, start, n = open_.pop(npi)
                out.append((npi, *attrs, start, at, False, n))
        for npi, attrs in snap.items():
            if npi not in open_:
                count[npi] = count.get(npi, 0) + 1
                open_[npi] = [attrs, EPOCH if count[npi] == 1 else at, count[npi]]
    out += [(npi, *attrs, start, None, True, n) for npi, (attrs, start, n) in open_.items()]
    return sorted(out, key=lambda r: (r[0], r[5]))


@given(st.lists(st.tuples(SNAPSHOT, st.booleans()), min_size=1, max_size=6))
@SETTINGS
def test_versions_match_the_reference_model_for_any_snapshots_over_any_runs(sql, deploy_sql, steps):
    """Each step lands one snapshot, then Gold runs if the step says so (and always at the end).
    After every run the table must equal a build over every batch so far, and every key an earlier
    run wrote must still be there."""
    con = lake()
    create_dim(con, deploy_sql)
    snapshots, landed, keys = [], [], set()
    for i, (snap, run_now) in enumerate(steps):
        snapshots.append(snap)
        landed.append(T0 + timedelta(days=30 * i))
        land(con, f"b{i}", landed[-1], snap)
        if run_now or i == len(steps) - 1:
            run_dim(con, sql)
            assert versions(con) == expected_versions(snapshots, landed)
            now = {r[0] for r in con.execute("SELECT provider_sk FROM phi.gold.dim_provider").fetchall()}
            assert keys <= now, "a key written by an earlier run changed"
            keys = now
    count = con.execute("SELECT count(*), count(DISTINCT provider_sk) FROM phi.gold.dim_provider").fetchone()
    assert count[0] == count[1]


def test_a_run_with_nothing_new_writes_nothing(sql, deploy_sql):
    con = lake()
    create_dim(con, deploy_sql)
    land(con, "b1", T0, {A: ("Clinic A", "cardiology", "MA", True)})
    assert run_dim(con, sql) == 1
    assert run_dim(con, sql) == 0
    # A later batch identical to the current state changes nothing, however often it is read.
    land(con, "b2", T0 + timedelta(days=1), {A: ("Clinic A", "cardiology", "MA", True)})
    assert run_dim(con, sql) == 0
    assert run_dim(con, sql) == 0


def test_applied_batches_are_not_needed_again(sql, deploy_sql):
    """What keeping history in Gold is for: once a run has applied batches, Bronze can lose them (a
    retention policy) and later runs still extend the same history."""
    con = lake()
    create_dim(con, deploy_sql)
    land(con, "b1", T0, {A: ("Clinic A", "cardiology", "MA", True)})
    land(con, "b2", T0 + timedelta(days=1), {A: ("Clinic A", "oncology", "MA", True)})
    run_dim(con, sql)
    first = versions(con)[0]
    con.execute("DELETE FROM phi.bronze.provider_directory__providers")  # retention removed them
    land(con, "b3", T0 + timedelta(days=2), {A: ("Clinic A", "pediatrics", "MA", True)})
    run_dim(con, sql)
    after = versions(con)
    assert after[0] == first
    assert [(r[2], r[8], r[7]) for r in after] == [("cardiology", 1, False), ("oncology", 2, False),
                                                   ("pediatrics", 3, True)]


def test_batches_that_landed_at_the_same_instant_are_both_applied(sql, deploy_sql):
    con = lake()
    create_dim(con, deploy_sql)
    land(con, "b1", T0, {A: ("Clinic A", "cardiology", "MA", True)})
    run_dim(con, sql)
    land(con, "b2", T0, {A: ("Clinic A", "oncology", "MA", True)})  # same landed_at, later batch id
    run_dim(con, sql)
    assert [r[2] for r in versions(con)] == ["cardiology", "oncology"]


def test_a_table_written_before_the_watermark_columns_is_rebuilt_with_the_same_keys(sql, deploy_sql):
    """Rows from the first, full-rebuild version of the model have no first_seen_at: the next run
    rebuilds them from Bronze, and every key stays what it was."""
    con = lake()
    create_dim(con, deploy_sql)
    land(con, "b1", T0, {A: ("Clinic A", "cardiology", "MA", True)})
    land(con, "b2", T0 + timedelta(days=1), {A: ("Clinic A", "oncology", "MA", True)})
    run_dim(con, sql)
    q = "SELECT provider_sk, valid_from, valid_to FROM phi.gold.dim_provider ORDER BY valid_from"
    expected = con.execute(q).fetchall()
    con.execute("UPDATE phi.gold.dim_provider SET first_seen_at = NULL, ended_by_batch_id = NULL")
    assert run_dim(con, sql) == 2
    assert con.execute(q).fetchall() == expected
    assert con.execute("SELECT count(*) FROM phi.gold.dim_provider WHERE first_seen_at IS NULL").fetchone()[0] == 0


def test_each_claim_gets_the_provider_version_valid_on_its_service_date(sql, deploy_sql):
    con = lake()
    t2, t3, t4 = datetime(2026, 3, 1, tzinfo=timezone.utc), datetime(2026, 5, 1, tzinfo=timezone.utc), \
        datetime(2026, 7, 1, tzinfo=timezone.utc)
    a, b = A, "1000000002"
    land(con, "b1", T0, {a: ("Clinic A", "cardiology", "MA", True), b: ("Clinic B", "oncology", "NY", True)})
    land(con, "b2", t2, {a: ("Clinic A", "pediatrics", "MA", True), b: ("Clinic B", "oncology", "NY", True)})
    land(con, "b3", t3, {a: ("Clinic A", "pediatrics", "MA", True)})  # b drops out
    land(con, "b4", t4, {a: ("Clinic A", "pediatrics", "MA", True), b: ("Clinic B", "oncology", "NY", True)})
    claims = [("c1", a, date(2026, 2, 15)), ("c2", a, date(2026, 3, 1)), ("c3", b, date(2026, 6, 1)),
              ("c4", b, date(2026, 8, 1)), ("c5", "9999999999", date(2026, 2, 1)), ("c6", b, date(2025, 1, 1))]
    for claim_id, npi, day in claims:
        con.execute("INSERT INTO phi.silver.acme_claims__claims VALUES (?, NULL, NULL, ?, 'I10', '99213', ?, 10, 8, ?, 0, 'cb', ?)",
                    [claim_id, day, npi, f"s-{claim_id}", T0])
    create_dim(con, deploy_sql)
    run_dim(con, sql)
    con.execute(f"CREATE TABLE phi.gold.fact_claim AS {sql['fact_claim']}")
    got = dict(con.execute("""SELECT f.claim_id, d.specialty || '@' || d.version FROM phi.gold.fact_claim f
        LEFT JOIN phi.gold.dim_provider d ON d.provider_sk = f.provider_sk""").fetchall())
    assert got == {
        "c1": "cardiology@1",  # before the change
        "c2": "pediatrics@2",  # on the day the change landed (versions start at midnight UTC)
        "c3": None,            # b was out of the directory then
        "c4": "oncology@2",    # b's second version, after it returned
        "c5": None,            # never in the directory
        "c6": "oncology@1",    # before b was first seen: the first version is backdated
    }
    assert con.execute("SELECT count(*), count(DISTINCT claim_id) FROM phi.gold.fact_claim").fetchone() == (6, 6)
    assert {r[0] for r in con.execute("SELECT DISTINCT partner FROM phi.gold.fact_claim").fetchall()} == {"acme"}
    assert {r[0] for r in con.execute("SELECT DISTINCT partner FROM phi.gold.dim_provider").fetchall()} == {"npi_registry"}


def test_every_column_of_the_core_has_an_iceberg_type(sql, deploy_sql):
    """loader/gold.py creates each table from DESCRIBE of its deploy-compiled SQL; an unmapped type
    fails the deploy. The dimension's run SQL must return the columns it was deployed with."""
    con = lake()
    create_dim(con, deploy_sql)
    for model in ("dim_provider", "fact_claim"):
        for name, typ, *_ in con.execute(f"DESCRIBE ({deploy_sql[model]})").fetchall():
            assert gold.iceberg_type(typ), (model, name)
    columns = lambda q: sorted(r[:2] for r in con.execute(f"DESCRIBE ({q})").fetchall())  # noqa: E731
    assert columns(sql["dim_provider"]) == columns(deploy_sql["dim_provider"])


def test_every_gold_source_names_its_contracts_partner():
    """meta.partner in dbt/models/sources.yml is what Gold rows carry; it must match the contract."""
    by_table = {c.table: c for c in contracts.load_all().values()}
    for node in gold.manifest()["sources"].values():
        contract = by_table[node["name"]]
        assert node["config"]["meta"].get("partner") == contract.partner, node["unique_id"]


def test_models_come_after_the_models_they_read(sql):
    order = list(sql)
    assert order.index("dim_provider") < order.index("fact_claim") < order.index("provider_activity")
    assert order.index("fact_claim") < order.index("claims_monthly")
