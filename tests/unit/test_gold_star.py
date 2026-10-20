"""The Gold star schema's core: dim_provider (SCD type 2) and fact_claim. Their compiled dbt SQL runs
in plain DuckDB over hand-made Bronze and Silver tables (no stack): versions must match what a few
lines of Python say they should for any sequence of directory snapshots, every claim must find the
provider version valid on its service date, and the same batches must always give the same keys.
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


@pytest.fixture(scope="module")
def sql(tmp_path_factory) -> dict[str, str]:
    """Compiled SQL of every Gold model (dbt compile against a throwaway in-memory profile)."""
    tmp = tmp_path_factory.mktemp("dbt")
    profile = {"lakehouse_gold": {"target": "t", "outputs": {"t": {"type": "duckdb", "path": ":memory:"}}}}
    (tmp / "profiles.yml").write_text(yaml.safe_dump(profile))
    return {m.name: m.sql for m in gold.models(None, tmp)}


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


def build(con, sql, *models) -> None:
    for model in models:
        con.execute(f"CREATE OR REPLACE TABLE phi.gold.{model} AS {sql[model]}")


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


@given(st.lists(SNAPSHOT, min_size=1, max_size=6))
@SETTINGS
def test_versions_match_the_reference_model_for_any_sequence_of_snapshots(sql, snapshots):
    con = lake()
    landed = [T0 + timedelta(days=30 * i) for i in range(len(snapshots))]
    for i, (snap, at) in enumerate(zip(snapshots, landed)):
        land(con, f"b{i}", at, snap)
    build(con, sql, "dim_provider")
    assert versions(con) == expected_versions(snapshots, landed)
    # Versions of one provider never overlap, and the key is unique.
    keys = con.execute("SELECT provider_sk FROM phi.gold.dim_provider").fetchall()
    assert len(keys) == len(set(keys))


def test_a_rebuild_from_the_same_batches_gives_the_same_keys(sql):
    first, second = lake(), lake()
    for con in (first, second):
        land(con, "b1", T0, {"1000000001": ("Clinic A", "cardiology", "MA", True)})
        land(con, "b2", T0 + timedelta(days=5), {"1000000001": ("Clinic A", "oncology", "MA", True)})
        build(con, sql, "dim_provider")
    q = "SELECT provider_sk, npi, valid_from FROM phi.gold.dim_provider ORDER BY valid_from"
    assert first.execute(q).fetchall() == second.execute(q).fetchall()


def test_each_claim_gets_the_provider_version_valid_on_its_service_date(sql):
    con = lake()
    t2, t3, t4 = datetime(2026, 3, 1, tzinfo=timezone.utc), datetime(2026, 5, 1, tzinfo=timezone.utc), \
        datetime(2026, 7, 1, tzinfo=timezone.utc)
    a, b = "1000000001", "1000000002"
    land(con, "b1", T0, {a: ("Clinic A", "cardiology", "MA", True), b: ("Clinic B", "oncology", "NY", True)})
    land(con, "b2", t2, {a: ("Clinic A", "pediatrics", "MA", True), b: ("Clinic B", "oncology", "NY", True)})
    land(con, "b3", t3, {a: ("Clinic A", "pediatrics", "MA", True)})  # b drops out
    land(con, "b4", t4, {a: ("Clinic A", "pediatrics", "MA", True), b: ("Clinic B", "oncology", "NY", True)})
    claims = [("c1", a, date(2026, 2, 15)), ("c2", a, date(2026, 3, 1)), ("c3", b, date(2026, 6, 1)),
              ("c4", b, date(2026, 8, 1)), ("c5", "9999999999", date(2026, 2, 1)), ("c6", b, date(2025, 1, 1))]
    for claim_id, npi, day in claims:
        con.execute("INSERT INTO phi.silver.acme_claims__claims VALUES (?, NULL, NULL, ?, 'I10', '99213', ?, 10, 8, ?, 0, 'cb', ?)",
                    [claim_id, day, npi, f"s-{claim_id}", T0])
    build(con, sql, "dim_provider", "fact_claim")
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


def test_every_column_of_the_core_has_an_iceberg_type(sql):
    """loader/gold.py deploys each table from DESCRIBE of its SQL; an unmapped type fails the deploy."""
    con = lake()
    build(con, sql, "dim_provider")
    for model in ("dim_provider", "fact_claim"):
        for name, typ, *_ in con.execute(f"DESCRIBE ({sql[model]})").fetchall():
            assert gold.iceberg_type(typ), (model, name)


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
