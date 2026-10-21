"""Labs in Gold: fact_lab_result (every lab source, one standard unit per test) and the lab_monthly
mart built on it. Compiled dbt SQL runs in plain DuckDB over hand-made Silver tables (no stack).
"""
from datetime import date, datetime, timezone

import duckdb
import pytest

from loader import gold
from tests.unit.test_gold_star import compiled

GLUCOSE, HBA1C = "2345-7", "4548-4"


@pytest.fixture(scope="module")
def sql(tmp_path_factory) -> dict[str, str]:
    return compiled(tmp_path_factory.mktemp("dbt"), deploying=False)


def lake() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute("ATTACH ':memory:' AS phi")
    con.execute("ATTACH ':memory:' AS deid")
    for schema in ("phi.silver", "phi.gold", "deid.gold"):
        con.execute(f"CREATE SCHEMA {schema}")
    con.execute("""CREATE TABLE phi.silver.lab_results__results (result_id VARCHAR, patient_mrn BLOB, patient_email BLOB,
        collected_at TIMESTAMPTZ, test_code VARCHAR, value DOUBLE, unit VARCHAR, subject_id VARCHAR, pk_bucket INTEGER,
        _batch_id VARCHAR, _ingested_at TIMESTAMPTZ)""")
    con.execute("""CREATE TABLE phi.silver.acme_lab_mail__results (result_id VARCHAR, patient_mrn BLOB,
        collected_on DATE, test_code VARCHAR, value DOUBLE, unit VARCHAR, subject_id VARCHAR, pk_bucket INTEGER,
        _batch_id VARCHAR, _ingested_at TIMESTAMPTZ)""")
    return con


def metro(con, result_id, subject, at: datetime, code, value, unit) -> None:
    con.execute("INSERT INTO phi.silver.lab_results__results VALUES (?, NULL, NULL, ?, ?, ?, ?, ?, 0, 'm', ?)",
                [result_id, at, code, value, unit, subject, at])


def acme(con, result_id, subject, day: date, code, value, unit) -> None:
    con.execute("INSERT INTO phi.silver.acme_lab_mail__results VALUES (?, NULL, ?, ?, ?, ?, ?, 0, 'a', ?)",
                [result_id, day, code, value, unit, subject, datetime(2026, 1, 1, tzinfo=timezone.utc)])


def build(con, sql) -> None:
    con.execute(f"CREATE TABLE phi.gold.fact_lab_result AS {sql['fact_lab_result']}")
    con.execute(f"CREATE TABLE deid.gold.lab_monthly AS {sql['lab_monthly']}")


def test_the_fact_holds_every_result_of_every_lab_once_in_standard_units(sql):
    con = lake()
    metro(con, "R1", "s-metro-1", datetime(2026, 9, 1, 8, 30, tzinfo=timezone.utc), GLUCOSE, 99.0, "mg/dL")
    acme(con, "R1", "s-acme-1", date(2026, 9, 2), GLUCOSE, 5.5, " MMOL/L ")  # same result_id, other lab
    acme(con, "R2", "s-acme-1", date(2026, 9, 2), HBA1C, 48.0, "mmol/mol")
    acme(con, "R3", "s-acme-1", date(2026, 9, 2), GLUCOSE, 7.0, "mmol/dL")    # a unit we cannot convert
    build(con, sql)
    rows = {(r[0], r[1]): r[2:] for r in con.execute("""SELECT source_name, result_id, partner, collected_at,
        collected_date, collected_precision, round(value_std, 3), unit_std FROM phi.gold.fact_lab_result""").fetchall()}
    assert rows[("lab_results", "R1")] == ("metro_lab", datetime(2026, 9, 1, 8, 30, tzinfo=timezone.utc),
                                           date(2026, 9, 1), "instant", 99.0, "mg/dL")
    assert rows[("acme_lab_mail", "R1")] == ("acme", datetime(2026, 9, 2, tzinfo=timezone.utc), date(2026, 9, 2),
                                             "day", round(5.5 * 18.016, 3), "mg/dL")
    assert rows[("acme_lab_mail", "R2")][4:] == (round(48.0 * 0.09148 + 2.152, 3), "%")
    assert rows[("acme_lab_mail", "R3")][4:] == (None, None)
    keys = con.execute("SELECT count(*), count(DISTINCT lab_result_key) FROM phi.gold.fact_lab_result").fetchone()
    assert keys == (4, 4)


def test_lab_monthly_holds_no_single_patients_value(sql):
    """min and max are each one patient's result: they must not be in the mart at all."""
    con = lake()
    build(con, sql)
    columns = [r[0] for r in con.execute("DESCRIBE deid.gold.lab_monthly").fetchall()]
    assert columns == ["test_code", "unit", "collected_month", "results", "patients", "mean_value", "sum_value"]


def test_months_can_be_combined_by_weighting_with_sum_value(sql):
    con = lake()
    for i in range(11):
        metro(con, f"A{i}", f"s{i}", datetime(2026, 9, 1, tzinfo=timezone.utc), GLUCOSE, 100.0, "mg/dL")
        for j in range(3):
            metro(con, f"B{i}-{j}", f"s{i}", datetime(2026, 10, 1, tzinfo=timezone.utc), GLUCOSE, 200.0, "mg/dL")
    build(con, sql)
    months = con.execute("SELECT results, mean_value, sum_value FROM deid.gold.lab_monthly ORDER BY collected_month").fetchall()
    assert months == [(11, 100.0, 1100.0), (33, 200.0, 6600.0)]
    weighted = sum(m[2] for m in months) / sum(m[0] for m in months)
    assert weighted == pytest.approx(175.0)  # not (100 + 200) / 2


def test_a_group_is_kept_only_when_one_lab_alone_has_enough_patients(sql):
    """The same six people tested at both labs are twelve subjects but six people: suppressed.
    Eleven at one lab: kept, whatever the other lab adds."""
    con = lake()
    sept, oct_ = datetime(2026, 9, 3, tzinfo=timezone.utc), date(2026, 10, 3)
    for i in range(6):
        metro(con, f"M{i}", f"metro-{i}", sept, GLUCOSE, 90.0, "mg/dL")
        acme(con, f"A{i}", f"acme-{i}", sept.date(), GLUCOSE, 5.0, "mmol/L")
    for i in range(11):
        acme(con, f"B{i}", f"acme-{i}", oct_, GLUCOSE, 5.0, "mmol/L")
    metro(con, "M99", "metro-0", datetime(2026, 10, 4, tzinfo=timezone.utc), GLUCOSE, 90.0, "mg/dL")
    build(con, sql)
    rows = con.execute("SELECT collected_month, results, patients FROM deid.gold.lab_monthly").fetchall()
    assert rows == [(date(2026, 10, 1), 12, 12)]


def test_results_in_units_we_cannot_convert_stay_out_of_the_mart(sql):
    con = lake()
    for i in range(11):
        metro(con, f"A{i}", f"s{i}", datetime(2026, 9, 1, tzinfo=timezone.utc), GLUCOSE, 100.0, "mg/dL")
        metro(con, f"X{i}", f"s{i}", datetime(2026, 9, 2, tzinfo=timezone.utc), GLUCOSE, 9.0, "furlongs")
    build(con, sql)
    assert con.execute("SELECT unit, results, mean_value FROM deid.gold.lab_monthly").fetchall() == [("mg/dL", 11, 100.0)]


def test_every_lab_column_has_an_iceberg_type(sql):
    con = lake()
    build(con, sql)
    for model in ("fact_lab_result", "lab_monthly"):
        for name, typ, *_ in con.execute(f"DESCRIBE ({sql[model]})").fetchall():
            assert gold.iceberg_type(typ), (model, name)
