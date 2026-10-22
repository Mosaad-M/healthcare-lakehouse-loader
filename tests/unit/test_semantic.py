"""The semantic layer (loader/semantic.py): metrics defined in dbt, SQL written by MetricFlow. Here
that SQL runs in plain DuckDB over Gold tables built by the real dbt models from hand-made Silver and
Bronze (no stack), and every metric must equal a hand-written query, at every grouping.
"""
import functools
import random
from datetime import date, datetime, timedelta, timezone

import pytest

from loader import offboarding, semantic
from tests.unit.test_gold_star import compiled, create_dim, lake, land, run_dim

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
METRICS = ["claims", "billed", "paid", "distinct_members", "paid_per_claim"]


@functools.cache
def _sql():
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as tmp:
        run = compiled(Path(tmp), deploying=False)
    with tempfile.TemporaryDirectory() as tmp:
        deploy = compiled(Path(tmp), deploying=True)
    return run, deploy


@pytest.fixture(scope="module")
def gold():
    """Gold built from a directory with a change (a provider moves state) and 400 claims over 40
    members, 3 diagnoses and two months, some before the change and some after."""
    run, deploy = _sql()
    con = lake()
    con.execute("ATTACH ':memory:' AS deid")
    con.execute("CREATE SCHEMA deid.gold")
    con.execute(f"CREATE TABLE deid.gold.metricflow_time_spine AS {run['metricflow_time_spine']}")
    npis = [f"10000000{i:02d}" for i in range(6)]
    land(con, "p1", T0, {n: (f"Clinic {n}", ["cardiology", "oncology"][i % 2], ["MA", "NY"][i % 2], True)
                         for i, n in enumerate(npis)})
    moved = {n: (f"Clinic {n}", ["cardiology", "oncology"][i % 2], ["MA", "NY"][i % 2], True) for i, n in enumerate(npis)}
    moved[npis[0]] = (f"Clinic {npis[0]}", "cardiology", "NY", True)  # moves from MA to NY
    land(con, "p2", datetime(2026, 2, 15, tzinfo=timezone.utc), moved)
    create_dim(con, deploy)
    run_dim(con, run)
    rnd = random.Random(7)
    for i in range(400):
        day = date(2026, 2, 1) + timedelta(days=rnd.randrange(56))
        con.execute("INSERT INTO phi.silver.acme_claims__claims VALUES (?, NULL, NULL, ?, ?, '99213', ?, ?, ?, ?, 0, 'c', ?)",
                    [f"C{i}", day, rnd.choice(["I10", "E11.9", "J45"]), rnd.choice(npis),
                     round(rnd.uniform(50, 500), 2), round(rnd.uniform(10, 400), 2), f"m{rnd.randrange(40)}", T0])
    con.execute(f"CREATE TABLE phi.gold.fact_claim AS {run['fact_claim']}")
    return con


def hand_written(con, group_by: list[str]) -> list[tuple]:
    exprs = {"metric_time__month": "date_trunc('month', f.service_date)",
             "provider_version__state": "d.state", "provider_version__specialty": "d.specialty",
             "claim__diagnosis_code": "f.diagnosis_code"}
    cols = [exprs[g] for g in group_by]
    select = ", ".join(cols + ["count(*)", "sum(f.billed_amount)", "sum(f.paid_amount)", "count(DISTINCT f.subject_id)",
                               "sum(f.paid_amount) / count(*)"])
    sql = (f"SELECT {select} FROM phi.gold.fact_claim f LEFT JOIN phi.gold.dim_provider d "
           f"ON d.provider_sk = f.provider_sk" + (f" GROUP BY {', '.join(cols)}" if cols else ""))
    return con.execute(sql).fetchall()


def rounded(rows):
    def cell(v):
        if isinstance(v, datetime):
            return v.date()
        if isinstance(v, (float, int)) or hasattr(v, "as_tuple"):
            return round(float(v), 4)
        return v
    return sorted(tuple(cell(v) for v in row) for row in rows)


@pytest.mark.parametrize("group_by", [[], ["metric_time__month"], ["provider_version__state"],
                                      ["provider_version__specialty", "metric_time__month"],
                                      ["claim__diagnosis_code", "provider_version__state"]])
def test_every_metric_equals_a_hand_written_query(gold, group_by):
    columns = ", ".join(group_by + METRICS)  # by name: MetricFlow orders its own columns
    got = gold.execute(f"SELECT {columns} FROM ({semantic.sql(METRICS, group_by)})").fetchall()
    assert rounded(got) == rounded(hand_written(gold, group_by))


def test_provider_attributes_are_as_of_the_service_date(gold):
    """The provider that moved from MA to NY on Feb 15: its earlier claims count under MA."""
    sql = semantic.sql(["claims"], ["provider_version__state", "metric_time__month"])
    by_state = {(r[0].date() if hasattr(r[0], "date") else r[0], r[1]): r[2]
                for r in gold.execute(f"SELECT metric_time__month, provider_version__state, claims FROM ({sql})").fetchall()}
    expected = gold.execute("""SELECT date_trunc('month', f.service_date)::DATE, d.state, count(*)
        FROM phi.gold.fact_claim f JOIN phi.gold.dim_provider d ON d.provider_sk = f.provider_sk
        GROUP BY ALL""").fetchall()
    assert by_state == {(m, s): n for m, s, n in expected}
    states_of_mover = {r[0] for r in gold.execute("""SELECT DISTINCT d.state FROM phi.gold.fact_claim f
        JOIN phi.gold.dim_provider d ON d.provider_sk = f.provider_sk WHERE f.provider_npi = '1000000000'""").fetchall()}
    assert states_of_mover == {"MA", "NY"}


def test_distinct_members_is_counted_not_added(gold):
    """By month, members can appear in both months: the total is not the sum of the months."""
    total = gold.execute(semantic.sql(["distinct_members"])).fetchone()[0]
    months = [r[-1] for r in gold.execute(semantic.sql(["distinct_members"], ["metric_time__month"])).fetchall()]
    assert total == gold.execute("SELECT count(DISTINCT subject_id) FROM phi.gold.fact_claim").fetchone()[0]
    assert sum(months) > total


def test_unknown_names_are_refused():
    with pytest.raises(Exception):
        semantic.sql(["paid"], ["member__name"])
    with pytest.raises(Exception):
        semantic.sql(["no_such_metric"])


def test_metric_queries_read_only_the_semantic_tables():
    assert semantic.tables() == {("phi", "gold", "fact_claim"), ("phi", "gold", "dim_provider"),
                                 ("deid", "gold", "metricflow_time_spine")}


def test_the_delete_report_lists_the_metrics_built_on_a_partners_models():
    found = {d["model"]: d["metrics"] for d in offboarding.gold_dependents(["acme_claims"])}
    assert found["phi.gold.fact_claim"] == sorted(METRICS)
