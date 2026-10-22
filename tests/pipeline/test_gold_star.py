"""The Gold core on the stack: dim_provider (SCD type 2, from Bronze) and fact_claim, written to
Iceberg through Polaris by the Gold flow, and the marts built on them.

Gold reads the example sources, which every run shares, so checks hold for whatever earlier runs
loaded, and the version test changes one provider of its own run's directory file.
"""
import csv
import io
from datetime import date

import pytest

from flows.gold import refresh
from flows.ingest import ingest
from loader import contracts, duck, raw, synthetic


def gold(s):
    con = duck.connect(s, "phi", principal="gold", alias="phi")
    duck.attach_polaris(con, s, "deid", principal="gold", alias="deid")
    return con


def directory_with(body: bytes, npi: str, specialty: str) -> bytes:
    rows = list(csv.DictReader(io.StringIO(body.decode())))
    for row in rows:
        if row["npi"] == npi:
            row["specialty"] = specialty
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=list(rows[0]), lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return out.getvalue().encode()


@pytest.fixture(scope="module")
def loaded(s, run_id, canaries):
    found = contracts.load_all()
    seed = int(run_id[:6], 16) + 1
    directory = synthetic.providers_csv(50, seed=seed)
    zone = raw.RawZone(s)
    zone.drop(found["provider_directory"], f"providers_star_{run_id}.csv", directory)
    zone.drop(found["acme_claims"], f"claims_star_{run_id}.csv",
              synthetic.claims_csv(canaries, date(2026, 6, 1), days=5, per_day=80, seed=seed, members=40,
                                   id_offset=seed % 10_000 * 100 + 7))
    for source in ("provider_directory", "acme_claims"):
        ingest(source=source)
    refresh(serve=False)
    return found, directory


def test_fact_claim_holds_every_claim_in_silver_once(s, loaded):
    con = gold(s)
    silver = con.execute("SELECT count(*), sum(paid_amount) FROM phi.silver.acme_claims__claims").fetchone()
    fact = con.execute("SELECT count(*), count(DISTINCT claim_id), sum(paid_amount) FROM phi.gold.fact_claim").fetchone()
    assert fact == (silver[0], silver[0], silver[1])
    assert con.execute("SELECT DISTINCT partner FROM phi.gold.fact_claim").fetchall() == [("acme",)]


def test_provider_versions_never_overlap_and_each_provider_has_at_most_one_current(s, loaded):
    con = gold(s)
    assert con.execute("""SELECT count(*) FROM phi.gold.dim_provider a JOIN phi.gold.dim_provider b
        ON a.npi = b.npi AND a.provider_sk <> b.provider_sk
        AND a.valid_from < coalesce(b.valid_to, 'infinity'::TIMESTAMPTZ)
        AND b.valid_from < coalesce(a.valid_to, 'infinity'::TIMESTAMPTZ)""").fetchone()[0] == 0
    assert con.execute("""SELECT count(*) FROM (SELECT npi FROM phi.gold.dim_provider WHERE is_current
        GROUP BY npi HAVING count(*) > 1)""").fetchone()[0] == 0
    keys = con.execute("SELECT count(*), count(DISTINCT provider_sk) FROM phi.gold.dim_provider").fetchone()
    assert keys[0] == keys[1] > 0


def test_the_marts_agree_with_the_fact(s, loaded):
    con = gold(s)
    # Every unsuppressed cell of claims_monthly is the fact's own count for that month and code.
    mismatched = con.execute("""SELECT count(*) FROM deid.gold.claims_monthly m JOIN (
            SELECT date_trunc('month', service_date)::DATE AS service_month, diagnosis_code, count(*) AS claims
            FROM phi.gold.fact_claim GROUP BY ALL) f USING (service_month, diagnosis_code)
        WHERE m.claims <> f.claims""").fetchone()[0]
    assert mismatched == 0
    assert con.execute("SELECT count(*) FROM deid.gold.provider_activity").fetchone()[0] > 0


def test_a_directory_change_adds_a_version_and_earlier_claims_keep_theirs(s, loaded, run_id):
    found, directory = loaded
    con = gold(s)
    npi = con.execute("""SELECT d.npi FROM phi.gold.fact_claim f JOIN phi.gold.dim_provider d
        ON d.provider_sk = f.provider_sk ORDER BY d.npi LIMIT 1""").fetchone()[0]
    claim_keys = "SELECT DISTINCT provider_sk FROM phi.gold.fact_claim WHERE provider_npi = ? ORDER BY 1"
    before = con.execute(claim_keys, [npi]).fetchall()
    all_keys = {r[0] for r in con.execute("SELECT provider_sk FROM phi.gold.dim_provider").fetchall()}
    current = con.execute("SELECT provider_sk FROM phi.gold.dim_provider WHERE npi = ? AND is_current", [npi]).fetchone()[0]
    changed = f"Changed {run_id}"
    raw.RawZone(s).drop(found["provider_directory"], f"providers_star2_{run_id}.csv",
                        directory_with(directory, npi, changed))
    ingest(source="provider_directory")
    refresh(serve=False)

    con = gold(s)
    new_sk, new_from = con.execute("""SELECT provider_sk, valid_from FROM phi.gold.dim_provider
        WHERE npi = ? AND is_current AND specialty = ?""", [npi, changed]).fetchone()
    assert con.execute("SELECT valid_to, is_current FROM phi.gold.dim_provider WHERE provider_sk = ?",
                       [current]).fetchone() == (new_from, False)
    # The claims' service dates are before the change landed: they keep the versions they had.
    assert con.execute(claim_keys, [npi]).fetchall() == before
    # The run wrote only what changed: every key from before is still there.
    assert all_keys <= {r[0] for r in con.execute("SELECT provider_sk FROM phi.gold.dim_provider").fetchall()}
    assert new_sk not in {r[0] for r in before}
    assert changed not in {r[0] for r in con.execute("SELECT specialty FROM deid.gold.provider_activity").fetchall()}


def test_every_version_carries_its_watermark_columns(s, loaded):
    """first_seen_at (and ended_by_batch_id on ended versions) are what the next run starts from; a
    table deployed before they existed is rebuilt on its first run."""
    con = gold(s)
    assert con.execute("""SELECT count(*) FROM phi.gold.dim_provider
        WHERE first_seen_at IS NULL OR source_batch_id IS NULL
           OR (valid_to IS NOT NULL AND ended_by_batch_id IS NULL)""").fetchone()[0] == 0


def test_a_run_with_nothing_new_leaves_the_dimension_as_it_was(s, loaded):
    con = gold(s)
    q = "SELECT * FROM phi.gold.dim_provider ORDER BY provider_sk"
    before = con.execute(q).fetchall()
    refresh(serve=False)
    assert gold(s).execute(q).fetchall() == before


def test_a_run_reads_only_the_bronze_files_after_its_watermark(s, loaded):
    """With nothing new, the dimension must not fetch every directory file in Bronze: its cutoff on
    _ingested_at has to reach the scan as a filter (a join to the watermark did not, and each run
    read all of Bronze). Counted from DuckDB's HTTP log on a throwaway connection: the log holds
    request headers (loader connections keep it off), so only file paths are read from it here."""
    import re
    import tempfile
    from pathlib import Path

    from loader import gold as gold_mod

    with tempfile.TemporaryDirectory() as tmp:
        gold_mod.write_profile(s, Path(tmp))
        sql = {m.name: m.sql for m in gold_mod.models(s, Path(tmp))}["dim_provider"]
    con = gold(s)
    files = con.execute("""SELECT count(*) FROM iceberg_metadata('phi.bronze.provider_directory__providers')
        WHERE manifest_content = 'DATA' AND status <> 'DELETED'""").fetchone()[0]
    if files < 2:
        pytest.skip("Bronze holds a single data file: nothing to skip")
    con.execute("CALL enable_logging('HTTP')")
    assert con.execute(f"SELECT count(*) FROM ({sql})").fetchone()[0] == 0  # nothing new to write
    fetched = {path for (message,) in con.execute("SELECT message FROM duckdb_logs WHERE type = 'HTTP'").fetchall()
               for path in re.findall(r"/bronze/provider_directory__providers/data/[^\s\"'?]+\.parquet", message)}
    con.close()
    assert 0 < len(fetched) < files, f"fetched {len(fetched)} of {files} Bronze data files"


def test_fact_lab_result_holds_every_result_of_both_labs_once(s, loaded):
    con = gold(s)
    silver = sum(con.execute(f"SELECT count(*) FROM phi.silver.{t}").fetchone()[0]
                 for t in ("lab_results__results", "acme_lab_mail__results"))
    fact = con.execute("SELECT count(*), count(DISTINCT lab_result_key) FROM phi.gold.fact_lab_result").fetchone()
    assert fact == (silver, silver)
    partners = dict(con.execute("SELECT DISTINCT source_name, partner FROM phi.gold.fact_lab_result").fetchall())
    assert set(partners.items()) <= {("lab_results", "metro_lab"), ("acme_lab_mail", "acme")}


def test_lab_monthly_has_lost_its_minimum_and_maximum_columns(s, loaded):
    """A column removed from a Gold model leaves the Iceberg table at deploy, not just the model."""
    columns = [r[0] for r in gold(s).execute("DESCRIBE deid.gold.lab_monthly").fetchall()]
    assert "min_value" not in columns and "max_value" not in columns and "sum_value" in columns


def test_a_refresh_with_nothing_new_rewrites_nothing(s, loaded):
    """Gold rewrites whole tables; with no source changed since the last run it must not copy them
    again (it ran every 10 minutes, about 1,000 copies per table kept for a week)."""
    from loader import gold as gold_mod
    from loader.polaris import Polaris

    def current(table):
        return Polaris(s).load_table("phi", "gold", table)["metadata"]["current-snapshot-id"]

    gold_mod.run(s)  # whatever ran last, record a run of the current inputs
    before = current("fact_claim")
    assert gold_mod.run(s) == []
    assert current("fact_claim") == before
    assert gold_mod.run(s, force=True), "force rebuilds anyway"
    assert current("fact_claim") != before


def test_rewritten_gold_tables_keep_a_day_of_snapshots_and_dimensions_a_week(s, loaded):
    from loader import maintenance
    from loader.polaris import Polaris

    def expiry(catalog, table):
        effective = maintenance.policies(Polaris(s, "maintenance"), catalog, "gold", table)
        return effective["system.snapshot-expiry"]["config"]["max-snapshot-age-hours"]

    assert expiry("phi", "fact_claim") == 24 and expiry("deid", "claims_monthly") == 24
    assert expiry("phi", "dim_provider") == 168  # history: incremental writes, kept as the default


def test_metric_queries_run_as_the_metrics_principal_and_match_gold(s, loaded):
    """The semantic layer on the stack: MetricFlow's SQL, run as `metrics`, equals a direct query on
    Gold; and `metrics` reads the semantic tables only, not Silver, Bronze or the marts."""
    from loader import semantic

    columns, rows = semantic.query(s, ["claims", "paid", "distinct_members"], ["provider_version__state"])
    got = {r[columns.index("provider_version__state")]: (r[columns.index("claims")], r[columns.index("paid")],
                                                         r[columns.index("distinct_members")]) for r in rows}
    expected = {r[0]: tuple(r[1:]) for r in gold(s).execute("""SELECT d.state, count(*), sum(f.paid_amount),
        count(DISTINCT f.subject_id) FROM phi.gold.fact_claim f
        LEFT JOIN phi.gold.dim_provider d ON d.provider_sk = f.provider_sk GROUP BY 1""").fetchall()}
    assert got == expected and got
    # A limit with its order is honoured on Gold (without one, DuckDB ignores it there: sql() refuses).
    _, first = semantic.query(s, ["claims"], ["metric_time__month"], ["metric_time__month"], limit=1)
    assert len(first) == 1
    con = duck.connect(s, "phi", principal=semantic.PRINCIPAL, alias="phi")
    duck.attach_polaris(con, s, "deid", principal=semantic.PRINCIPAL, alias="deid")
    assert con.execute("SELECT count(*) FROM phi.gold.fact_claim").fetchone()[0] > 0
    for table in ("phi.silver.acme_claims__claims", "phi.bronze.provider_directory__providers",
                  "phi.gold.member_claims", "deid.gold.claims_monthly"):
        with pytest.raises(Exception, match="(?i)forbidden|not authorized|denied|403|does not exist"):
            con.execute(f"SELECT count(*) FROM {table}").fetchone()
