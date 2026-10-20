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
