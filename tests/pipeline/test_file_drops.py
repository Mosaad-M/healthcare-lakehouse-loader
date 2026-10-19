"""File drops end to end: land, Bronze, Silver, through the Prefect ingest flow.

Tests in this module run in order and share per-run sources (see conftest.py).
"""
import csv
import io
from datetime import date
from decimal import Decimal

import pytest

from flows.ingest import ingest
from loader import bronze, duck, pipeline, synthetic, tables
from loader.state import QUARANTINED, SILVER_COMMITTED, Manifest
from tests.pipeline.conftest import deploy_copy, drop, remove_source


def run(contract, contracts_dir) -> None:
    ingest(source=contract.source, contracts_dir=str(contracts_dir))


def table_rows(s, contract, layer: str, order: str) -> list[tuple]:
    con = duck.connect(s, tables.CATALOG, principal=contract.principal)
    return con.execute(f"SELECT * FROM {tables.qualified(contract, layer)} ORDER BY {order}").fetchall()


def plain_rows(s, contract, layer: str, order: str) -> list[tuple]:
    """Rows with identifier ciphertext replaced by its plaintext (ciphertext differs per encryption)."""
    from loader import crypto
    from loader.vault import KeyVault

    con = duck.connect(s, tables.CATALOG, principal=contract.principal)
    cur = con.execute(f"SELECT * FROM {tables.qualified(contract, layer)} ORDER BY {order}")
    names = [d[0] for d in cur.description]
    rows = cur.fetchall()
    idents = {c.name for c in contract.identifiers}
    keys = KeyVault(s).get([r[names.index("subject_id")] for r in rows]) if idents else {}
    out = []
    for row in rows:
        sid = row[names.index("subject_id")] if idents else None
        out.append(tuple(crypto.decrypt(keys[sid], sid, n, bytes(v)) if n in idents and v is not None else v
                         for n, v in zip(names, row)))
    return out


def scalar(s, contract, sql: str):
    con = duck.connect(s, tables.CATALOG, principal=contract.principal)
    return con.execute(sql.format(bronze=tables.qualified(contract, tables.BRONZE),
                                  silver=tables.qualified(contract, tables.SILVER))).fetchone()


def source_totals(body: bytes) -> tuple[int, Decimal]:
    """Row count and exact billed total computed from the CSV itself, independently of DuckDB."""
    rows = list(csv.DictReader(io.StringIO(body.decode())))
    return len(rows), sum((Decimal(r["billed_amount"]) for r in rows), Decimal("0"))


# ---------------------------------------------------------------- claims: window semantics

def test_claims_load_matches_the_source(s, sources, canaries, contracts_dir):
    claims = sources["acme_claims"]
    body = synthetic.claims_csv(canaries, date(2026, 9, 1), days=5, per_day=10, seed=1)
    drop(s, claims, "Smith_John_claims_20260905.csv", body)  # file names may carry PHI
    run(claims, contracts_dir)

    rows, billed = source_totals(body)
    assert scalar(s, claims, "SELECT count(*), round(sum(billed_amount), 2) FROM {bronze}") == (rows, billed)
    assert scalar(s, claims, "SELECT count(*), round(sum(billed_amount), 2), count(DISTINCT claim_id) "
                             "FROM {silver}") == (rows, billed, rows)
    batches = Manifest(s).for_source(claims.source)
    assert [(b.state, b.rows_bronze, b.rows_silver) for b in batches] == [(SILVER_COMMITTED, rows, rows)]
    assert "Smith" not in batches[0].raw_key and "data.csv" in batches[0].raw_key


def test_claims_window_restatement_replaces_only_its_dates(s, sources, canaries, contracts_dir):
    claims = sources["acme_claims"]
    restated = synthetic.claims_csv(canaries, date(2026, 9, 4), days=4, per_day=8, seed=2)
    drop(s, claims, "claims_20260907.csv", restated)
    run(claims, contracts_dir)

    # Sep 1-3 from the first file (30 rows), Sep 4-7 exactly as restated (32 rows).
    assert scalar(s, claims, "SELECT count(*) FROM {silver}") == (62,)
    assert scalar(s, claims, "SELECT count(*) FROM {silver} WHERE service_date < DATE '2026-09-04'") == (30,)
    _, billed = source_totals(restated)
    assert scalar(s, claims, "SELECT round(sum(billed_amount), 2) FROM {silver} "
                             "WHERE service_date >= DATE '2026-09-04'") == (billed,)
    assert scalar(s, claims, "SELECT count(*) FROM {bronze}") == (82,)  # Bronze keeps both files


def test_rerun_and_redrop_change_nothing(s, sources, canaries, contracts_dir):
    claims = sources["acme_claims"]
    before = (plain_rows(s, claims, tables.BRONZE, "_batch_id, _row_number"),
              plain_rows(s, claims, tables.SILVER, "claim_id"))
    # The same file dropped again is recognized by its bytes; an empty run does nothing.
    drop(s, claims, "again.csv", synthetic.claims_csv(canaries, date(2026, 9, 4), days=4, per_day=8, seed=2))
    run(claims, contracts_dir)
    run(claims, contracts_dir)
    # Reprocessing a committed batch (as after a crash) replaces its rows instead of adding them.
    batch = Manifest(s).for_source(claims.source)[-1]
    bronze.write(s, claims, batch)
    pipeline.to_silver(s, claims, batch, pipeline.Summary())
    after = (plain_rows(s, claims, tables.BRONZE, "_batch_id, _row_number"),
             plain_rows(s, claims, tables.SILVER, "claim_id"))
    assert after == before
    assert len(Manifest(s).for_source(claims.source)) == 2


def test_replay_from_raw_reproduces_the_tables(s, sources, contracts_dir):
    """Same rows, subject IDs and decrypted identifiers; only the ciphertext bytes differ (new nonces)."""
    claims = sources["acme_claims"]
    before = (plain_rows(s, claims, tables.BRONZE, "_batch_id, _row_number"),
              plain_rows(s, claims, tables.SILVER, "claim_id"))
    ciphertext = table_rows(s, claims, tables.SILVER, "claim_id")
    con = duck.connect(s, tables.CATALOG, principal=claims.principal)
    con.execute(f"DELETE FROM {tables.qualified(claims, tables.BRONZE)}")
    con.execute(f"DELETE FROM {tables.qualified(claims, tables.SILVER)}")
    assert pipeline.replay(s, claims) == 2
    after = (plain_rows(s, claims, tables.BRONZE, "_batch_id, _row_number"),
             plain_rows(s, claims, tables.SILVER, "claim_id"))
    assert after == before
    assert table_rows(s, claims, tables.SILVER, "claim_id") != ciphertext


@pytest.mark.parametrize("bad", ["bad_date", "extra_column"])
def test_malformed_files_are_quarantined_with_nothing_committed(s, sources, canaries, contracts_dir, bad):
    claims = sources["acme_claims"]
    before = scalar(s, claims, "SELECT count(*) FROM {bronze}")
    body = (synthetic.claims_with_bad_date if bad == "bad_date" else synthetic.claims_with_extra_column)(canaries)
    drop(s, claims, f"{bad}.csv", body)
    run(claims, contracts_dir)

    assert scalar(s, claims, "SELECT count(*) FROM {bronze}") == before
    quarantined = [b for b in Manifest(s).for_source(claims.source) if b.state == QUARANTINED]
    latest = quarantined[-1]
    assert latest.raw_key.startswith(f"quarantine/{claims.source}/")
    expected = "service_date (1 rows)" if bad == "bad_date" else "undeclared columns: member_phone"
    assert expected in latest.reason
    assert not any(c in latest.reason for c in canaries.all())


@pytest.mark.parametrize("cap, value, reason", [("max_rows", 50, "has 100 rows; the contract allows 50"),
                                                ("max_file_bytes", 1000, "the contract allows 1000 (max_file_bytes)")])
def test_oversized_files_are_quarantined_before_loading(s, run_id, contracts_dir, cap, value, reason):
    """A file over the contract's caps never reaches memory: one that ran the task out of memory would
    be retried on every run."""
    contract = deploy_copy(s, contracts_dir, "provider_directory", f"capped_{cap}_{run_id}"[:40], **{cap: value})
    try:
        drop(s, contract, "big.csv", synthetic.providers_csv(100, seed=3))
        run(contract, contracts_dir)
        batches = Manifest(s).for_source(contract.source)
        assert [b.state for b in batches] == [QUARANTINED]
        assert reason in batches[0].reason
    finally:
        remove_source(s, contract)


def test_a_corrected_service_date_replaces_the_old_row(s, run_id, contracts_dir, canaries):
    """Window semantics: a restatement that moves a claim to another date replaces it, even though
    its old date is outside the restated range (found by the Silver property tests)."""
    claims = deploy_copy(s, contracts_dir, "acme_claims", f"corrected_{run_id}")
    header = "claim_id,member_id,member_ssn,member_name,service_date,diagnosis_code,procedure_code,provider_npi,billed_amount,paid_amount"
    row = "C{n},M{n},000000000,Synthetic Member {n},{day},Z00,99213,1000000001,100.00,{paid}"
    try:
        drop(s, claims, "week1.csv", "\n".join([header, row.format(n=1, day="2026-03-01", paid="80.00"),
                                                 row.format(n=2, day="2026-03-02", paid="90.00")]).encode() + b"\n")
        run(claims, contracts_dir)
        drop(s, claims, "fix.csv", "\n".join([header, row.format(n=1, day="2026-03-10", paid="85.00")]).encode() + b"\n")
        run(claims, contracts_dir)
        assert scalar(s, claims, "SELECT count(*) FROM {silver} WHERE claim_id = 'C1'") == (1,)
        assert scalar(s, claims, "SELECT CAST(service_date AS VARCHAR), CAST(paid_amount AS VARCHAR) FROM {silver} "
                                 "WHERE claim_id = 'C1'") == ("2026-03-10", "85.00")
        assert scalar(s, claims, "SELECT count(*) FROM {silver}") == (2,)
    finally:
        remove_source(s, claims)


# ---------------------------------------------------------------- providers: snapshot semantics

def test_provider_snapshot_replaces_the_directory(s, sources, contracts_dir):
    providers = sources["provider_directory"]
    drop(s, providers, "providers_week1.csv", synthetic.providers_csv(100, seed=1))
    run(providers, contracts_dir)
    assert scalar(s, providers, "SELECT count(*) FROM {silver}") == (100,)
    drop(s, providers, "providers_week2.csv", synthetic.providers_csv(80, seed=2))
    run(providers, contracts_dir)
    assert scalar(s, providers, "SELECT count(*), count(DISTINCT npi) FROM {silver}") == (80, 80)
    assert scalar(s, providers, "SELECT count(*) FROM {bronze}") == (180,)


# ---------------------------------------------------------------- lab results: append semantics

def test_lab_results_append_ignores_resent_results(s, sources, canaries, contracts_dir):
    labs = sources["lab_results"]
    drop(s, labs, "results_1.ndjson", synthetic.lab_ndjson(canaries, 100, seed=1))
    run(labs, contracts_dir)
    drop(s, labs, "results_2.ndjson", synthetic.lab_ndjson(canaries, 100, seed=1, id_offset=50))
    run(labs, contracts_dir)
    assert scalar(s, labs, "SELECT count(*), count(DISTINCT result_id) FROM {silver}") == (150, 150)
    assert scalar(s, labs, "SELECT count(*) FROM {bronze}") == (200,)
    # The first sending of R0000050..R0000099 wins; resent copies stay only in Bronze.
    assert scalar(s, labs, "SELECT count(DISTINCT _batch_id) FROM {silver} WHERE result_id < 'R0000050'") == (1,)


# ---------------------------------------------------------------- contracts

def test_a_new_source_needs_only_a_contract(s, run_id, canaries, contracts_dir):
    """A new file-drop source from a contract file alone, through the same flow."""
    contract = deploy_copy(s, contracts_dir, "lab_results", f"imaging_{run_id}", entity="events",
                           owner="imaging-team@example.org")
    try:
        drop(s, contract, "events.ndjson", synthetic.lab_ndjson(canaries, 40, seed=7))
        run(contract, contracts_dir)
        assert scalar(s, contract, "SELECT count(*) FROM {silver}") == (40,)
        assert [b.state for b in Manifest(s).for_source(contract.source)] == [SILVER_COMMITTED]
    finally:
        remove_source(s, contract)
