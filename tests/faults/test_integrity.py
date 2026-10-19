"""Integrity check and repair: a committed data file that disappears from storage is detected,
the table is brought back to a readable state, and the next ingest restores every row. A writer that
loses a file to DuckDB 1.5.6's late-commit failure heals its own table."""
import dataclasses
import uuid
from datetime import date

import botocore.exceptions
import pytest

from flows.ingest import ingest
from loader import duck, integrity, maintenance, pipeline, synthetic, tables
from loader.state import SILVER_COMMITTED, Manifest
from tests.conformance.test_slow_commit import SlowCommits
from tests.pipeline.conftest import deploy_copy, drop, remove_source


@pytest.fixture
def claims(s, run_id, contracts_dir, canaries):
    contract = deploy_copy(s, contracts_dir, "acme_claims", f"integ_{uuid.uuid4().hex[:6]}_{run_id}")
    try:  # cleanup also when the setup itself fails (pytest skips teardown then)
        total = 0
        for week in range(2):
            body = synthetic.claims_csv(canaries, date(2026, 5, 1 + 7 * week), days=3, per_day=6, seed=40 + week,
                                        id_offset=1000 * week)
            total += body.count(b"\n") - 1
            drop(s, contract, f"week{week}.csv", body)
            ingest(source=contract.source, contracts_dir=str(contracts_dir))  # one batch per run: two snapshots
        yield contract, total
    finally:
        remove_source(s, contract)


def lose_file(s, contract, layer: str, newest: bool = True) -> str:
    """Delete a data file from storage as the failure did: with the table's own vended credentials
    (DuckDB deleted the files of a commit it took as failed). The lake keeps the file's previous
    version, so repair copies it back; tests of a loss older than that switch the copy off
    (`no_restore`)."""
    from loader.polaris import Polaris, vended_s3

    con = duck.connect(s, tables.CATALOG, principal=contract.principal)
    path = con.execute(f"SELECT file_path FROM iceberg_metadata('{tables.qualified(contract, layer)}') "
                       f"WHERE status <> 'DELETED' AND file_path LIKE '%.parquet' "
                       f"ORDER BY manifest_sequence_number {'DESC' if newest else 'ASC'}, file_path LIMIT 1").fetchone()[0]
    bucket, key = path.removeprefix("s3://").split("/", 1)
    body = Polaris(s, contract.principal).load_table(tables.CATALOG, layer, contract.table, vended=True)
    s.s3(vended_s3(body)).delete_object(Bucket=bucket, Key=key)
    return path


@pytest.fixture
def no_restore(monkeypatch):
    """The loss is older than the lake keeps earlier versions: nothing to copy back."""
    monkeypatch.setattr(integrity, "restore", lambda *_: [])


def counts(s, contract) -> tuple[int, int]:
    """Rows of Bronze and Silver, read from their data files (a bare count(*) is answered from
    manifest row counts and would not notice a missing file)."""
    con = duck.connect(s, tables.CATALOG, principal=contract.principal)
    return tuple(con.execute(f"SELECT count(*) FROM {tables.qualified(contract, layer)} WHERE length(_batch_id) > 0").fetchone()[0]
                 for layer in (tables.BRONZE, tables.SILVER))


@pytest.mark.parametrize("layer", [tables.BRONZE, tables.SILVER])
def test_a_lost_file_is_found_repaired_and_reloaded(s, claims, contracts_dir, layer, no_restore):
    claims, expected = claims
    lost = lose_file(s, claims, layer)
    report = integrity.check(s, tables.CATALOG, layer, claims.table)
    assert not report.ok and lost in report.missing

    result = integrity.repair(s, tables.CATALOG, layer, claims.table)
    assert result["repaired"] and result["rolled_back_to"] and result["batches_reset"] >= 1
    assert not result["unrecoverable_batches"]
    assert integrity.check(s, tables.CATALOG, layer, claims.table).ok  # readable again at once

    ingest(source=claims.source, contracts_dir=str(contracts_dir))  # the next run redoes the batches
    assert {b.state for b in Manifest(s).for_source(claims.source)} == {SILVER_COMMITTED}
    assert counts(s, claims) == (expected, expected)
    assert integrity.check(s, tables.CATALOG, layer, claims.table).ok


def test_maintenance_repairs_instead_of_maintaining(s, claims, contracts_dir):
    claims, expected = claims
    lose_file(s, claims, tables.BRONZE)
    run = maintenance.maintain_table(s, maintenance.executor(s), tables.CATALOG, tables.BRONZE, claims.table)
    assert run.results["integrity"]["repaired"]
    ingest(source=claims.source, contracts_dir=str(contracts_dir))
    assert counts(s, claims) == (expected, expected)


def test_a_healthy_table_passes(s, claims):
    claims, _ = claims
    for layer in (tables.BRONZE, tables.SILVER):
        report = integrity.check(s, tables.CATALOG, layer, claims.table)
        assert report.ok and report.referenced > 2


def test_with_no_whole_snapshot_an_operator_recreates_the_table(s, claims, contracts_dir, capsys, no_restore):
    from loader import cli

    claims, expected = claims
    lose_file(s, claims, tables.BRONZE, newest=False)  # the first batch's file: no earlier snapshot
    with pytest.raises(integrity.NeedsOperator):
        integrity.repair(s, tables.CATALOG, tables.BRONZE, claims.table)
    cli.main(["--contracts-dir", str(contracts_dir), "repair", "--recreate", claims.source, "--layer", "bronze", "--yes"])
    assert '"recreated": true' in capsys.readouterr().out
    ingest(source=claims.source, contracts_dir=str(contracts_dir))
    assert counts(s, claims) == (expected, expected)
    assert integrity.check(s, tables.CATALOG, tables.BRONZE, claims.table).ok


@pytest.mark.parametrize("layer", [tables.BRONZE, tables.SILVER])
def test_a_lost_file_is_restored_from_its_previous_version(s, claims, layer):
    claims, expected = claims
    lost = lose_file(s, claims, layer)
    assert lost in integrity.check(s, tables.CATALOG, layer, claims.table).missing
    result = integrity.repair(s, tables.CATALOG, layer, claims.table)
    assert result["repaired"] and result["restored"] == 1
    assert result["rolled_back_to"] is None and result["batches_reset"] == 0  # nothing to redo
    assert integrity.check(s, tables.CATALOG, layer, claims.table).ok
    assert counts(s, claims) == (expected, expected)


def test_the_restore_identity_can_only_copy_versions_back(s, claims):
    claims, _ = claims
    con = duck.connect(s, tables.CATALOG, principal=claims.principal)
    path = con.execute(f"SELECT file_path FROM iceberg_metadata('{tables.qualified(claims, tables.SILVER)}') "
                       "WHERE file_path LIKE '%.parquet' LIMIT 1").fetchone()[0]
    bucket, key = path.removeprefix("s3://").split("/", 1)
    restore = s.s3("lake-restore")

    def denied(fn) -> bool:
        try:
            fn()
            return False
        except botocore.exceptions.ClientError as err:
            return err.response["Error"]["Code"] in ("AccessDenied", "403")

    assert denied(lambda: restore.put_object(Bucket=bucket, Key=key, Body=b"overwritten"))
    assert denied(lambda: restore.put_object(Bucket=bucket, Key=key.rsplit("/", 1)[0] + "/new.parquet", Body=b"x"))
    assert denied(lambda: restore.delete_object(Bucket=bucket, Key=key))
    version = restore.list_object_versions(Bucket=bucket, Prefix=key)["Versions"][0]["VersionId"]
    assert denied(lambda: restore.delete_object(Bucket=bucket, Key=key, VersionId=version))
    assert denied(lambda: restore.copy_object(Bucket=s.bucket("raw"), Key="landed/x", CopySource={"Bucket": bucket, "Key": key}))


@pytest.fixture
def late_commits(s, monkeypatch):
    """Settings whose catalog requests go through a proxy that answers the first commit late, with a
    short DuckDB timeout: DuckDB 1.5.6 then resends it, gets 409 and deletes the files of the commit
    Polaris applied (tests/conformance/test_slow_commit.py)."""
    monkeypatch.setenv("LOADER_HTTP_TIMEOUT", "3")
    proxy = SlowCommits(s.polaris_uri, delay=8)
    yield proxy, dataclasses.replace(s, polaris_uri=proxy.uri.removesuffix("/"))
    proxy.close()


@pytest.mark.parametrize("restorable", [True, False])
def test_a_writer_heals_the_table_it_broke(s, claims, canaries, late_commits, monkeypatch, restorable):
    """The real failure, end to end: Bronze's commit for a new batch is answered late, DuckDB deletes
    the applied commit's file, and the same ingest finds it, repairs the table and loads the batch.
    With the earlier version kept, the file is copied back; without, the table is rolled back and the
    pass starts over."""
    if duckdb_fixed():
        pytest.skip("this DuckDB does not resend a late commit")
    claims, expected = claims
    proxy, slow = late_commits
    if not restorable:
        monkeypatch.setattr(integrity, "restore", lambda *_: [])
    body = synthetic.claims_csv(canaries, date(2026, 5, 15), days=3, per_day=6, seed=42, id_offset=2000)
    drop(s, claims, "week2.csv", body)
    pipeline.ingest(slow, claims)
    assert proxy.commits >= 2, "the late commit was resent"
    assert {b.state for b in Manifest(s).for_source(claims.source)} == {SILVER_COMMITTED}
    added = body.count(b"\n") - 1
    assert counts(s, claims) == (expected + added, expected + added)
    for layer in (tables.BRONZE, tables.SILVER):
        assert integrity.check(s, tables.CATALOG, layer, claims.table).ok


def duckdb_fixed() -> bool:
    import duckdb

    return not duckdb.__version__.startswith("1.")
