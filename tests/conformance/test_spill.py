"""DuckDB spills (staging holds plaintext before encryption) are encrypted and stay out of the
working tree."""
from pathlib import Path

import duckdb

from loader import duck


def test_spill_files_are_encrypted_and_private(s, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    con = duckdb.connect()
    duck.load_extensions(con, s)
    settings = dict(con.execute("SELECT name, value FROM duckdb_settings() WHERE name IN "
                                "('temp_file_encryption', 'temp_directory')").fetchall())
    assert settings["temp_file_encryption"] == "true"
    spill = Path(settings["temp_directory"])
    assert not spill.is_relative_to(tmp_path) and oct(spill.stat().st_mode & 0o777) == "0o700"
    # Force a spill and look for the plaintext.
    con.execute("SET memory_limit = '32MB'; SET preserve_insertion_order = false")
    con.execute("CREATE TEMP TABLE t AS SELECT 'Synthetic Member ' || i AS name FROM range(5000000) r(i)")
    blobs = [p.read_bytes() for p in spill.rglob("*") if p.is_file()]
    assert blobs, "nothing spilled; the test did not exercise spilling"
    assert not any(b"Synthetic Member" in b for b in blobs)
    assert not (tmp_path / ".tmp").exists()


def test_duckdb_http_logging_is_off(s):
    """DuckDB's HTTP log records request headers, bearer tokens included (seen in testing)."""
    con = duckdb.connect()
    duck.load_extensions(con, s)
    assert con.execute("SELECT count(*) FROM duckdb_logs").fetchone() == (0,)
    assert con.execute("SELECT value FROM duckdb_settings() WHERE name = 'enable_logging'").fetchone() in (("false",), ("0",), None)


def test_an_ingest_leaves_nothing_in_the_shared_temp_directory(s, run_id, contracts_dir, canaries, tmp_path, monkeypatch):
    """Drops are hashed as they stream and Bronze stages in the private spill directory, so the
    shared temp directory never holds a batch's rows."""
    import tempfile
    from datetime import date

    from loader import pipeline, synthetic
    from loader.state import SILVER_COMMITTED, Manifest
    from tests.pipeline.conftest import deploy_copy, drop, remove_source

    spill = Path(duck.spill_directory())  # created before the temp directory is redirected
    made, unnamed = [], []
    mkdtemp, temporary_file = tempfile.mkdtemp, tempfile.TemporaryFile

    def recording_mkdtemp(*args, **kwargs):
        path = mkdtemp(*args, **kwargs)
        made.append(Path(path))
        return path

    def recording_temporary_file(*args, **kwargs):
        unnamed.append(kwargs.get("dir"))
        return temporary_file(*args, **kwargs)

    contract = deploy_copy(s, contracts_dir, "acme_claims", f"tmp_{run_id}")
    try:
        drop(s, contract, "claims.csv", synthetic.claims_csv(canaries, date(2026, 5, 1), days=2, per_day=5, seed=7))
        monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
        monkeypatch.setattr(tempfile, "mkdtemp", recording_mkdtemp)
        monkeypatch.setattr(tempfile, "TemporaryFile", recording_temporary_file)
        pipeline.ingest(s, contract)
        monkeypatch.undo()
        assert {b.state for b in Manifest(s).for_source(contract.source)} == {SILVER_COMMITTED}
        assert made and all(p.is_relative_to(spill) for p in made), f"temp directories outside the spill directory: {made}"
        assert not unnamed, "a file was written to an unnamed temp file"
        assert not list(tmp_path.rglob("*")), "the ingest left files in the shared temp directory"
        assert not [p for p in spill.rglob("*") if p.is_file()], "staging was not removed"
    finally:
        remove_source(s, contract)
