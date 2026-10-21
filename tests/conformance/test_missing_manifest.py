"""A table whose manifest is gone from storage must fail the query with an error the loader can
handle, not abort the process (DuckDB 1.5.6 aborted: one bad table killed a whole worker)."""
import subprocess
import sys

from loader import duck


def test_a_missing_manifest_is_a_clean_error(s, table):
    _, qualified = table
    con = duck.connect(s, "phi")
    con.execute(f"CREATE TABLE {qualified} AS SELECT i AS id FROM range(100) r(i)")
    con.execute(f"INSERT INTO {qualified} SELECT i FROM range(100, 200) r(i)")
    manifest = con.execute(f"SELECT manifest_path FROM iceberg_metadata('{qualified}') LIMIT 1").fetchone()[0]
    bucket, key = manifest.removeprefix("s3://").split("/", 1)
    s.s3("admin").delete_object(Bucket=bucket, Key=key)
    # In a child process: an abort would take the test runner down with it.
    code = subprocess.run([sys.executable, "-c", "import sys\nfrom loader import duck, settings\n"
                           f"try:\n    duck.connect(settings.load(), 'phi').execute('SELECT count(*), max(id) FROM {qualified}')"
                           ".fetchone()\nexcept Exception:\n    sys.exit(3)\n"]).returncode
    assert code == 3, f"expected a clean error (exit 3), got {code}"
