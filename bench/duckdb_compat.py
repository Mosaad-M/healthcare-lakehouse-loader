"""Which of the DuckDB behaviours the loader works around does a given DuckDB version still have?

    python -m bench.duckdb_compat [check ...]      (inside the stack network; see below)

Each check runs in its own process (an abort is a result, not the end of the run) against the local
stack, and prints `RESULT {json}` lines: the DuckDB version, then one line per check with what
happened. Extensions are installed for the running DuckDB into a temporary directory (the nightly
repository for dev builds), so the same script tests 1.5.6 and pre-releases:

    docker compose ... run --rm -T toolbox bash -c \\
        "pip install -q --target /tmp/dd duckdb==<version> && PYTHONPATH=/tmp/dd python -m bench.duckdb_compat"
"""
import json
import os
import subprocess
import sys
import tempfile
import time
import uuid

CHECKS = ["merge_update_and_delete", "delete_all_with_a_missing_file", "removed_main_ref_reads_empty",
          "missing_manifest_errors_cleanly", "manifest_reads_per_query", "http_log_has_no_bearer_token",
          "defaults", "iceberg_metadata_at_a_snapshot", "rename_table", "slow_commit_outcome",
          "concurrent_writers_keep_credentials"]


def prepare_extensions() -> str:
    import duckdb

    path = tempfile.mkdtemp(prefix="ddext-")
    con = duckdb.connect()
    con.execute(f"SET extension_directory = '{path}'")
    for ext in ("httpfs", "aws", "iceberg", "avro"):
        try:
            con.execute(f"INSTALL {ext}")
        except duckdb.Error:
            con.execute(f"INSTALL {ext} FROM core_nightly")
    return path


def setup(name: str):
    """A fresh namespace and table with two commits; returns (settings, connection factory, qualified, ns)."""
    from loader import duck, settings

    s = settings.load()
    ns = f"compat_{uuid.uuid4().hex[:8]}"
    con = duck.connect(s, "phi")
    con.execute(f"CREATE SCHEMA ice.{ns}")
    con.execute(f"CREATE TABLE ice.{ns}.{name} AS SELECT i AS id, 'v' || i AS v FROM range(100) r(i)")
    con.execute(f"INSERT INTO ice.{ns}.{name} SELECT i AS id, 'w' || i AS v FROM range(100, 200) r(i)")
    return s, (lambda: duck.connect(s, "phi")), f"ice.{ns}.{name}", ns


def drop_storage_file(s, path: str) -> None:
    bucket, key = path.removeprefix("s3://").split("/", 1)
    s.s3("admin").delete_object(Bucket=bucket, Key=key)


def run_check(name: str) -> dict:
    import duckdb

    from loader.polaris import Polaris

    if name == "merge_update_and_delete":
        s, connect, t, _ = setup("m")
        try:
            connect().execute(f"MERGE INTO {t} AS d USING (SELECT i AS id FROM range(0, 300, 50) r(i)) AS c ON d.id = c.id "
                              "WHEN MATCHED AND d.id < 100 THEN UPDATE SET v = 'updated' WHEN MATCHED THEN DELETE "
                              "WHEN NOT MATCHED THEN INSERT VALUES (c.id, 'new')")
            got = connect().execute(f"SELECT count(*) FILTER (v = 'updated'), count(*) FILTER (v = 'new'), count(*) FROM {t}").fetchone()
            return {"ok": True, "updated_new_total": list(got)}
        except duckdb.Error as err:
            return {"ok": False, "error": str(err)[:200]}

    if name == "delete_all_with_a_missing_file":
        s, connect, t, _ = setup("d")
        path = connect().execute(f"SELECT file_path FROM iceberg_metadata('{t}') WHERE file_path LIKE '%.parquet' LIMIT 1").fetchone()[0]
        drop_storage_file(s, path)
        try:
            connect().execute(f"DELETE FROM {t}")
            return {"ok": True, "rows_after": connect().execute(f"SELECT count(*) FROM {t}").fetchone()[0]}
        except duckdb.Error as err:
            return {"ok": False, "error": str(err)[:200]}

    if name == "removed_main_ref_reads_empty":
        s, connect, t, ns = setup("r")
        p = Polaris(s, "loader")
        current = p.load_table("phi", ns, "r")["metadata"]["current-snapshot-id"]
        p.rest("POST", p.table_url("phi", ns, "r"), {
            "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": current}],
            "updates": [{"action": "remove-snapshot-ref", "ref-name": "main"}]})
        rows = connect().execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        return {"ok": rows == 0, "rows_read": rows}

    if name == "missing_manifest_errors_cleanly":
        s, connect, t, _ = setup("x")
        manifest = connect().execute(f"SELECT manifest_path FROM iceberg_metadata('{t}') LIMIT 1").fetchone()[0]
        drop_storage_file(s, manifest)
        code = subprocess.run([sys.executable, "-c", "import os, sys\nfrom loader import duck, settings\n"
                               f"try:\n    duck.connect(settings.load(), 'phi').execute(\"SELECT count(*), max(v) FROM {t}\").fetchone()\n"
                               "except Exception:\n    sys.exit(3)\n"], env=os.environ).returncode
        return {"ok": code == 3, "exit_code": code, "meaning": {3: "clean error", 0: "read succeeded?!", -6: "aborted (SIGABRT)",
                                                              134: "aborted (SIGABRT)"}.get(code, "other")}

    if name == "manifest_reads_per_query":
        s, connect, t, _ = setup("q")
        con = connect()
        logdir = tempfile.mkdtemp()
        con.execute(f"CALL enable_logging('HTTP', storage='file', storage_path='{logdir}')")
        con.execute(f"SELECT count(*) FROM {t}").fetchone()
        con.close()
        text = open(os.path.join(logdir, "duckdb_log_entries.csv")).read()
        manifests = [line for line in text.splitlines() if "-m0.avro" in line or "-m1.avro" in line]
        return {"ok": True, "manifest_gets": len(manifests)}

    if name == "http_log_has_no_bearer_token":
        s, connect, t, _ = setup("h")
        con = connect()
        logdir = tempfile.mkdtemp()
        con.execute(f"CALL enable_logging('HTTP', storage='file', storage_path='{logdir}')")
        con.execute(f"SELECT count(*) FROM {t}").fetchone()
        con.close()
        text = open(os.path.join(logdir, "duckdb_log_entries.csv")).read()
        return {"ok": "Bearer " not in text and "X-Amz-Security-Token=" not in text,
                "bearer_in_log": "Bearer " in text}

    if name == "defaults":

        con = duckdb.connect()
        con.execute(f"SET extension_directory = '{os.environ['LOADER_DUCKDB_EXTENSION_DIR']}'")
        for ext in ("httpfs", "iceberg"):
            con.execute(f"LOAD {ext}")
        vals = dict(con.execute("SELECT name, value FROM duckdb_settings() WHERE name IN "
                                "('httpfs_connection_caching', 'temp_file_encryption', 'memory_limit', 'http_timeout', "
                                "'http_retries', 'temp_directory')").fetchall())
        return {"ok": True, **vals}

    if name == "iceberg_metadata_at_a_snapshot":
        s, connect, t, _ = setup("s")
        con = connect()
        first = con.execute(f"SELECT snapshot_id FROM iceberg_snapshots('{t}') ORDER BY sequence_number LIMIT 1").fetchone()[0]
        out = {}
        for arg in ("version", "snapshot_from_id"):
            try:
                out[arg] = con.execute(f"SELECT count(*) FROM iceberg_metadata('{t}', {arg} => {first}) WHERE status <> 'DELETED'").fetchone()[0]
            except duckdb.Error as err:
                out[arg] = "error: " + str(err)[:80]
        out["current"] = con.execute(f"SELECT count(*) FROM iceberg_metadata('{t}') WHERE status <> 'DELETED'").fetchone()[0]
        return {"ok": out.get("version") == 1, **out}

    if name == "rename_table":
        s, connect, t, ns = setup("n")
        try:
            connect().execute(f"ALTER TABLE {t} RENAME TO n2")
            return {"ok": True, "rows": connect().execute(f"SELECT count(*) FROM ice.{ns}.n2").fetchone()[0]}
        except duckdb.Error as err:
            return {"ok": False, "error": str(err)[:200]}

    if name == "slow_commit_outcome":
        from tests.conformance.test_slow_commit import SlowCommits, through

        s, connect, t, ns = setup("c")
        proxy = SlowCommits(s.polaris_uri, delay=8)
        try:
            con = through(s, proxy)
            con.execute("SET http_timeout = 3; SET http_retries = 1")
            try:
                con.execute("BEGIN"); con.execute(f"INSERT INTO ice.{ns}.c VALUES (999, 'x')"); con.execute("COMMIT")
                client = "succeeded"
            except duckdb.Error as err:
                client = "error: " + str(err)[:100]
        finally:
            time.sleep(10)
            proxy.close()
        try:
            rows = connect().execute(f"SELECT count(*) FROM {t} WHERE id = 999").fetchone()[0]
            readable = True
        except duckdb.Error as err:
            rows, readable = None, "404" in str(err)
            readable = "table lost its data file (404)" if readable else str(err)[:120]
        return {"ok": readable is True, "commits_sent": proxy.commits, "client": client, "readable": readable,
                "applied_rows": rows}

    if name == "concurrent_writers_keep_credentials":
        import re

        out = subprocess.run([sys.executable, "scripts/repro_lost_credentials.py", "processes", "5", "--no-retry"],
                             capture_output=True, text=True, env=os.environ).stdout
        match = re.search(r"(\d+)/(\d+) trials with errors", out)
        failed = int(match.group(1)) if match else -1
        return {"ok": failed == 0, "trials_with_errors": failed, "trials": 5,
                "lost_credentials_seen": "s3-unscoped" in out}

    raise ValueError(name)


def main() -> None:
    if len(sys.argv) > 2 and sys.argv[1] == "--one":
        print("RESULT " + json.dumps({"check": sys.argv[2], **run_check(sys.argv[2])}, default=str), flush=True)
        return
    import duckdb

    os.environ["LOADER_DUCKDB_EXTENSION_DIR"] = prepare_extensions()
    print("RESULT " + json.dumps({"duckdb": duckdb.__version__}), flush=True)
    for check in sys.argv[1:] or CHECKS:
        proc = subprocess.run([sys.executable, "-m", "bench.duckdb_compat", "--one", check], capture_output=True, text=True,
                              env=os.environ, timeout=900)
        lines = [line for line in proc.stdout.splitlines() if line.startswith("RESULT ")]
        if lines:
            print(lines[-1], flush=True)
        else:
            tail = [line for line in proc.stderr.splitlines() if line.strip()][-2:]
            print("RESULT " + json.dumps({"check": check, "ok": False, "exit_code": proc.returncode,
                                          "error": " | ".join(tail)[:300]}), flush=True)


if __name__ == "__main__":
    main()
