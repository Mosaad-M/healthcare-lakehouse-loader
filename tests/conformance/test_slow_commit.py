"""A catalog commit that is slow to answer: the root cause of the missing-file failures.

Twice (once in CI, once in a benchmark) a table ended up referencing a data file that was gone,
right after Polaris had stalled and DuckDB had resent a commit past its HTTP timeout. DuckDB 1.5.x
does this: the first commit is applied but answered late, the resend gets 409, and DuckDB, taking
the transaction as failed, deletes the data files it wrote, which the applied snapshot references.
(Iceberg's Java library treats such an outcome as unknown and keeps the files.) It happens on a table
that already has snapshots; the first commit to an empty table takes another path. DuckDB 2.0 (dev,
October 2026) does not resend the commit and keeps the files.

The loader never times out on the catalog (loader/duck.py: 600 s), so DuckDB waits instead of
resending; loader/integrity.py repairs a table if it happens anyway. A proxy here forwards the commit
at once and holds the answer back.
"""
import http.client
import http.server
import threading
import time
import urllib.parse

import duckdb
import pytest

from loader import duck


class SlowCommits:
    """Forwards everything to Polaris; answers the first `slow` commit POSTs `delay` seconds late."""

    def __init__(self, target: str, delay: float, slow: int = 1):
        self.target, self.delay, self.slow, self.commits = urllib.parse.urlsplit(target), delay, slow, 0
        proxy = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def _forward(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", 0) or 0))
                conn = http.client.HTTPConnection(proxy.target.hostname, proxy.target.port, timeout=120)
                headers = {k: v for k, v in self.headers.items() if k.lower() not in ("host", "connection")}
                conn.request(self.command, self.path, body=body or None, headers=headers)
                resp = conn.getresponse()
                data = resp.read()
                is_commit = self.command == "POST" and "/tables/" in self.path
                if is_commit:
                    proxy.commits += 1
                    if proxy.commits <= proxy.slow:
                        time.sleep(proxy.delay)  # applied by Polaris, answered late
                self.send_response(resp.status)
                for k, v in resp.getheaders():
                    if k.lower() not in ("transfer-encoding", "connection", "content-length"):
                        self.send_header(k, v)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            do_GET = do_POST = do_DELETE = do_HEAD = _forward

            def log_message(self, *args):
                pass

        self.server = http.server.ThreadingHTTPServer(("0.0.0.0", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    @property
    def uri(self) -> str:
        return f"http://127.0.0.1:{self.server.server_port}{self.target.path}"

    def close(self):
        self.server.shutdown()


def through(s, proxy: SlowCommits) -> duckdb.DuckDBPyConnection:
    """A loader connection whose catalog requests go through the proxy."""
    con = duckdb.connect()
    duck.load_extensions(con, s)
    duck.deny_unscoped(con, s)
    client_id, secret = s.principal("loader")
    con.execute(f"CREATE SECRET p (TYPE iceberg, CLIENT_ID {duck.sql_str(client_id)}, CLIENT_SECRET {duck.sql_str(secret)}, "
                f"OAUTH2_SERVER_URI {duck.sql_str(s.token_uri)}, OAUTH2_SCOPE 'PRINCIPAL_ROLE:ALL')")
    con.execute(f"ATTACH 'phi' AS ice (TYPE iceberg, ENDPOINT {duck.sql_str(proxy.uri)}, SECRET p, "
                "ACCESS_DELEGATION_MODE 'vended_credentials', DISABLE_MULTI_TABLE_COMMIT true)")
    return con


@pytest.fixture
def table(s, ns):
    name = f"ice.{ns}.slow_{int(time.time() * 1000) % 10**9}"
    duck.connect(s, "phi").execute(f"CREATE TABLE {name} (id INTEGER)")
    return name


def readable(s, table) -> int | None:
    """Rows of the table, read from its data files (a bare count(*) is answered from manifest row
    counts and opens no data file, so it cannot tell that one is gone)."""
    try:
        return len(duck.connect(s, "phi").execute(f"SELECT list(id) FROM {table}").fetchone()[0] or [])
    except duckdb.Error:
        return None


def test_duckdb_1_5_loses_the_files_of_a_commit_answered_late(s, table):
    """DuckDB's behaviour, kept as a check: on 1.5.x the table is left referencing a deleted file; on
    later versions the commit is not resent and the table stays whole. When this changes with an
    upgrade, the long timeout in loader/duck.py can be revisited."""
    duck.connect(s, "phi").execute(f"INSERT INTO {table} VALUES (0)")  # a snapshot exists: the failing case
    proxy = SlowCommits(s.polaris_uri, delay=8)
    try:
        con = through(s, proxy)
        con.execute("SET http_timeout = 3; SET http_retries = 1")
        with pytest.raises(duckdb.Error):
            con.execute("BEGIN")
            con.execute(f"INSERT INTO {table} VALUES (1), (2)")
            con.execute("COMMIT")
        try:  # what duck.run_txn does after a failed attempt
            con.execute("ROLLBACK")
        except duckdb.Error:
            pass
    finally:
        time.sleep(1)
        proxy.close()
    if duckdb.__version__.startswith("1."):
        assert proxy.commits >= 2, "DuckDB 1.x resends the commit"
        assert readable(s, table) is None, "the applied commit's data file is expected to be gone on DuckDB 1.x"
    else:
        assert readable(s, table) == 3


def test_the_loader_waits_for_a_slow_commit(s, table):
    proxy = SlowCommits(s.polaris_uri, delay=8)
    try:
        con = through(s, proxy)  # the loader's settings: a long HTTP timeout
        con.execute("BEGIN")
        con.execute(f"INSERT INTO {table} VALUES (1), (2)")
        con.execute("COMMIT")
    finally:
        proxy.close()
    assert proxy.commits == 1
    assert readable(s, table) == 2
