"""DuckDB connections to a Polaris catalog, and the transaction helpers every writer uses."""
import atexit
import functools
import logging
import os
import random
import re
import shutil
import tempfile
import time
from collections.abc import Callable

import duckdb

from loader.settings import Settings

log = logging.getLogger(__name__)

CONFLICT_MARKERS = ("Conflict_409", "CommitFailedException", "CommitStateUnknown")
# Host of the fail-closed fallback secret (a network alias of the storage service).
UNSCOPED_HOST = "s3-unscoped"


def sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


@functools.cache
def spill_directory() -> str:
    """This process's private spill directory (mode 0700), removed at exit."""
    path = tempfile.mkdtemp(prefix="duckdb-spill-")
    atexit.register(shutil.rmtree, path, True)
    return path


def load_extensions(con: duckdb.DuckDBPyConnection, s: Settings) -> None:
    if s.duckdb_extension_dir:
        # Extensions are baked into the image; never download code at run time.
        con.execute("SET autoinstall_known_extensions = false")
        con.execute("SET autoload_known_extensions = false")
        con.execute(f"SET extension_directory = {sql_str(s.duckdb_extension_dir)}")
    else:
        con.execute("INSTALL httpfs; INSTALL aws; INSTALL iceberg; INSTALL avro")
    con.execute("LOAD httpfs; LOAD aws; LOAD iceberg; LOAD avro")
    # Large batches spill to disk, and staging holds plaintext (raw rows before encryption). Spill
    # files are encrypted with a key that lives only in this process, in a private directory, never
    # DuckDB's default `.tmp` under the working directory (locally that is the mounted repository).
    con.execute(f"SET temp_file_encryption = true; SET temp_directory = {sql_str(spill_directory())}")
    if os.environ.get("LOADER_DUCKDB_MEMORY_LIMIT"):  # leave the Python side room inside the task's cap
        con.execute(f"SET memory_limit = {sql_str(os.environ['LOADER_DUCKDB_MEMORY_LIMIT'])}")
    # Never time out a request the server may still complete. A catalog commit that outlives the
    # timeout is retried by DuckDB; the first one then succeeds late, the retry gets 409, and DuckDB
    # rolls back by deleting the data files the committed snapshot references: a table with missing
    # files (Polaris stalled 90 s behind purge tasks; the same failure seen once in CI). A long
    # timeout makes DuckDB wait for the answer instead. Retries stay at DuckDB's default (3). If it
    # happens anyway, the writer finds the loss and heals the table (run_txn's `heal`).
    con.execute(f"SET http_timeout = {int(os.environ.get('LOADER_HTTP_TIMEOUT', '600'))}")
    # Reuse connections: a new one per request can run the client out of ports under load.
    con.execute("SET httpfs_connection_caching = true")


def connect(s: Settings, catalog: str, principal: str = "loader", alias: str = "ice",
            multi_table_commit: bool = False) -> duckdb.DuckDBPyConnection:
    """A connection with Polaris catalog `catalog` attached as `alias`, acting as `principal`.

    Storage access uses only the credentials Polaris vends per table, so a principal can touch
    exactly the tables its grants allow. Commits go to the single-table endpoint (one
    transaction writes one table); `multi_table_commit` uses Polaris's atomic transactions/commit
    endpoint instead, which needs TABLE_CREATE on the namespace.
    """
    con = duckdb.connect()
    load_extensions(con, s)
    attach_polaris(con, s, catalog, principal, alias, multi_table_commit)
    return con


def attach_polaris(con: duckdb.DuckDBPyConnection, s: Settings, catalog: str, principal: str = "loader",
                   alias: str = "ice", multi_table_commit: bool = False) -> None:
    deny_unscoped(con, s)
    client_id, secret = s.principal(principal)
    con.execute(
        f"CREATE OR REPLACE SECRET {alias}_polaris (TYPE iceberg, CLIENT_ID {sql_str(client_id)}, "
        f"CLIENT_SECRET {sql_str(secret)}, OAUTH2_SERVER_URI {sql_str(s.token_uri)}, "
        f"OAUTH2_SCOPE 'PRINCIPAL_ROLE:ALL')"
    )
    con.execute(
        f"ATTACH {sql_str(catalog)} AS {alias} (TYPE iceberg, ENDPOINT {sql_str(s.polaris_uri)}, "
        f"SECRET {alias}_polaris, ACCESS_DELEGATION_MODE 'vended_credentials', "
        f"DISABLE_MULTI_TABLE_COMMIT {str(not multi_table_commit).lower()})"
    )


def unscoped_endpoint(s: Settings) -> tuple[str, bool]:
    """(host, use_ssl) of the fail-closed fallback: the storage service under a separate name
    locally, an unresolvable name on AWS. Either way the request never reaches public S3, and the
    name in the error marks it as lost credentials."""
    if s.s3_endpoint:
        port = s.s3_endpoint.rsplit(":", 1)[1] if s.s3_endpoint.count(":") == 2 else "9000"
        return f"{UNSCOPED_HOST}:{port}", s.s3_endpoint.startswith("https")
    return f"{UNSCOPED_HOST}.invalid", True


def deny_unscoped(con: duckdb.DuckDBPyConnection, s: Settings) -> None:
    """Fail closed. Tables are read with per-table vended secrets; any S3 request that does not
    match one (or another scoped secret) falls back to this one, with invalid keys, at an endpoint
    that is not public S3 (see unscoped_endpoint and is_lost_credentials)."""
    host, ssl = unscoped_endpoint(s)
    con.execute(
        f"CREATE OR REPLACE SECRET deny_unscoped (TYPE s3, SCOPE 's3://', KEY_ID 'deny', SECRET 'deny', "
        f"ENDPOINT {sql_str(host)}, URL_STYLE 'path', USE_SSL {str(ssl).lower()}, REGION 'us-east-1')"
    )


def is_conflict(err: BaseException) -> bool:
    return any(marker in str(err) for marker in CONFLICT_MARKERS)


def is_lost_credentials(err: BaseException) -> bool:
    """DuckDB 1.5.6 sometimes reads a table's newer metadata without its vended credentials when
    writers contend (seen with threads and with processes, on first attempts and retries). The
    request then hits the fail-closed secret. Safe to retry on a fresh connection."""
    return UNSCOPED_HOST in str(err)


def is_unreachable(err: BaseException) -> bool:
    """Storage refused the connection: the request never reached it, so retrying is safe (seen
    under load)."""
    return "Could not connect to server" in str(err)


def is_missing_file(err: BaseException) -> bool:
    """A file the table references is gone from storage (HTTP 404 on a read)."""
    text = str(err)
    return "404" in text and ("Not Found" in text or "NoSuchKey" in text)


def run_txn(connect: Callable[[], duckdb.DuckDBPyConnection], statements: list[str],
            max_attempts: int = 8, base_delay: float = 0.2, heal: Callable[[], bool] | None = None) -> int:
    """Run statements in one transaction, retrying the whole transaction on commit conflicts, lost
    vended credentials and refused storage connections.

    DuckDB does not retry a 409 itself. Every attempt uses a new connection from `connect`:
    retrying on the same connection can reuse stale table state and lose the vended credentials.
    Returns the number of attempts used; non-conflict errors are raised immediately.

    `heal` (the caller holds the table's lock) checks the table and repairs it. It returns True if
    the table is whole again as it was (the transaction is then retried), False if nothing was
    wrong, and raises if the repair had to roll the table back. DuckDB 1.5.6 deletes the files of an
    applied commit whose answer it lost (loader/integrity.py), so `heal` runs when a statement hits a
    missing file, and after a commit that succeeded only on a retry.
    """
    conflicted = healed = False
    for attempt in range(1, max_attempts + 1):
        con = connect()
        try:
            con.execute("BEGIN")
            for stmt in statements:
                con.execute(stmt)
            con.execute("COMMIT")
            if conflicted and heal:
                heal()  # a failed commit may have been applied, and its files deleted
            return attempt
        except duckdb.Error as err:
            try:
                con.execute("ROLLBACK")
            except duckdb.Error:
                pass
            if is_missing_file(err) and heal and not healed and attempt < max_attempts:
                healed = True
                if heal():
                    log.error("run_txn: the table had lost files and they were restored; retrying (attempt %d)", attempt)
                    continue
                raise
            conflicted = conflicted or is_conflict(err)
            if not (is_conflict(err) or is_lost_credentials(err) or is_unreachable(err)) or attempt == max_attempts:
                raise
            if is_lost_credentials(err):
                log.warning("run_txn: DuckDB read without vended credentials; retrying (attempt %d)", attempt)
            if is_unreachable(err):
                log.warning("run_txn: storage refused a connection; retrying (attempt %d)", attempt)
            time.sleep(base_delay * 2 ** (attempt - 1) * random.uniform(0.5, 1.5))
    raise AssertionError("unreachable")


_QUOTED = re.compile(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"")


def redact(err: BaseException) -> str:
    """Error text that is safe to log: keeps the error type, drops every quoted literal.

    DuckDB embeds offending values in messages (a failed cast quotes the value), which would put
    PHI into logs.
    """
    return f"{type(err).__name__}: {_QUOTED.sub('<redacted>', str(err))}"
