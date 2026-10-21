"""Write semantics the design depends on, against Polaris."""
import re
import threading

import duckdb
import pytest

from loader import duck


def test_single_table_txn_is_atomic(s, con, table):
    """Replacing a batch (DELETE + INSERT) commits together or not at all."""
    _, t = table
    con.execute(f"CREATE TABLE {t} (_batch_id VARCHAR, id INT)")
    con.execute(f"INSERT INTO {t} VALUES ('b1', 1), ('b1', 2)")
    with pytest.raises(duckdb.Error):
        duck.run_txn(lambda: duck.connect(s, "phi"), [
            f"DELETE FROM {t} WHERE _batch_id = 'b1'",
            f"INSERT INTO {t} VALUES ('b1', 'not-an-int')",
        ])
    assert con.execute(f"SELECT count(*) FROM {t} WHERE _batch_id = 'b1'").fetchone()[0] == 2


def test_multi_table_txn_is_atomic(s, table):
    """Polaris commits a two-table transaction atomically: when one table's commit fails, neither
    lands. The failure is a dropped table, which no client can rebase onto (DuckDB 2.0 rebases a
    commit onto a racing append, so an append no longer makes it fail)."""
    _, a = table
    b = f"{a}_b"
    setup = duck.connect(s, "phi")
    setup.execute(f"CREATE TABLE {a} (id INT)")
    setup.execute(f"CREATE TABLE {b} (id INT)")
    writer = duck.connect(s, "phi", multi_table_commit=True)
    writer.execute("BEGIN")
    writer.execute(f"INSERT INTO {a} VALUES (1)")
    writer.execute(f"INSERT INTO {b} VALUES (1)")
    duck.connect(s, "phi").execute(f"DROP TABLE {b}")
    with pytest.raises(duckdb.Error):
        writer.execute("COMMIT")
    assert duck.connect(s, "phi").execute(f"SELECT count(*) FROM {a}").fetchone()[0] == 0, "partial commit"


def test_concurrent_appenders_with_retry(s, table):
    """Retrying whole transactions on 409 loses and duplicates nothing."""
    _, t = table
    duck.connect(s, "phi").execute(f"CREATE TABLE {t} (_batch_id VARCHAR, n INT)")
    writers, rows_per_batch = 16, 50  # 16 writers: enough retries to expose stale-retry bugs
    errors: list[str] = []

    def write(i: int) -> None:
        try:
            duck.run_txn(lambda: duck.connect(s, "phi"),
                         [f"INSERT INTO {t} SELECT 'b{i}', x FROM range({rows_per_batch}) r(x)"],
                         max_attempts=12)
        except duckdb.Error as err:
            errors.append(duck.redact(err))

    threads = [threading.Thread(target=write, args=(i,)) for i in range(writers)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    assert not errors, errors
    counts = dict(duck.connect(s, "phi").execute(f"SELECT _batch_id, count(*) FROM {t} GROUP BY 1").fetchall())
    assert counts == {f"b{i}": rows_per_batch for i in range(writers)}


@pytest.mark.parametrize("trial", range(3))
def test_racing_commits_never_lose_data(s, table, trial):
    """Six commits released together, no retries: every commit Polaris accepts is in the table."""
    _, t = table
    duck.connect(s, "phi").execute(f"CREATE TABLE {t} (writer INT)")
    duck.connect(s, "phi").execute(f"INSERT INTO {t} VALUES (-1)")  # a base snapshot every writer sees
    writers = 6
    cons = [duck.connect(s, "phi") for _ in range(writers)]
    for i, c in enumerate(cons):
        c.execute("BEGIN")
        c.execute(f"INSERT INTO {t} VALUES ({i})")
    barrier, outcome = threading.Barrier(writers), {}

    def commit(i: int) -> None:
        barrier.wait()
        try:
            cons[i].execute("COMMIT")
            outcome[i] = "ok"
        except duckdb.Error as err:
            outcome[i] = "conflict" if duck.is_conflict(err) else duck.redact(err)

    threads = [threading.Thread(target=commit, args=(i,)) for i in range(writers)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    landed = {r[0] for r in duck.connect(s, "phi").execute(f"SELECT writer FROM {t} WHERE writer >= 0").fetchall()}
    accepted = {i for i, o in outcome.items() if o == "ok"}
    assert accepted, f"no commit succeeded: {outcome}"
    assert landed == accepted, f"accepted commits missing from the table: {sorted(accepted - landed)}"


def test_duckdb_writes_to_a_sorted_table_only_from_2_0(s, con, polaris, ns, table):
    """Tables never declare a sort order because DuckDB 1.x cannot write to them. DuckDB 2.0 can,
    so the rule (and sort-order compaction) can be revisited."""
    name, t = table
    con.execute(f"CREATE TABLE {t} (id INT, ts TIMESTAMP)")
    meta = polaris.load_table("phi", ns, name)["metadata"]
    ts_id = next(f["id"] for f in meta["schemas"][-1]["fields"] if f["name"] == "ts")
    polaris.update_table("phi", ns, name, [
        {"action": "add-sort-order", "sort-order": {"order-id": 1, "fields": [
            {"transform": "identity", "source-id": ts_id, "direction": "desc", "null-order": "nulls-last"}]}},
        {"action": "set-default-sort-order", "sort-order-id": -1},
    ])
    insert = f"INSERT INTO {t} VALUES (1, now()::TIMESTAMP)"
    if duckdb.__version__.startswith("1."):
        with pytest.raises(duckdb.Error, match="sorted iceberg table"):
            duck.connect(s, "phi").execute(insert)
    else:
        duck.connect(s, "phi").execute(insert)
        assert duck.connect(s, "phi").execute(f"SELECT count(*) FROM {t}").fetchone()[0] == 1


def _files_read(con, sql: str) -> int:
    plan = con.execute("EXPLAIN ANALYZE " + sql).fetchall()[0][1]
    return sum(int(n) for n in re.findall(r"Total Files Read:\s*(\d+)", plan))


def test_merge_prunes_only_with_bucket_predicate(con, table):
    """An explicit pk_bucket predicate limits the target scan of a Silver MERGE."""
    _, t = table
    con.execute(f"CREATE TABLE {t} (id VARCHAR, pk_bucket INT, v VARCHAR) PARTITIONED BY (pk_bucket)")
    con.execute(f"INSERT INTO {t} SELECT uuid()::VARCHAR, NULL, 'x' FROM range(200000)")
    con.execute(f"UPDATE {t} SET pk_bucket = (md5_number(id) % 16)::INT")
    probe = con.execute(f"SELECT id, pk_bucket FROM {t} LIMIT 1").fetchone()
    without = _files_read(con, f"SELECT count(*) FROM {t} t JOIN (SELECT '{probe[0]}' id) s ON t.id = s.id")
    with_pred = _files_read(con, f"SELECT count(*) FROM {t} t JOIN (SELECT '{probe[0]}' id) s "
                                 f"ON t.pk_bucket = {probe[1]} AND t.id = s.id")
    assert with_pred < without


def test_lake_requests_never_leave_the_stack(s, table):
    """Fail closed: an S3 request without a vended secret is refused by local storage, not sent to
    public AWS (DuckDB 1.5.6 sent unsigned requests to s3.amazonaws.com after a stale retry)."""
    _, t = table
    con = duck.connect(s, "phi")
    con.execute(f"CREATE TABLE {t} AS SELECT 1 AS x")
    con.execute("CALL enable_logging('HTTP')")
    with pytest.raises(duckdb.Error) as exc:  # refused locally (403), unresolvable on AWS
        con.execute(f"SELECT * FROM read_text('s3://{s.bucket('lake-phi')}/not-a-table/x.json')")
    assert duck.is_lost_credentials(exc.value), "the request did not use the fail-closed secret"
    hosts = [r[0] for r in con.execute(
        "SELECT message FROM duckdb_logs WHERE message ILIKE '%amazonaws%'").fetchall()]
    assert not hosts, "a lake request went to public AWS"
