"""Reproduce DuckDB 1.5.6 reading Iceberg metadata without vended credentials under contention.

    make test ARGS="--co -q"   # once, so the toolbox image exists; then:
    docker compose -f stack/compose.yaml --env-file .env run --rm --no-deps toolbox \
        python scripts/repro_lost_credentials.py processes 20 [--no-retry]

16 writers (separate processes or threads) append to one table at once. With --no-retry, a
writer fails the first time DuckDB reads without the table's vended credentials; such requests hit
the fail-closed secret (host s3-unscoped) and are refused. Without the flag, run_txn retries them.
Throttling the container (docker update --cpus 1) makes it frequent: about 1 in 5 trials.
"""
import multiprocessing as mp
import queue
import sys
import threading
import uuid

import duckdb

from loader import duck, settings

WRITERS = 16


def write(table: str, i: int, out, retry: bool) -> None:
    s = settings.load()
    sql = [f"INSERT INTO {table} SELECT 'b{i}', x FROM range(50) r(x)"]
    try:
        if retry:
            duck.run_txn(lambda: duck.connect(s, "phi"), sql, max_attempts=12)
        else:
            con = duck.connect(s, "phi")
            for attempt in range(12):  # conflicts only; lost credentials are reported
                try:
                    con = duck.connect(s, "phi")
                    con.execute(sql[0])
                    break
                except duckdb.Error as err:
                    if not duck.is_conflict(err):
                        raise
        out.put(None)
    except duckdb.Error as err:
        out.put("lost-credentials" if duck.is_lost_credentials(err) else duck.redact(err))


def main() -> None:
    mode, trials = sys.argv[1], int(sys.argv[2])
    retry = "--no-retry" not in sys.argv
    s = settings.load()
    ns = "repro_" + uuid.uuid4().hex[:6]
    duck.connect(s, "phi").execute(f"CREATE SCHEMA ice.{ns}")
    failed = 0
    for trial in range(trials):
        table = f"ice.{ns}.t{trial}"
        duck.connect(s, "phi").execute(f"CREATE TABLE {table} (_batch_id VARCHAR, n INT)")
        if mode == "threads":
            out = queue.Queue()
            workers = [threading.Thread(target=write, args=(table, i, out, retry)) for i in range(WRITERS)]
        else:
            ctx = mp.get_context("spawn")
            out = ctx.Queue()
            workers = [ctx.Process(target=write, args=(table, i, out, retry)) for i in range(WRITERS)]
        for w in workers:
            w.start()
        for w in workers:
            w.join()
        errors = [e for e in (out.get() for _ in range(WRITERS)) if e]
        rows = duck.connect(s, "phi").execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        if errors or rows != WRITERS * 50:
            failed += 1
            print(f"trial {trial}: errors={errors[:2]} rows={rows}")
    print(f"{mode}, retry={retry}: {failed}/{trials} trials with errors or missing rows")


if __name__ == "__main__":
    main()
