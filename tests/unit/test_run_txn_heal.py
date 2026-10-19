"""duck.run_txn's `heal` hook: when it runs, and what the transaction does afterwards."""
import duckdb
import pytest

from loader import duck

MISSING = duckdb.HTTPException("HTTP GET error on 's3://lake/t/data/f.parquet' (HTTP 404 Not Found)")
CONFLICT = duckdb.TransactionException("Conflict_409: CommitFailedException: requirement failed")


class Con:
    def __init__(self, fail_with=None):
        self.fail_with = fail_with

    def execute(self, sql):
        if self.fail_with and sql.startswith("INSERT"):
            raise self.fail_with
        return self


def run(failures, heal):
    """run_txn over connections that fail in turn with `failures` (None: succeeds)."""
    cons = iter(Con(f) for f in failures)
    return duck.run_txn(lambda: next(cons), ["INSERT INTO t VALUES (1)"], base_delay=0, heal=heal)


def test_a_missing_file_heals_then_retries():
    calls = []
    assert run([MISSING, None], lambda: calls.append(1) or True) == 2
    assert calls == [1]


def test_a_missing_file_on_a_healthy_table_is_raised():
    with pytest.raises(duckdb.HTTPException):
        run([MISSING, None], lambda: False)


def test_heal_runs_once_per_transaction():
    with pytest.raises(duckdb.HTTPException):
        run([MISSING, MISSING, None], lambda: True)


def test_a_commit_that_succeeds_after_a_conflict_is_checked():
    calls = []
    assert run([CONFLICT, None], lambda: calls.append(1) or False) == 2
    assert calls == [1]


def test_a_first_time_success_is_not_checked():
    run([None], lambda: pytest.fail("healed a clean commit"))


def test_a_rollback_stops_the_transaction():
    class Rolled(Exception):
        pass

    def heal():
        raise Rolled

    with pytest.raises(Rolled):
        run([MISSING, None], heal)


def test_without_heal_a_missing_file_is_raised():
    cons = iter([Con(MISSING), Con()])
    with pytest.raises(duckdb.HTTPException):
        duck.run_txn(lambda: next(cons), ["INSERT INTO t VALUES (1)"], base_delay=0)
