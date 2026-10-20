"""Silver semantics against a reference model. The SQL that loader/silver.py generates runs in
plain DuckDB (the same statements the loader commits to Iceberg) over random sequences of batches;
after every batch Silver must equal what a few lines of Python say it should, rerunning a batch must
change nothing, and no primary key may appear twice. Small key and date spaces make collisions,
restatements and out-of-order changes common.
"""
import types
from datetime import date, timedelta

import duckdb
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from loader import silver, tables
from loader.contracts import Contract

DUCK = {"string": "VARCHAR", "long": "BIGINT", "int": "INTEGER", "date": "DATE", "timestamptz": "TIMESTAMPTZ",
        "binary": "BLOB", "double": "DOUBLE"}
DAY0 = date(2026, 1, 1)
KEYS = st.sampled_from(list("abcde"))
DAYS = st.integers(0, 9)
VALS = st.integers(0, 99)
SETTINGS = settings(max_examples=300, deadline=None, suppress_health_check=[HealthCheck.too_slow])


def contract(semantics: str) -> Contract:
    cols = [{"name": "id", "type": "string", "phi": "none"}, {"name": "day", "type": "date", "phi": "none"},
            {"name": "val", "type": "long", "phi": "none"}]
    silver_spec = {"semantics": semantics}
    if semantics == "window":
        silver_spec["window_column"] = "day"
    if semantics == "cdc":
        cols += [{"name": "op", "type": "string", "phi": "none"}, {"name": "seq", "type": "long", "phi": "none"}]
        silver_spec.update(op_column="op", order_column="seq")
    return Contract.model_validate({"source": f"prop_{semantics}", "partner": "prop_partner", "entity": "rows", "owner": "test@example.org",
                                    "format": {"type": "csv"}, "columns": cols, "primary_key": ["id"],
                                    "silver": silver_spec})


class Lake:
    """Bronze and Silver as plain DuckDB tables under the names the loader's SQL uses."""

    def __init__(self, c: Contract):
        self.c, self.con, self.batches = c, duckdb.connect(), 0
        self.con.execute("ATTACH ':memory:' AS ice; CREATE SCHEMA ice.bronze; CREATE SCHEMA ice.silver")
        for layer in (tables.BRONZE, tables.SILVER):
            cols = ", ".join(f"{name} {DUCK[typ]}" for name, typ in tables.columns(c, layer))
            self.con.execute(f"CREATE TABLE {tables.qualified(c, layer)} ({cols})")
        self.names = [name for name, _ in tables.data_columns(c)]

    def load(self, rows: list[tuple]) -> str:
        """Commit a batch to Bronze (as bronze.write would) and return its ID."""
        self.batches += 1
        bid = f"b{self.batches}"
        for i, row in enumerate(rows):
            values = dict(zip(self.names, row))
            values["day"] = DAY0 + timedelta(days=values["day"])
            self.con.execute(f"INSERT INTO {tables.qualified(self.c, tables.BRONZE)} "
                             f"({', '.join(self.names)}, _batch_id, _source_file, _row_number, _ingested_at) "
                             f"VALUES ({', '.join('?' for _ in self.names)}, ?, 'f', ?, now())",
                             [values[n] for n in self.names] + [bid, i])
        return bid

    def apply(self, bid: str) -> None:
        self.con.execute("BEGIN")
        for statement in silver.statements(self.c, types.SimpleNamespace(batch_id=bid)):
            self.con.execute(statement)
        self.con.execute("COMMIT")

    def silver(self) -> list[tuple]:
        cols = ", ".join(f"datediff('day', DATE '{DAY0}', day)" if n == "day" else n for n in self.names)
        return sorted(self.con.execute(f"SELECT {cols} FROM {tables.qualified(self.c, tables.SILVER)}").fetchall())


def check(semantics: str, batches: list[list[tuple]], reference) -> None:
    lake, state = Lake(contract(semantics)), {}
    for rows in batches:
        bid = lake.load(rows)
        lake.apply(bid)
        state = reference(state, rows)
        assert lake.silver() == sorted(state.values()), f"after {bid}"
        lake.apply(bid)  # a rerun (replay, retry) changes nothing
        assert lake.silver() == sorted(state.values()), f"rerun of {bid}"
    keys = [row[0] for row in lake.silver()]
    assert len(keys) == len(set(keys)), "a primary key appears twice in Silver"


def unique_ids(rows):
    return st.lists(rows, min_size=1, max_size=6, unique_by=lambda r: r[0])


ROWS = st.tuples(KEYS, DAYS, VALS)


@SETTINGS
@given(st.lists(unique_ids(ROWS), min_size=1, max_size=5))
def test_snapshot_replaces_silver_with_the_batch(batches):
    check("snapshot", batches, lambda state, rows: {r[0]: r for r in rows})


@SETTINGS
@given(st.lists(unique_ids(ROWS), min_size=1, max_size=5))
def test_append_keeps_the_first_version_of_each_key(batches):
    def ref(state, rows):
        return {**{r[0]: r for r in rows}, **state}  # existing keys win
    check("append", batches, ref)


@SETTINGS
@given(st.lists(unique_ids(ROWS), min_size=1, max_size=5))
def test_window_replaces_the_batch_range_and_its_keys(batches):
    def ref(state, rows):
        lo, hi, ids = min(r[1] for r in rows), max(r[1] for r in rows), {r[0] for r in rows}
        kept = {k: r for k, r in state.items() if not (lo <= r[1] <= hi) and k not in ids}
        return {**kept, **{r[0]: r for r in rows}}
    check("window", batches, ref)


CHANGES = st.tuples(KEYS, DAYS, VALS, st.sampled_from(["I", "U", "D"]), st.integers(0, 30))


@SETTINGS
@given(st.lists(st.lists(CHANGES, min_size=1, max_size=8, unique_by=lambda r: (r[0], r[4])), min_size=1, max_size=5))
def test_cdc_applies_the_newest_change_per_key(batches):
    def ref(state, rows):
        state = dict(state)
        for key in {r[0] for r in rows}:
            latest = max((r for r in rows if r[0] == key), key=lambda r: r[4])
            current = state.get(key)
            if current and current[4] >= latest[4]:
                continue  # an older or equal change: Silver already has newer
            state.pop(key, None)
            if latest[3] != "D":
                state[key] = latest
        return state
    check("cdc", batches, ref)
