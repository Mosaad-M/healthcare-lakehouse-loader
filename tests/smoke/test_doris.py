"""Doris reads Polaris tables as `doris_reader`, with vended credentials."""
import pymysql
import pytest

from loader import doris, duck

pytestmark = [pytest.mark.doris]


def test_doris_reads_what_duckdb_wrote(s, ns, table):
    name, t = table
    duck.connect(s, "phi").execute(f"CREATE TABLE {t} AS SELECT x AS id, 'v' || x AS v FROM range(1000) r(x)")
    con = doris.connect()
    assert doris.query(con, f"SELECT count(*), sum(id) FROM lake_phi.{ns}.{name}") == [(1000, 499500)]


def test_doris_reader_cannot_write(s, ns, table):
    name, t = table
    duck.connect(s, "phi").execute(f"CREATE TABLE {t} AS SELECT 1 AS id")
    with pytest.raises(pymysql.MySQLError):
        doris.query(doris.connect(), f"INSERT INTO lake_phi.{ns}.{name} VALUES (2)")
    assert duck.connect(s, "phi").execute(f"SELECT count(*) FROM {t}").fetchone()[0] == 1
