"""One principal per pipeline: a source can write its own Bronze and Silver tables and nothing else."""
import duckdb
import pytest

from loader import duck

OWN = "ice.bronze.acme_claims__claims"


def test_source_writes_its_own_tables(s, run_id):
    con = duck.connect(s, "phi", principal="src_acme_claims")
    batch = f"probe_{run_id}"
    con.execute(f"INSERT INTO {OWN} (claim_id, _batch_id) VALUES ('probe', '{batch}')")
    assert con.execute(f"SELECT count(*) FROM {OWN} WHERE _batch_id = '{batch}'").fetchone()[0] == 1
    con.execute(f"DELETE FROM {OWN} WHERE _batch_id = '{batch}'")
    assert duck.connect(s, "phi", principal="src_acme_claims").execute(
        f"SELECT count(*) FROM {OWN} WHERE _batch_id = '{batch}'").fetchone()[0] == 0


@pytest.mark.parametrize("sql", [
    "SELECT count(*) FROM ice.bronze.lab_results__results",
    "INSERT INTO ice.bronze.lab_results__results (result_id) VALUES ('x')",
    "SELECT count(*) FROM ice.silver.provider_directory__providers",
    "CREATE TABLE ice.bronze.not_allowed (x INT)",
    "DROP TABLE ice.silver.acme_claims__claims",
])
def test_source_cannot_touch_anything_else(s, sql):
    con = duck.connect(s, "phi", principal="src_acme_claims")
    with pytest.raises(duckdb.Error, match="403|Forbidden|not authorized"):
        con.execute(sql)


def test_source_cannot_use_multi_table_commit(s, run_id):
    """Polaris's transactions/commit needs namespace-wide TABLE_CREATE, which sources do not get."""
    con = duck.connect(s, "phi", principal="src_acme_claims", multi_table_commit=True)
    with pytest.raises(duckdb.Error, match="403|Forbidden|not authorized"):
        con.execute(f"INSERT INTO {OWN} (claim_id, _batch_id) VALUES ('probe', 'mt_{run_id}')")
