"""DuckDB 2.1's stricter rules, applied on 2.0 already (loader/duck.py strict_settings): the loader's
connections and dbt's both reject an unquoted name passed to a table function as a string."""
from types import SimpleNamespace

import duckdb
import pytest

from loader import duck, gold


def test_an_unquoted_table_function_argument_is_rejected():
    con = duckdb.connect(config=duck.strict_settings())
    with pytest.raises(duckdb.BinderException, match="implicit conversion"):
        con.execute("SELECT * FROM read_csv(people.csv)")


def test_dbt_connects_with_the_same_rules():
    options = gold.extension_options(SimpleNamespace(duckdb_extension_dir=None))
    assert duck.strict_settings().items() <= options.items()
