"""Gold deploy evolves a table's schema by column id (loader/deploy.py ensure_table_columns). Iceberg
reads data files by column id, so a dropped column's id must never be given to a later column: the
dropped column's old values would read as the new column's, in current data files and in every
snapshot that still holds them. No stack: a stand-in catalog applies the updates as Iceberg does.
"""
from loader import deploy


class Catalog:
    """One table's metadata, updated as the REST catalog would for the actions deploy sends."""

    def __init__(self):
        self.meta = None

    def table_url(self, catalog, namespace, table=None):
        return f"/{catalog}/{namespace}/{table or ''}"

    def rest(self, method, url, body=None):
        if method == "GET":
            return (404, None) if self.meta is None else (200, {"metadata": self.meta})
        assert method == "POST"
        schema = body["schema"]
        self.meta = {"schemas": [schema], "current-schema-id": schema["schema-id"],
                     "last-column-id": max(f["id"] for f in schema["fields"]), "properties": body["properties"]}
        return 200, {}

    def update_table(self, catalog, namespace, table, updates):
        added = None
        for u in updates:
            if u["action"] == "add-schema":
                assert u["last-column-id"] >= self.meta["last-column-id"], "last-column-id went down"
                self.meta["last-column-id"] = u["last-column-id"]
                fields = u["schema"]["fields"]
                same = [s for s in self.meta["schemas"] if s["fields"] == fields]
                if same:  # as Iceberg does: an existing schema is not added twice
                    continue
                self.meta["schemas"].append(u["schema"])
                added = u["schema"]["schema-id"]
            elif u["action"] == "set-current-schema":
                if u["schema-id"] == -1:
                    # Iceberg: "Cannot set last added schema: no schema has been added"
                    assert added is not None, "set-current-schema -1 without a schema added"
                    self.meta["current-schema-id"] = added
                else:
                    assert any(s["schema-id"] == u["schema-id"] for s in self.meta["schemas"])
                    self.meta["current-schema-id"] = u["schema-id"]

    def columns(self) -> dict[str, int]:
        current = next(s for s in self.meta["schemas"] if s["schema-id"] == self.meta["current-schema-id"])
        return {f["name"]: f["id"] for f in current["fields"]}


def deploy_cols(cat, cols):
    deploy.ensure_table_columns(cat, "deid", "gold", "t", [(c, "string") for c in cols], {}, partition=None,
                                drop_missing=True)


def test_a_column_added_after_a_drop_gets_a_new_id():
    cat = Catalog()
    deploy_cols(cat, ["a", "b", "c"])
    assert cat.columns() == {"a": 1, "b": 2, "c": 3}
    deploy_cols(cat, ["a", "b"])         # c (id 3, the highest) dropped
    deploy_cols(cat, ["a", "b", "d"])    # a later deploy adds d
    assert cat.columns() == {"a": 1, "b": 2, "d": 4}


def test_dropping_and_adding_in_one_deploy_keeps_ids_apart():
    cat = Catalog()
    deploy_cols(cat, ["a", "b", "c"])
    deploy_cols(cat, ["a", "d"])
    assert cat.columns() == {"a": 1, "d": 4}


def test_re_adding_a_dropped_name_gives_it_a_new_id():
    """A column re-added under a dropped column's name is a new column: its old values stay unread."""
    cat = Catalog()
    deploy_cols(cat, ["a", "b"])
    deploy_cols(cat, ["a"])
    deploy_cols(cat, ["a", "b"])
    assert cat.columns() == {"a": 1, "b": 3}


def test_dropping_a_column_added_since_returns_to_the_earlier_schema():
    """Add a column, then remove it: the wanted schema is one the table already has. The catalog does
    not add it again, so deploy must make that schema current (found on a stack that had run
    lab_monthly with sum_value, then without its min and max, then with them back)."""
    cat = Catalog()
    deploy_cols(cat, ["a", "b"])
    deploy_cols(cat, ["a", "b", "c"])
    deploy_cols(cat, ["a", "b"])
    assert cat.columns() == {"a": 1, "b": 2}
    assert cat.meta["current-schema-id"] == 0
