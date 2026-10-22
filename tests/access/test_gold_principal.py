"""The gold principal reads only Gold's dbt sources, table by table (loader/gold.py ensure_access):
not the rest of the PHI catalog, where every partner's Bronze and Silver live."""
from loader import gold
from loader.polaris import Polaris


def test_gold_reads_its_sources_and_no_other_table(s):
    root, reader = Polaris(s), Polaris(s, gold.PRINCIPAL)
    sources = {(namespace, table) for catalog, namespace, table in gold.source_tables() if catalog == "phi"}
    for namespace, table in sources:
        assert reader.rest("GET", reader.table_url("phi", namespace, table), vended=True)[0] == 200, table
    others = [(namespace, ident["name"]) for namespace in ("bronze", "silver")
              for ident in root.rest("GET", root.table_url("phi", namespace) + "/tables")[1]["identifiers"]
              if (namespace, ident["name"]) not in sources]
    assert others, "no table outside Gold's sources to check against"
    for namespace, table in others:
        status, _ = reader.rest("GET", reader.table_url("phi", namespace, table), vended=True)
        assert status in (403, 404), f"gold can load {namespace}.{table}"


def test_gold_no_longer_holds_the_whole_catalog_reader_role(s):
    roles = Polaris(s).mgmt("GET", f"/principal-roles/{gold.PRINCIPAL}_role/catalog-roles/phi")[1]["roles"]
    assert "reader" not in {r["name"] for r in roles}
