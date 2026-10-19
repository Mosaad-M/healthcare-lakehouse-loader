"""Remove what interrupted test runs left behind on the local stack (never the example sources).

    make clean-test-data            # dry run: lists what would go
    make clean-test-data ARGS=--yes # removes it (run again if a large purge times out)

Test sources are named `<example>_<8 hex run id>` (tables `<source>__<entity>`) and test namespaces
`<prefix>_<8 hex>`, and so are their stream topics; the example sources have no such suffix and are never touched.
"""
import re
import sys

from loader import raw, settings
from loader.polaris import Polaris
from loader.state import MANIFEST, Manifest

RUN = r"[0-9a-f]{8}"
TEST_TABLE = re.compile(rf"^(?P<source>[a-z][a-z0-9_]*_{RUN})__[a-z0-9_]+$")
TEST_NAMESPACE = re.compile(rf"^[a-z]+(_[a-z]+)*_{RUN}$")
TEST_SOURCE = re.compile(rf"^[a-z][a-z0-9_]*_{RUN}$")


def main(apply: bool) -> None:
    s = settings.load()
    root = Polaris(s)
    sources, tables, namespaces = set(), [], []
    for catalog in ("phi", "deid"):
        status, body = root.rest("GET", f"{s.polaris_uri}/v1/{catalog}/namespaces")
        for (ns,) in (body or {}).get("namespaces", []) if status == 200 else []:
            if TEST_NAMESPACE.match(ns):
                namespaces.append((catalog, ns))
                continue
            st, tb = root.rest("GET", root.table_url(catalog, ns) + "/tables")
            for ident in (tb or {}).get("identifiers", []) if st == 200 else []:
                match = TEST_TABLE.match(ident["name"])
                if match:
                    tables.append((catalog, ns, ident["name"]))
                    sources.add(match["source"])
    from stack.stream_sink import streams

    topics = [t for t in streams.existing(s) if TEST_SOURCE.match(t)]
    print(f"{len(tables)} test tables, {len(namespaces)} test namespaces, {len(sources)} test sources, "
          f"{len(topics)} test topics")
    if not apply:
        print("dry run; pass --yes to remove them")
        return
    # Dropping with purge deletes data, which Polaris allows content managers (`loader`), not the
    # catalog admin role root holds.
    writer, failed = Polaris(s, "loader"), []

    def drop(catalog, ns, table):
        try:
            status, _ = writer.rest("DELETE", writer.table_url(catalog, ns, table) + "?purgeRequested=true")
        except TimeoutError:  # purging a large table can take a while; a rerun finishes it
            failed.append(f"{catalog}.{ns}.{table}: timed out (run again)")
            return
        if status not in (204, 404):
            failed.append(f"{catalog}.{ns}.{table}: HTTP {status}")

    for catalog, ns, table in tables:
        drop(catalog, ns, table)
    for catalog, ns in namespaces:
        st, tb = writer.rest("GET", writer.table_url(catalog, ns) + "/tables")
        for ident in (tb or {}).get("identifiers", []) if st == 200 else []:
            drop(catalog, ns, ident["name"])
        status, _ = writer.rest("DELETE", f"{s.polaris_uri}/v1/{catalog}/namespaces/{ns}")
        if status not in (204, 404):
            failed.append(f"namespace {catalog}.{ns}: HTTP {status}")
    zone, ddb = raw.RawZone(s), s.dynamodb()
    for source in sorted(sources):
        principal = f"src_{source}"
        root.mgmt("DELETE", f"/principals/{principal}")
        root.mgmt("DELETE", f"/principal-roles/{principal}_role")
        root.mgmt("DELETE", f"/catalogs/phi/catalog-roles/{principal}")
        s.delete_principal(principal)
        for batch in Manifest(s).for_source(source):
            ddb.delete_item(TableName=MANIFEST, Key={"batch_id": {"S": batch.batch_id}})
        for prefix in (f"drops/{source}/", f"landed/{source}/", f"quarantine/{source}/"):
            for key in zone.keys(prefix):
                zone.delete(key)
    if topics:
        streams.delete(s, topics)
    print(f"removed; {len(failed)} failures" + "".join(f"\n  {f}" for f in failed[:20]))
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main("--yes" in sys.argv)
