"""Benchmark sources: copies of the claims contract under bench_<name>, deployed and removed whole
(tables, principal, manifest, raw files, subjects, identity map entries, people and keys)."""
from pathlib import Path

import yaml

from loader import contracts, deploy, raw, tables
from loader.identity import IDENTITY_MAP, PERSONS, SUBJECTS
from loader.polaris import Polaris
from loader.settings import Settings
from loader.state import MANIFEST, Manifest
from loader.vault import KEY_VAULT

EXAMPLE = "acme_claims"
# Scenarios that need another Silver semantics than the claims contract's window.
VARIANTS = {"silver_append": {"silver": {"semantics": "append"}},
            "silver_append_random": {"silver": {"semantics": "append"}}}


def contract(name: str, **changes) -> contracts.Contract:
    data = yaml.safe_load((Path("contracts") / f"{EXAMPLE}.yaml").read_text())
    data.update(source=f"bench_{name}", **VARIANTS.get(name, {}), **changes)
    return contracts.Contract.model_validate(data)


def deploy_source(s: Settings, c: contracts.Contract) -> None:
    deploy.deploy_contract(Polaris(s), s, c)


def _scan(ddb, table: str, source: str) -> list[dict]:
    items, kwargs = [], {"TableName": table, "FilterExpression": "#s = :s",
                         "ExpressionAttributeNames": {"#s": "source"}, "ExpressionAttributeValues": {":s": {"S": source}}}
    while True:
        page = ddb.scan(**kwargs)
        items += page["Items"]
        if "LastEvaluatedKey" not in page:
            return items
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def _delete(ddb, table: str, key: str, values: list[str]) -> None:
    for i in range(0, len(values), 25):
        request = {table: [{"DeleteRequest": {"Key": {key: {"S": v}}}} for v in values[i:i + 25]]}
        while request:
            request = ddb.batch_write_item(RequestItems=request).get("UnprocessedItems") or None


def remove_source(s: Settings, c: contracts.Contract) -> None:
    writer, root = Polaris(s, "loader"), Polaris(s)
    for layer in (tables.BRONZE, tables.SILVER):
        writer.rest("DELETE", writer.table_url(tables.CATALOG, layer, c.table) + "?purgeRequested=true")
    root.mgmt("DELETE", f"/principals/{c.principal}")
    root.mgmt("DELETE", f"/principal-roles/{c.principal}_role")
    root.mgmt("DELETE", f"/catalogs/{tables.CATALOG}/catalog-roles/{c.principal}")
    s.delete_principal(c.principal)
    ddb = s.dynamodb()
    _delete(ddb, MANIFEST, "batch_id", [b.batch_id for b in Manifest(s).for_source(c.source)])
    subjects = _scan(ddb, SUBJECTS, c.source)
    _delete(ddb, KEY_VAULT, "subject_id", [i["subject_id"]["S"] for i in subjects])
    _delete(ddb, PERSONS, "person_id", [i["person_id"]["S"] for i in subjects])
    _delete(ddb, IDENTITY_MAP, "blind_index", [i["blind_index"]["S"] for i in subjects])
    _delete(ddb, SUBJECTS, "subject_id", [i["subject_id"]["S"] for i in subjects])
    zone = raw.RawZone(s)
    for prefix in (f"drops/{c.source}/", f"landed/{c.source}/", f"quarantine/{c.source}/"):
        for key in zone.keys(prefix):
            zone.delete(key)
