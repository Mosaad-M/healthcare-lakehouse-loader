"""Canary identifiers (kept, dropped, and in rejected rows) appear only as ciphertext outside the
raw zone: not in table data, Iceberg metadata, any control table, the shred journal, or Prefect's logs.

Runs after test_file_drops.py, which loaded and quarantined files carrying this run's canaries. Each
search has a positive control, so a scan that cannot see anything does not pass by accident.
"""
import json
import os
import urllib.request

import duckdb

from loader import duck, raw, tables
from loader.polaris import Polaris, vended_s3
from loader.state import MANIFEST


def object_text(con, s3, bucket: str, key: str) -> str:
    """Searchable text of one object: decoded rows for Parquet and Avro (both compress), bytes otherwise."""
    uri = f"s3://{bucket}/{key}"
    if key.endswith(".parquet"):
        rows = con.execute(f"SELECT CAST(t AS VARCHAR) FROM read_parquet({duck.sql_str(uri)}) t").fetchall()
        return "\n".join(r[0] for r in rows)
    if key.endswith(".avro"):
        rows = con.execute(f"SELECT CAST(t AS VARCHAR) FROM read_avro({duck.sql_str(uri)}) t").fetchall()
        return "\n".join(r[0] for r in rows)
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("latin-1")


def duckdb_reader(s) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    duck.load_extensions(con, s)
    return con


def table_objects(s, con, catalog: str, namespace: str, table: str):
    """(s3 client, bucket, keys) for one table, read with credentials Polaris vends for it to the
    read-only `auditor` principal; the bucket guardrail blocks any other path on AWS."""
    body = Polaris(s, "auditor").load_table(catalog, namespace, table, vended=True)
    creds = vended_s3(body)
    bucket, prefix = body["metadata"]["location"].removeprefix("s3://").split("/", 1)
    con.execute(s.duckdb_s3_secret(f"t_{abs(hash(prefix))}", f"s3://{bucket}/{prefix}", creds))
    s3 = s.s3(creds)
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix.rstrip("/") + "/"):
        keys += [o["Key"] for o in page.get("Contents", [])]
    return s3, bucket, keys


def found(text: str, canaries) -> list[str]:
    return [c for c in canaries.all() if c in text]


def test_canaries_are_absent_from_table_data_and_metadata(s, sources, canaries):
    con, scanned = duckdb_reader(s), 0
    for contract in sources.values():
        for layer in (tables.BRONZE, tables.SILVER):
            s3, bucket, keys = table_objects(s, con, tables.CATALOG, layer, contract.table)
            for key in keys:
                assert not found(object_text(con, s3, bucket, key), canaries), f"canary in {bucket}/{key}"
                scanned += 1
    assert scanned > 20, "too few objects scanned to mean anything"


def test_scan_finds_canaries_where_they_belong(s, sources, canaries):
    """Positive controls: the raw zone holds the canaries (plaintext by design, short expiry), and a
    compressed Parquet file holding one is detected."""
    con, zone = duckdb_reader(s), raw.RawZone(s)
    con.execute(s.duckdb_s3_secret("raw_scan", f"s3://{zone.bucket}/", "loader-raw"))
    claims = sources["acme_claims"]
    landed = zone.keys(f"landed/{claims.source}/")
    assert landed and all(found(object_text(con, zone.s3, zone.bucket, k), canaries) for k in landed)
    key = f"scan-control/{canaries.tag}.parquet"
    con.execute(f"COPY (SELECT {duck.sql_str(canaries.ssn)} AS x FROM range(1000)) TO "
                f"{duck.sql_str(zone.uri(key))} (FORMAT parquet, COMPRESSION zstd)")
    try:
        assert found(object_text(con, zone.s3, zone.bucket, key), canaries)
    finally:
        zone.delete(key)


def test_canaries_are_absent_from_every_control_table(s, sources, canaries):
    """The manifest, identity service, key vault, batch subjects, shred events and decrypt audit."""
    ddb = s.dynamodb()
    dump, mine = [], 0
    for table in ddb.list_tables()["TableNames"]:
        for page in ddb.get_paginator("scan").paginate(TableName=table):
            dump += page.get("Items", [])
            if table == MANIFEST:
                mine += sum(i["source"]["S"] in {c.source for c in sources.values()} for i in page.get("Items", []))
    assert mine >= 8  # loaded and quarantined batches of this run
    assert not found(json.dumps(dump, default=str), canaries)


def test_canaries_are_absent_from_the_shred_journal(s, canaries):
    from loader import journal

    assert not found(json.dumps(journal.entries(s)), canaries)


def test_identifiers_were_kept_encrypted_not_lost(s, sources, canaries):
    """Positive control for the scans above: the audited path returns the canary member's name."""
    from loader import decrypt
    from loader.identity import Identity

    claims = sources["acme_claims"]
    sid = Identity(s).resolve(claims.source, [(canaries.member_id,)])[(canaries.member_id,)]
    result = decrypt.reidentify(s, "control@example.org", "care_coordinator", "treatment", claims, [sid])
    assert result.rows and {r["member_name"] for r in result.rows} == {canaries.name}


def prefect_headers() -> dict:
    """The Prefect API requires basic auth."""
    import base64

    auth = base64.b64encode(os.environ["PREFECT_API_AUTH_STRING"].encode()).decode()
    return {"Content-Type": "application/json", "Authorization": f"Basic {auth}"}


def prefect_logs_since(started_at: str) -> list[dict]:
    api, logs, offset = os.environ["PREFECT_API_URL"], [], 0
    while True:
        body = json.dumps({"logs": {"timestamp": {"after_": started_at}}, "limit": 200, "offset": offset}).encode()
        req = urllib.request.Request(f"{api}/logs/filter", data=body, headers=prefect_headers())
        with urllib.request.urlopen(req) as resp:
            page = json.load(resp)
        logs += page
        if len(page) < 200:
            return logs
        offset += 200


def test_canaries_are_absent_from_prefect_logs_and_states(s, sources, canaries, started_at):
    logs = prefect_logs_since(started_at)
    # Positive control: the loader's own quarantine warnings from this run are among the logs.
    assert any(log["name"] == "loader.pipeline" and "quarantined:" in log["message"] for log in logs), \
        "loader logs did not reach Prefect"
    assert not found("\n".join(log["message"] for log in logs), canaries)

    api = os.environ["PREFECT_API_URL"]
    body = json.dumps({"flow_runs": {"start_time": {"after_": started_at}}, "limit": 200}).encode()
    req = urllib.request.Request(f"{api}/flow_runs/filter", data=body, headers=prefect_headers())
    with urllib.request.urlopen(req) as resp:
        runs = json.load(resp)
    assert runs
    assert not found(json.dumps([{"name": r["name"], "parameters": r["parameters"], "state": r["state"]}
                                 for r in runs]), canaries)
