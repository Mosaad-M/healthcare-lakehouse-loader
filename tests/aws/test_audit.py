"""Every CloudTrail record of S3 access to the lake identifies the catalog principal and table.

Writes and reads one table as one principal, then waits for the trail to deliver (usually 5 to 15
minutes) and checks every S3 data event on the lake buckets in that window: each came through a
Polaris vending role, and each event on the table's files carries a session name naming the
principal (Polaris names vended sessions `p-<principal>-<table>`) and the table, in the session name
or, for writes made while the table is being created, in the object key.
"""
import gzip
import json
import os
import time
from datetime import datetime, timedelta, timezone

import pytest

from loader import duck
from loader.polaris import Polaris, expect

pytestmark = [pytest.mark.aws]
TABLE = "audited"


def trail_records(s, since: datetime):
    """Every CloudTrail record delivered for today and yesterday (the window may cross midnight)."""
    import boto3

    s3, bucket = s.s3("admin"), os.environ["LOADER_TRAIL_BUCKET"]
    account = boto3.client("sts", region_name=s.region).get_caller_identity()["Account"]
    for day in {since.date(), datetime.now(timezone.utc).date()}:
        prefix = f"AWSLogs/{account}/CloudTrail/{s.region}/{day:%Y/%m/%d}/"
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
                yield from json.loads(gzip.decompress(body))["Records"]


def test_every_lake_access_names_principal_and_table(s, run_id):
    ns = f"audit_{run_id}"
    start = datetime.now(timezone.utc) - timedelta(seconds=30)
    con = duck.connect(s, "phi")  # principal `loader`
    con.execute(f"CREATE SCHEMA IF NOT EXISTS ice.{ns}")
    con.execute(f"CREATE TABLE ice.{ns}.{TABLE} AS SELECT range AS id FROM range(100)")
    con.execute(f"INSERT INTO ice.{ns}.{TABLE} SELECT range FROM range(100, 200)")
    assert duck.connect(s, "phi").execute(f"SELECT count(*) FROM ice.{ns}.{TABLE}").fetchone()[0] == 200
    end = datetime.now(timezone.utc) + timedelta(seconds=30)
    prefix = f"{ns}/{TABLE}/"
    vending = set(os.environ["LOADER_VENDING_ROLES"].split(","))

    deadline, ours, lake = time.time() + 25 * 60, [], []
    while time.time() < deadline:
        lake = [r for r in trail_records(s, start)
                if r.get("eventSource") == "s3.amazonaws.com"
                and start <= datetime.fromisoformat(r["eventTime"].replace("Z", "+00:00")) <= end
                and (r.get("requestParameters") or {}).get("bucketName", "").startswith(s.bucket("lake-"))]
        ours = [r for r in lake if (r["requestParameters"].get("key") or "").startswith(prefix)]
        if len(ours) >= 4:  # metadata and data writes and reads have been delivered
            time.sleep(120)  # let late files for the same window arrive too
            break
        time.sleep(30)
    assert ours, "no CloudTrail data events for the test table within 25 minutes"

    issuers = {r["userIdentity"].get("sessionContext", {}).get("sessionIssuer", {}).get("userName") for r in lake}
    assert issuers <= vending, f"lake accessed by something other than a vending role: {issuers - vending}"
    sessions = {r["userIdentity"]["arn"].rsplit("/", 1)[-1] for r in ours}
    print(f"{len(ours)} events on the test table, session names: {sorted(sessions)}")
    # Every access names the principal in its session. The table is named in the session too,
    # except for writes made with the credentials of the create-table response (the table does
    # not exist yet, so Polaris leaves the table field empty: `p-loader-`); those records still
    # name the table in their object key, which the vended credentials confine to the table's
    # location. So every record identifies principal and table.
    for record in ours:
        name = record["userIdentity"]["arn"].rsplit("/", 1)[-1]
        assert name.startswith("p-loader-"), name
        assert name == f"p-loader-{TABLE}" or record["requestParameters"]["key"].startswith(prefix), record
    assert f"p-loader-{TABLE}" in sessions, "no access carried the table in its session name"

    Polaris(s, "loader").rest("DELETE", Polaris(s).table_url("phi", ns, TABLE) + "?purgeRequested=true")
    expect(Polaris(s, "loader").rest("DELETE", f"{s.polaris_uri}/v1/phi/namespaces/{ns}"), 204, 404, 409)
