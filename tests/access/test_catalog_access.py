"""Catalog RBAC and vended-credential scoping (G5 C2 and C3), on RustFS.

RustFS issues STS credentials with Polaris's per-table session policy and enforces it, so the
storage-level checks run locally as well as on AWS.
"""

import boto3
import botocore.config
import botocore.exceptions
import pytest

from loader import duck
from loader.polaris import Polaris, expect, vended_s3
from tests.conftest import purge_namespace

SCHEMA = {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": False, "type": "int"}]}


def s3_client(s, creds: dict | None):
    if creds is None:  # anonymous
        return boto3.client("s3", endpoint_url=s.s3_endpoint or None, region_name=s.region,
                            config=botocore.config.Config(signature_version=botocore.UNSIGNED))
    return s.s3(creds)


def allowed(fn) -> bool:
    try:
        fn()
        return True
    except botocore.exceptions.ClientError as err:
        if err.response["Error"]["Code"] in ("AccessDenied", "403", "Forbidden"):
            return False
        raise


@pytest.fixture(scope="module")
def tables(s, run_id):
    """Namespace with tables a and b, each holding one object under its location."""
    writer = Polaris(s, "loader")
    ns = f"access_{run_id}"
    expect(writer.rest("POST", f"{s.polaris_uri}/v1/phi/namespaces", {"namespace": [ns]}), 200)
    out = {"ns": ns}
    for name in ("a", "b"):
        body = expect(writer.rest("POST", writer.table_url("phi", ns) + "/tables",
                                  {"name": name, "schema": SCHEMA}, vended=True), 200)
        key = body["metadata"]["location"].removeprefix(f"s3://{s.bucket('lake-phi')}/") + "/data/probe.parquet"
        s3_client(s, vended_s3(body)).put_object(Bucket=s.bucket("lake-phi"), Key=key, Body=b"probe")
        out[name] = key
    yield out
    purge_namespace(writer, "phi", ns)


@pytest.fixture(scope="module")
def analyst(s, run_id):
    """A fresh principal with an empty principal role (no catalog grants yet)."""
    root = Polaris(s)
    name = f"analyst_{run_id}"
    creds = expect(root.mgmt("POST", "/principals", {"principal": {"name": name}}), 201)["credentials"]
    expect(root.mgmt("POST", "/principal-roles", {"principalRole": {"name": f"{name}_role"}}), 201)
    expect(root.mgmt("PUT", f"/principals/{name}/principal-roles", {"principalRole": {"name": f"{name}_role"}}), 201, 200)
    yield name, Polaris(s, name, credentials=(creds["clientId"], creds["clientSecret"]))
    root.mgmt("DELETE", f"/principals/{name}")
    root.mgmt("DELETE", f"/principal-roles/{name}_role")
    root.mgmt("DELETE", f"/catalogs/phi/catalog-roles/{name}_read_a")


def test_c2_writer_credentials_are_scoped_to_one_table(s, tables):
    body = Polaris(s, "loader").load_table("phi", tables["ns"], "a", vended=True)
    s3 = s3_client(s, vended_s3(body))
    assert allowed(lambda: s3.get_object(Bucket=s.bucket("lake-phi"), Key=tables["a"]))
    assert allowed(lambda: s3.put_object(Bucket=s.bucket("lake-phi"), Key=tables["a"] + ".w", Body=b"w"))
    assert not allowed(lambda: s3.get_object(Bucket=s.bucket("lake-phi"), Key=tables["b"]))
    assert not allowed(lambda: s3.put_object(Bucket=s.bucket("lake-phi"), Key=tables["b"] + ".w", Body=b"w"))
    assert not allowed(lambda: s3.list_objects_v2(Bucket=s.bucket("lake-phi")))
    assert not allowed(lambda: s3.put_object(Bucket=s.bucket("lake-deid"), Key="x", Body=b"w"))
    assert not allowed(lambda: s3.put_object(Bucket=s.bucket("raw"), Key="x", Body=b"w"))


def test_c3_ungranted_principal_is_refused(tables, analyst):
    _, cat = analyst
    status, _ = cat.rest("GET", cat.table_url("phi", tables["ns"], "a"), vended=True)
    assert status in (401, 403, 404)


def test_c3_read_grant_on_one_table(s, tables, analyst):
    name, cat = analyst
    root = Polaris(s)
    role = f"{name}_read_a"
    expect(root.mgmt("POST", "/catalogs/phi/catalog-roles", {"catalogRole": {"name": role}}), 201)
    expect(root.mgmt("PUT", f"/catalogs/phi/catalog-roles/{role}/grants", {"grant": {
        "type": "table", "namespace": [tables["ns"]], "tableName": "a", "privilege": "TABLE_READ_DATA"}}), 201, 200)
    expect(root.mgmt("PUT", f"/principal-roles/{name}_role/catalog-roles/phi", {"catalogRole": {"name": role}}), 201, 200)
    cat._token = None  # pick up the new grant

    status, body = cat.rest("GET", cat.table_url("phi", tables["ns"], "a"), vended=True)
    assert status == 200
    status, _ = cat.rest("GET", cat.table_url("phi", tables["ns"], "b"), vended=True)
    assert status in (403, 404)
    status, _ = cat.rest("POST", cat.table_url("phi", tables["ns"]) + "/tables", {"name": "c", "schema": SCHEMA})
    assert status in (403, 404)
    status, _ = cat.rest("POST", cat.table_url("phi", tables["ns"], "a"), {"requirements": [], "updates": [
        {"action": "set-properties", "updates": {"reader-wrote": "yes"}}]})
    assert status in (403, 404)

    s3 = s3_client(s, vended_s3(body))
    assert allowed(lambda: s3.get_object(Bucket=s.bucket("lake-phi"), Key=tables["a"]))
    assert not allowed(lambda: s3.put_object(Bucket=s.bucket("lake-phi"), Key=tables["a"] + ".r", Body=b"r"))
    assert not allowed(lambda: s3.get_object(Bucket=s.bucket("lake-phi"), Key=tables["b"]))


def test_doris_reader_reads_but_cannot_write(s, ns, table):
    _, t = table
    duck.connect(s, "phi").execute(f"CREATE TABLE {t} AS SELECT 1 AS x")
    reader = duck.connect(s, "phi", principal="doris_reader")
    assert reader.execute(f"SELECT x FROM {t}").fetchall() == [(1,)]
    with pytest.raises(Exception):
        reader.execute(f"INSERT INTO {t} VALUES (2)")


def test_storage_refuses_anonymous_access(s, tables):
    s3 = s3_client(s, None)
    for bucket in (s.bucket(b) for b in ("raw", "lake-phi", "lake-deid")):
        assert not allowed(lambda: s3.list_objects_v2(Bucket=bucket))
    assert not allowed(lambda: s3.get_object(Bucket=s.bucket("lake-phi"), Key=tables["a"]))
