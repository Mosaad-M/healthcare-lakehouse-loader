"""Run inside the compose network: python -m stack.bootstrap

Safe to rerun. Principal secrets are returned by Polaris only when created or reset, so they are
kept per principal (the state volume locally, Secrets Manager on AWS); a principal whose secret
was lost is reset.
"""
import json
import os
import time
from pathlib import Path

from loader import admin, deploy
from loader import contracts as contracts_mod
from loader import settings as settings_mod
from loader.polaris import Polaris, expect

MODEL = json.loads((Path(__file__).parent / "model.json").read_text())


def log(msg: str) -> None:
    print(f"bootstrap: {msg}", flush=True)


def ensure_buckets(s) -> None:
    """Local only: on AWS, Terraform owns buckets, keys, policies and lifecycles."""
    s3 = s3_admin(s)
    existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    for bucket in (s.bucket(b) for b in MODEL["buckets"]):
        if bucket not in existing:
            s3.create_bucket(Bucket=bucket)
            log(f"bucket {bucket} created")


def s3_admin(s):
    return s.s3("admin")


def ensure_raw_expiry(s) -> None:
    """The raw zone holds plaintext and cannot be shredded per subject, so it expires."""
    days = MODEL["raw_expiry_days"]
    s3_admin(s).put_bucket_lifecycle_configuration(Bucket=s.bucket("raw"), LifecycleConfiguration={"Rules": [
        {"ID": f"expire-{prefix.rstrip('/')}", "Status": "Enabled", "Filter": {"Prefix": prefix},
         "Expiration": {"Days": days}} for prefix in ("drops/", "landed/", "quarantine/", "inbox/")]})
    s3_admin(s).put_bucket_lifecycle_configuration(Bucket=s.bucket("cdc-staging"), LifecycleConfiguration={"Rules": [
        {"ID": "expire-all", "Status": "Enabled", "Filter": {"Prefix": ""}, "Expiration": {"Days": days}}]})


def ensure_lake_versioning(s) -> None:
    """The lake buckets keep a deleted object's previous version for a few days, so an integrity
    repair can restore a file a writer lost (loader/integrity.py). infra/sandbox/s3.tf on AWS."""
    lakes = [s.bucket(b) for b in ("lake-phi", "lake-deid")]
    for bucket in lakes:
        s3_admin(s).put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
        s3_admin(s).put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration={"Rules": [
            {"ID": "expire-earlier-versions", "Status": "Enabled", "Filter": {"Prefix": ""},
             "NoncurrentVersionExpiration": {"NoncurrentDays": MODEL["lake_version_days"]},
             "Expiration": {"ExpiredObjectDeleteMarker": True}}]})
    admin.ensure_storage_user(s, "lake-restore", lakes, statements=admin.restore_statements(lakes))


def ensure_stream_sink(s) -> None:
    """The streaming sink (Redpanda Connect) may only put files into drop prefixes; it can read and
    delete nothing. Its settings go to an env file in the state volume (Firehose's role on AWS)."""
    admin.ensure_storage_user(s, "stream-sink", [s.bucket("raw")], actions=["s3:PutObject"], prefix="drops/")
    key, secret = s.storage_user("stream-sink")
    (s.state_dir / "stream-sink.env").write_text(
        f"STREAM_SINK_ACCESS_KEY={key}\nSTREAM_SINK_SECRET_KEY={secret}\nRAW_BUCKET={s.bucket('raw')}\n")


def ensure_shred_journal(s) -> None:
    """A write-once journal. Object Lock (COMPLIANCE) is set at bucket creation; its writer
    may put and read entries, never delete them."""
    from loader import journal

    s3, name = s3_admin(s), journal.bucket(s)
    if name not in {b["Name"] for b in s3.list_buckets().get("Buckets", [])}:
        s3.create_bucket(Bucket=name, ObjectLockEnabledForBucket=True)
        log(f"bucket {name} created with object lock")
    s3.put_object_lock_configuration(Bucket=name, ObjectLockConfiguration={
        "ObjectLockEnabled": "Enabled",
        "Rule": {"DefaultRetention": {"Mode": "COMPLIANCE", "Days": MODEL["shred_journal_retention_days"]}}})
    admin.ensure_storage_user(s, "shred-journal", [name], ["s3:PutObject", "s3:GetObject", "s3:ListBucket"])


def ensure_dynamodb_tables(s) -> None:
    ddb = s.dynamodb()
    existing = set(ddb.list_tables()["TableNames"])
    for table, key in MODEL["dynamodb_tables"].items():
        if table not in existing:
            ddb.create_table(TableName=table, BillingMode="PAY_PER_REQUEST",
                             AttributeDefinitions=[{"AttributeName": key, "AttributeType": "S"}],
                             KeySchema=[{"AttributeName": key, "KeyType": "HASH"}])
            log(f"dynamodb table {table} created")
    for table, attribute in MODEL.get("dynamodb_ttl", {}).items():
        if s.is_aws:
            continue  # Terraform sets it
        current = ddb.describe_time_to_live(TableName=table)["TimeToLiveDescription"]
        if current.get("TimeToLiveStatus") != "ENABLED":
            ddb.update_time_to_live(TableName=table, TimeToLiveSpecification={"Enabled": True, "AttributeName": attribute})


def ensure_catalogs(root: Polaris, s) -> None:
    for name, logical in MODEL["catalogs"].items():
        location = f"s3://{s.bucket(logical)}/"
        properties = {"default-base-location": location, **MODEL["catalog_properties"]}
        status, body = root.mgmt("POST", "/catalogs", {"catalog": {
            "name": name, "type": "INTERNAL", "readOnly": False,
            "properties": properties, "storageConfigInfo": storage_config(s, name, location)}})
        if status == 201:
            log(f"catalog {name} created")
        elif status == 409:
            current = expect(root.mgmt("GET", f"/catalogs/{name}"), 200)
            if any(current["properties"].get(k) != v for k, v in properties.items()):
                expect(root.mgmt("PUT", f"/catalogs/{name}", {
                    "currentEntityVersion": current["entityVersion"],
                    "properties": {**current["properties"], **properties}}), 200)
                log(f"catalog {name} properties updated")
        else:
            raise RuntimeError(f"create catalog {name}: HTTP {status} {body}")
        for role, grants in MODEL["catalog_roles"].items():
            admin.ensure_catalog_role(root, name, role, grants)


def storage_config(s, catalog: str, location: str) -> dict:
    """Locally: clients and Polaris reach RustFS at the in-network name. On AWS: Polaris vends
    credentials by assuming the catalog's vending role, and every session may use the catalog's
    own KMS key (one bucket and key per sensitivity)."""
    base = {"storageType": "S3", "allowedLocations": [location], "region": s.region}
    if not s.is_aws:
        return {**base, "endpoint": s.s3_endpoint, "pathStyleAccess": True}
    return {**base, "roleArn": os.environ[f"LOADER_VENDING_ROLE_{catalog.upper()}"],
            "encryptionKeys": [os.environ[f"LOADER_KMS_{catalog.upper()}"]]}


def ensure_principals(root: Polaris, s) -> None:
    for principal, catalog_roles in MODEL["principals"].items():
        if admin.ensure_principal(root, s, principal, {c: catalog_roles for c in MODEL["catalogs"]}):
            log(f"principal {principal}: credentials issued")


def write_spark_conf(s) -> None:
    """Spark Connect properties: one Iceberg REST catalog per Polaris catalog, as `maintenance`.

    Storage access uses only vended credentials, like every other client.
    """
    client_id, client_secret = s.principal("maintenance")
    creds = {"clientId": client_id, "clientSecret": client_secret}
    lines = ["spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"]
    for catalog in MODEL["catalogs"]:
        p = f"spark.sql.catalog.{catalog}"
        lines += [
            f"{p}=org.apache.iceberg.spark.SparkCatalog",
            f"{p}.type=rest",
            f"{p}.uri={s.polaris_uri}",
            f"{p}.warehouse={catalog}",
            f"{p}.credential={creds['clientId']}:{creds['clientSecret']}",
            f"{p}.scope=PRINCIPAL_ROLE:ALL",
            f"{p}.header.X-Iceberg-Access-Delegation=vended-credentials",
            f"{p}.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
            f"{p}.client.region=us-east-1",
        ]
    path = s.state_dir / "spark-maintenance.conf"
    path.write_text("\n".join(lines) + "\n")
    path.chmod(0o644)


def ensure_policies(root: Polaris) -> None:
    """Default maintenance policies, stored in namespace `ops` and attached to each catalog."""
    ns = MODEL["policy_namespace"]
    for catalog in MODEL["catalogs"]:
        expect(root.rest("POST", f"{root.settings.polaris_uri}/v1/{catalog}/namespaces",
                         {"namespace": [ns]}), 200, 409)
        for name, policy in MODEL["default_policies"].items():
            base = root.policy_url(catalog, f"/namespaces/{ns}/policies")
            status, body = root.rest("POST", base, {
                "name": name, "type": policy["type"], "description": policy["description"],
                "content": json.dumps(policy["content"])})
            if status not in (200, 409):
                raise RuntimeError(f"create policy {catalog}.{name}: HTTP {status} {body}")
            expect(root.rest("PUT", f"{base}/{name}/mappings", {"target": {"type": "catalog"}}), 204, 200)
        for name, policy in MODEL["table_policies"].items():  # attached to single tables at deploy
            status, body = root.rest("POST", root.policy_url(catalog, f"/namespaces/{ns}/policies"), {
                "name": name, "type": policy["type"], "description": policy["description"],
                "content": json.dumps(policy["content"])})
            if status not in (200, 409):
                raise RuntimeError(f"create policy {catalog}.{name}: HTTP {status} {body}")


def main() -> None:
    s = settings_mod.load()
    root = Polaris(s)
    for attempt in range(30):  # Polaris reports healthy slightly before its token endpoint answers
        try:
            root.token
            break
        except Exception:
            time.sleep(2)
    if not s.is_aws:  # on AWS, Terraform owns buckets, keys, lifecycles and IAM
        ensure_buckets(s)
    ensure_dynamodb_tables(s)
    ensure_catalogs(root, s)
    ensure_principals(root, s)
    ensure_policies(root)
    if not s.is_aws:
        write_spark_conf(s)  # EMR Serverless reads the maintenance credential itself on AWS
        admin.ensure_storage_user(s, "loader-raw", [s.bucket("raw"), s.bucket("cdc-staging")])
        log("storage user loader-raw (raw and cdc-staging buckets) ready")
        # The capture tool's identity (DMS's role on AWS): writes change files to its own bucket only.
        admin.ensure_storage_user(s, "cdc-sink", [s.bucket("cdc-staging")], actions=["s3:PutObject"])
        log("storage user cdc-sink (put into cdc-staging only) ready")
        ensure_stream_sink(s)
        log("storage user stream-sink (put into raw/drops/ only) ready")
        ensure_raw_expiry(s)
        ensure_shred_journal(s)
        log("raw zone expiry and shred journal ready")
        ensure_lake_versioning(s)
        log("lake versioning and storage user lake-restore (copy back earlier versions only) ready")
    for contract in contracts_mod.load_all().values():
        deploy.deploy_contract(root, s, contract)
        log(f"source {contract.source} deployed")
    from stack.stream_sink import streams

    stream_sources = [c.source for c in contracts_mod.load_all().values() if c.fetch.kind == "stream"]
    streams.ensure(s, stream_sources)
    log(f"streams ready: {', '.join(stream_sources)}")
    from loader import gold

    log(f"gold tables ready: {', '.join(m.qualified for m in gold.deploy(s))}")
    from flows.deploy import deploy_gold, deploy_maintenance, deploy_sources

    log(f"{len(deploy_sources())} ingest deployments registered with Prefect")
    deploy_gold()
    deploy_maintenance()
    log("gold and maintenance deployments registered with Prefect")
    log("done")


if __name__ == "__main__":
    main()
