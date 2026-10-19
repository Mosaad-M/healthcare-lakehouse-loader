"""Iceberg maintenance operations on a Spark session: the only Spark code the loader has.

Self-contained (standard library and PySpark only) so it serves twice: imported by the Spark
Connect executor locally, and uploaded as the entry point of EMR Serverless maintenance jobs on AWS.
Tables are `catalog.namespace.name`. Actions are JSON-serializable dicts:

    {"kind": "compact", "options": {...}}
    {"kind": "rewrite_manifests"}
    {"kind": "expire_snapshots", "older_than": "YYYY-MM-DD HH:MM:SS", "retain_last": 5}
    {"kind": "remove_orphans", "older_than": "...", "listing": [[path, iso_time], ...], "dry_run": false}
"""
import json
import sys
import uuid
from datetime import datetime


def _split(table: str) -> tuple[str, str]:
    catalog, rest = table.split(".", 1)
    return catalog, rest


def stats(spark, table: str) -> dict:
    """Live files of the current snapshot (from the `files` metadata table: snapshot summaries carry
    running totals that writers do not always keep exact) and the manifest count."""
    row = spark.sql(
        "SELECT count_if(content = 0) AS data_files, count_if(content <> 0) AS delete_files, "
        "coalesce(sum(CASE WHEN content = 0 THEN record_count END), 0) AS records, "
        f"coalesce(sum(CASE WHEN content = 0 THEN file_size_in_bytes END), 0) AS data_bytes FROM {table}.files"
    ).collect()[0].asDict()
    row["manifests"] = spark.sql(f"SELECT count(*) AS n FROM {table}.manifests").collect()[0]["n"]
    return {k: int(v or 0) for k, v in row.items()}


def run(spark, table: str, action: dict) -> dict:
    catalog, rest = _split(table)

    def call(procedure: str, args: str = "") -> dict:
        rows = spark.sql(f"CALL {catalog}.system.{procedure}(table => '{rest}'{args})").collect()
        return rows[0].asDict() if rows else {}

    kind = action["kind"]
    if kind == "compact":
        opts = ", ".join(f"'{k}', '{v}'" for k, v in action.get("options", {}).items())
        return call("rewrite_data_files", f", options => map({opts})" if opts else "")
    if kind == "rewrite_manifests":
        return call("rewrite_manifests")
    if kind == "expire_snapshots":
        return call("expire_snapshots", f", older_than => TIMESTAMP '{action['older_than']}', "
                                        f"retain_last => {int(action['retain_last'])}")
    if kind == "remove_orphans":
        view = f"listing_{uuid.uuid4().hex[:8]}"
        listing = [(path, datetime.fromisoformat(when)) for path, when in action["listing"]]
        spark.createDataFrame(listing, "file_path string, last_modified timestamp").createOrReplaceTempView(view)
        try:
            rows = spark.sql(f"CALL {catalog}.system.remove_orphan_files(table => '{rest}', "
                             f"older_than => TIMESTAMP '{action['older_than']}', "
                             f"dry_run => {str(bool(action.get('dry_run'))).lower()}, "
                             f"file_list_view => '{view}')").collect()
        finally:
            spark.catalog.dropTempView(view)
        return {"orphan_files": len(rows), "dry_run": bool(action.get("dry_run"))}
    raise ValueError(f"unknown maintenance action {kind!r}")


# ---------------------------------------------------------------- EMR Serverless entry point

def _emr_session(spec: dict):
    """A Spark session with one Iceberg REST catalog per Polaris catalog, as `maintenance`, whose
    credential is read from Secrets Manager (never passed on the command line)."""
    import boto3
    from pyspark.sql import SparkSession

    creds = json.loads(boto3.client("secretsmanager", region_name=spec["region"])
                       .get_secret_value(SecretId=spec["principals_secret"])["SecretString"])
    builder = SparkSession.builder.appName("lakehouse-maintenance").config(
        "spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    for catalog in spec["catalogs"]:
        p = f"spark.sql.catalog.{catalog}"
        for key, value in {
            "": "org.apache.iceberg.spark.SparkCatalog", ".type": "rest", ".uri": spec["polaris_uri"],
            ".warehouse": catalog, ".credential": f"{creds['clientId']}:{creds['clientSecret']}",
            ".scope": "PRINCIPAL_ROLE:ALL", ".header.X-Iceberg-Access-Delegation": "vended-credentials",
            ".io-impl": "org.apache.iceberg.aws.s3.S3FileIO", ".client.region": spec["region"],
        }.items():
            builder = builder.config(p + key, value)
    return builder.getOrCreate()


def emr_main(spec_uri: str) -> None:
    """Read a job spec from S3, run its operations in order, write the results next to it."""
    import boto3

    bucket, key = spec_uri.removeprefix("s3://").split("/", 1)
    s3 = boto3.client("s3")
    spec = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    spark = _emr_session(spec)
    results = []
    for op in spec["operations"]:
        if op["op"] == "stats":
            results.append(stats(spark, op["table"]))
        else:
            results.append(run(spark, op["table"], op["action"]))
    s3.put_object(Bucket=bucket, Key=key.rsplit("/", 1)[0] + "/results.json",
                  Body=json.dumps(results, default=str).encode())


if __name__ == "__main__":
    emr_main(sys.argv[1])
