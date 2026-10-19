"""PHI must not leak into Iceberg metadata or into logs."""
import duckdb
import pytest

from loader import duck

FAKE_SSN = "123-45-6789"  # synthetic


def manifest_bounds(s, con, qualified: str) -> dict[int, dict[str, bytes | None]]:
    """Column id -> lower/upper bound bytes across the table's current data manifests.

    Manifests are read with credentials Polaris vends for this table to the read-only `auditor`
    (the lake buckets admit no other path on AWS).
    """
    from loader.polaris import Polaris, vended_s3

    _, namespace, table = qualified.split(".")
    body = Polaris(s, "auditor").load_table("phi", namespace, table, vended=True)
    location = body["metadata"]["location"]
    con.execute(s.duckdb_s3_secret("audit_manifests", location, vended_s3(body)))
    paths = [r[0] for r in con.execute(
        f"SELECT DISTINCT manifest_path FROM iceberg_metadata({qualified}) WHERE manifest_content = 'DATA'").fetchall()]
    bounds: dict[int, dict[str, bytes | None]] = {}
    for path in paths:
        for lower, upper in con.execute(
                "SELECT data_file.lower_bounds, data_file.upper_bounds FROM read_avro(?)", [path]).fetchall():
            for col_id, value in (lower or {}).items():
                bounds.setdefault(col_id, {})["lower"] = value
            for col_id, value in (upper or {}).items():
                bounds.setdefault(col_id, {})["upper"] = value
    return bounds


@pytest.mark.parametrize("props, expect_ssn_bounds", [
    ({}, True),
    ({"write.metadata.metrics.column.ssn": "counts"}, False),
    ({"write.metadata.metrics.column.ssn": "none"}, False),
    ({"write.metadata.metrics.default": "none"}, False),
])
def test_metrics_policy_controls_bounds(s, polaris, ns, table, props, expect_ssn_bounds):
    """Default truncate(16) stores short PHI verbatim in manifests; the metrics policy prevents it."""
    name, t = table
    duck.connect(s, "phi").execute(f"CREATE TABLE {t} (id INT, ssn VARCHAR)")
    if props:
        polaris.update_table("phi", ns, name, [{"action": "set-properties", "updates": props}])
    writer = duck.connect(s, "phi")
    writer.execute(f"INSERT INTO {t} VALUES (1, '{FAKE_SSN}')")
    meta = polaris.load_table("phi", ns, name)["metadata"]
    ssn_id = next(f["id"] for f in meta["schemas"][-1]["fields"] if f["name"] == "ssn")
    ssn = manifest_bounds(s, writer, t).get(ssn_id, {})
    leaked = FAKE_SSN.encode() in (ssn.get("lower") or b"") + (ssn.get("upper") or b"")
    assert leaked == expect_ssn_bounds


def test_redact_strips_values_from_errors(con, table):
    """DuckDB quotes offending values in errors; logging str(err) would leak PHI."""
    _, t = table
    con.execute(f"CREATE TABLE {t} (dob DATE)")
    with pytest.raises(duckdb.Error) as exc:
        con.execute(f"INSERT INTO {t} VALUES ('{FAKE_SSN}')")
    assert FAKE_SSN in str(exc.value), "DuckDB no longer echoes values; redaction may be unnecessary"
    assert FAKE_SSN not in duck.redact(exc.value)
