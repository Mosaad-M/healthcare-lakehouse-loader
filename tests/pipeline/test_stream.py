"""The streaming minutes tier. Events published to a source's stream (a Redpanda topic locally,
a Firehose stream on AWS) are written into its drop prefix every few seconds, and the file-drop
path loads them. Delivery is at least once, so repeats must be harmless.
"""
import json
import os
import time

import boto3
import botocore.exceptions
import pytest

from flows.ingest import ingest
from loader import duck, raw, tables
from loader.state import QUARANTINED, SILVER_COMMITTED, Manifest
from stack.stream_sink import streams as entry
from tests.pipeline.conftest import deploy_copy, remove_source


@pytest.fixture(scope="module")
def streams(s, run_id, contracts_dir):
    """Two stream sources, to show the sink never mixes topics."""
    deployed = [deploy_copy(s, contracts_dir, "acme_vitals", f"vitals_{side}_{run_id}") for side in "ab"]
    try:
        entry.ensure(s, [c.source for c in deployed])
        yield deployed
    finally:  # also when creating the streams failed: the tables exist either way
        for contract in deployed:
            remove_source(s, contract)
        present = set(entry.existing(s))
        entry.delete(s, [c.source for c in deployed if c.source in present])


@pytest.fixture(scope="module")
def producer(s):
    return entry.Publisher(s)


def reading(contract, i: int, mrn: str, value: float = 70.0) -> dict:
    return {"reading_id": f"{contract.source}-{i:05d}", "patient_mrn": mrn, "device_id": f"monitor-{i % 4}",
            "measured_at": f"2026-10-13T10:{i % 60:02d}:00Z", "metric": "heart_rate", "value": value + i % 30}


def publish(producer, contract, events: list[dict]) -> None:
    producer.publish(contract.source, events)


def dropped_ids(s, contract) -> list[str]:
    zone = raw.RawZone(s)
    return [json.loads(line)["reading_id"] for key in zone.pending_drops(contract)
            for line in zone.read(key).splitlines() if line.strip()]


def wait_for_drops(s, contract, count: int, timeout: float = 180) -> None:
    """Until the sink has written `count` events into the drop prefix (one flush is 10 s)."""
    deadline = time.time() + timeout
    while len(dropped_ids(s, contract)) < count:
        if time.time() > deadline:
            raise TimeoutError(f"{len(dropped_ids(s, contract))} of {count} events dropped within {timeout} s")
        time.sleep(1)


def run(contract, contracts_dir):
    ingest(source=contract.source, contracts_dir=str(contracts_dir))


def silver_count(s, contract) -> int:
    con = duck.connect(s, tables.CATALOG, principal=contract.principal)
    return con.execute(f"SELECT count(*) FROM {tables.qualified(contract, tables.SILVER)}").fetchone()[0]


def test_events_arrive_in_few_files_and_never_mix_topics(s, streams, producer, canaries):
    a, b = streams
    events_a = [reading(a, i, canaries.mrn if i == 0 else f"MRN{canaries.tag}{i:04d}") for i in range(300)]
    events_b = [reading(b, i, f"MRN{canaries.tag}B{i:04d}") for i in range(40)]
    for start in range(0, 300, 30):  # interleaved, as on a shared broker
        publish(producer, a, events_a[start:start + 30])
        publish(producer, b, events_b[start // 30 * 4:start // 30 * 4 + 4])
    wait_for_drops(s, a, 300)
    wait_for_drops(s, b, 40)
    assert sorted(dropped_ids(s, a)) == sorted(e["reading_id"] for e in events_a)
    assert sorted(dropped_ids(s, b)) == sorted(e["reading_id"] for e in events_b)
    assert len(raw.RawZone(s).pending_drops(a)) < 10, "the sink should batch events, not write one file each"


def test_dropped_events_load_as_one_batch_per_file(s, streams, contracts_dir):
    a, b = streams
    files = len(raw.RawZone(s).pending_drops(a))
    run(a, contracts_dir)
    run(b, contracts_dir)
    batches = Manifest(s).for_source(a.source)
    assert len(batches) == files and {x.state for x in batches} == {SILVER_COMMITTED}
    assert (silver_count(s, a), silver_count(s, b)) == (300, 40)


def test_a_redelivered_event_is_loaded_once(s, streams, producer, contracts_dir, canaries):
    a, _ = streams
    again = [reading(a, i, f"MRN{canaries.tag}{i:04d}") for i in range(1, 6)]
    publish(producer, a, again + again)  # repeats within one flush and of events already in Silver
    wait_for_drops(s, a, 10)
    run(a, contracts_dir)
    assert silver_count(s, a) == 300
    assert QUARANTINED not in {x.state for x in Manifest(s).for_source(a.source)}


def test_a_changed_repeat_in_one_file_is_quarantined(s, streams, contracts_dir, canaries):
    """Identical copies are one event; two different readings under one ID are an error."""
    a, _ = streams
    first, changed = reading(a, 9000, f"MRN{canaries.tag}9000"), reading(a, 9000, f"MRN{canaries.tag}9000", 99.0)
    raw.RawZone(s).drop(a, "conflict.ndjson", (json.dumps(first) + "\n" + json.dumps(changed) + "\n").encode())
    run(a, contracts_dir)
    assert QUARANTINED in {x.state for x in Manifest(s).for_source(a.source)}
    assert silver_count(s, a) == 300


def test_identifiers_are_ciphertext_in_bronze_and_silver(s, streams, canaries):
    a, _ = streams
    con = duck.connect(s, tables.CATALOG, principal=a.principal)
    for layer in (tables.BRONZE, tables.SILVER):
        rows = con.execute(f"SELECT patient_mrn FROM {tables.qualified(a, layer)}").fetchall()
        assert rows and all(isinstance(r[0], (bytes, bytearray)) for r in rows)
        assert canaries.mrn.encode() not in b"".join(bytes(r[0]) for r in rows)


def test_the_sink_can_only_put_into_drop_prefixes(s, streams):
    """A compromised sink cannot read the raw zone, delete from it, or write where the loader lands."""
    bucket, a = s.bucket("raw"), streams[0]
    landed = Manifest(s).for_source(a.source)[0].raw_key
    if s.is_aws:  # the sink is Firehose, delivering as its role: ask IAM what that role may do
        iam, allowed = boto3.client("iam"), set()
        for where, key in (("drops", f"drops/{a.source}/x.ndjson"), ("landed", landed)):
            for result in iam.simulate_principal_policy(
                    PolicySourceArn=os.environ["LOADER_FIREHOSE_ROLE"],
                    ActionNames=["s3:PutObject", "s3:GetObject", "s3:DeleteObject"],
                    ResourceArns=[f"arn:aws:s3:::{bucket}/{key}"])["EvaluationResults"]:
                if result["EvalDecision"] == "allowed":
                    allowed.add((result["EvalActionName"], where))
        # Firehose's documented minimum includes GetObject; it is limited to drops/ too.
        assert allowed == {("s3:PutObject", "drops"), ("s3:GetObject", "drops")}, allowed
        return
    key, secret = s.storage_user("stream-sink")
    sink = boto3.client("s3", endpoint_url=s.s3_endpoint, region_name=s.region,
                        aws_access_key_id=key, aws_secret_access_key=secret)
    sink.put_object(Bucket=bucket, Key=f"drops/{a.source}/probe.ndjson", Body=b"")
    raw.RawZone(s).delete(f"drops/{a.source}/probe.ndjson")
    attempts = {
        "list": lambda: sink.list_objects_v2(Bucket=bucket, Prefix="landed/"),
        "read": lambda: sink.get_object(Bucket=bucket, Key=landed),
        "delete": lambda: sink.delete_object(Bucket=bucket, Key=landed),
        "write landed": lambda: sink.put_object(Bucket=bucket, Key=f"landed/{a.source}/x", Body=b"x"),
    }
    for name, attempt in attempts.items():
        with pytest.raises(botocore.exceptions.ClientError, match="AccessDenied"):
            attempt()
