"""The streaming entry point of each stream source, on either platform. Idempotent.

Locally: a Kafka topic named after the source (Redpanda); Redpanda Connect writes its events into
raw/drops/<source>/. On AWS: a Firehose Direct PUT stream `lakehouse-<source>` that writes into the
same prefix (10 s buffer, the raw zone's KMS key), delivering as a role that may only put there.
"""
import json
import os
import time

from loader.settings import Settings

BROKERS = os.environ.get("KAFKA_BROKERS", "redpanda:9092")
FIREHOSE_PREFIX = "lakehouse-"
FLUSH_SECONDS = 10


def _firehose(s: Settings):
    import boto3

    return boto3.client("firehose", region_name=s.region)


def _kafka_admin():
    from confluent_kafka.admin import AdminClient

    return AdminClient({"bootstrap.servers": BROKERS})


def ensure(s: Settings, sources: list[str]) -> None:
    if not sources:
        return
    missing = [name for name in sources if name not in existing(s)]
    if not missing:
        return
    if not s.is_aws:
        from confluent_kafka.admin import NewTopic

        admin = _kafka_admin()  # keep the client alive until its futures resolve
        futures = admin.create_topics([NewTopic(n, num_partitions=1, replication_factor=1) for n in missing])
        for future in futures.values():
            future.result(timeout=30)
        return
    firehose = _firehose(s)
    for name in missing:
        firehose.create_delivery_stream(
            DeliveryStreamName=f"{FIREHOSE_PREFIX}{name}", DeliveryStreamType="DirectPut",
            ExtendedS3DestinationConfiguration={
                "RoleARN": os.environ["LOADER_FIREHOSE_ROLE"],
                "BucketARN": f"arn:aws:s3:::{s.bucket('raw')}",
                "Prefix": f"drops/{name}/",
                "FileExtension": ".ndjson",
                "BufferingHints": {"IntervalInSeconds": FLUSH_SECONDS, "SizeInMBs": 5},
                "CompressionFormat": "UNCOMPRESSED",
                "EncryptionConfiguration": {"KMSEncryptionConfig": {"AWSKMSKeyARN": os.environ["LOADER_KMS_RAW"]}},
            },
            Tags=[{"Key": "Project", "Value": "lakehouse-loader"}])
    for name in missing:  # creation takes a minute or two
        deadline = time.time() + 600
        while firehose.describe_delivery_stream(DeliveryStreamName=f"{FIREHOSE_PREFIX}{name}")[
                "DeliveryStreamDescription"]["DeliveryStreamStatus"] != "ACTIVE":
            if time.time() > deadline:
                raise TimeoutError(f"delivery stream for {name} not active after 10 minutes")
            time.sleep(5)


def existing(s: Settings) -> list[str]:
    """Source names that have a stream entry point."""
    if not s.is_aws:
        return sorted(t for t in _kafka_admin().list_topics(timeout=30).topics if not t.startswith("_"))
    names, kwargs, firehose = [], {"Limit": 100}, _firehose(s)
    while True:
        page = firehose.list_delivery_streams(**kwargs)
        names += [n.removeprefix(FIREHOSE_PREFIX) for n in page["DeliveryStreamNames"] if n.startswith(FIREHOSE_PREFIX)]
        if not page["HasMoreDeliveryStreams"]:
            return sorted(names)
        kwargs["ExclusiveStartDeliveryStreamName"] = page["DeliveryStreamNames"][-1]


def delete(s: Settings, sources: list[str]) -> None:
    if not sources:
        return
    if not s.is_aws:
        admin = _kafka_admin()
        for future in admin.delete_topics(sources).values():
            future.result(timeout=30)
        return
    for name in sources:
        _firehose(s).delete_delivery_stream(DeliveryStreamName=f"{FIREHOSE_PREFIX}{name}", AllowForceDelete=True)


class Publisher:
    """What a producing application does: send JSON events to a source's stream."""

    def __init__(self, s: Settings):
        self.s = s
        if s.is_aws:
            self.firehose = _firehose(s)
        else:
            from confluent_kafka import Producer

            self.producer = Producer({"bootstrap.servers": BROKERS, "enable.idempotence": True})

    def publish(self, source: str, events: list[dict]) -> None:
        if not self.s.is_aws:
            for event in events:
                self.producer.produce(source, json.dumps(event).encode())
            if self.producer.flush(30):
                raise RuntimeError("events not acknowledged by the broker")
            return
        for start in range(0, len(events), 500):  # Firehose takes 500 records per call
            records = [{"Data": (json.dumps(e) + "\n").encode()} for e in events[start:start + 500]]
            result = self.firehose.put_record_batch(DeliveryStreamName=f"{FIREHOSE_PREFIX}{source}", Records=records)
            if result["FailedPutCount"]:
                raise RuntimeError(f"{result['FailedPutCount']} events refused by Firehose")
