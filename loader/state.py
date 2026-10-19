"""The batch manifest and table locks, in DynamoDB.

Manifest states: LANDED -> BRONZE_COMMITTED -> SILVER_COMMITTED, or QUARANTINED. Items hold no PHI:
ids, hashes, counts, timestamps and our own error messages (never raw error text).
"""
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from contextlib import contextmanager
from dataclasses import dataclass

import botocore.exceptions

from loader.settings import Settings

log = logging.getLogger(__name__)

MANIFEST, LOCKS = "loader_manifest", "loader_locks"
LANDED, BRONZE_COMMITTED, SILVER_COMMITTED, QUARANTINED = (
    "LANDED", "BRONZE_COMMITTED", "SILVER_COMMITTED", "QUARANTINED")
DONE = (SILVER_COMMITTED, QUARANTINED)


@dataclass
class Batch:
    batch_id: str
    source: str
    state: str
    raw_key: str
    content_sha256: str
    name_sha256: str
    size: int
    landed_at: str  # ISO-8601 UTC; becomes Bronze's _ingested_at, so replays are identical
    rows_bronze: int = 0
    rows_silver: int = 0
    reason: str = ""
    raw_purged: bool = False  # raw file deleted by a shred; replay skips the batch

    @classmethod
    def from_item(cls, item: dict) -> "Batch":
        val = {k: next(iter(v.values())) for k, v in item.items()}
        return cls(batch_id=val["batch_id"], source=val["source"], state=val["state"], raw_key=val["raw_key"],
                   content_sha256=val["content_sha256"], name_sha256=val["name_sha256"], size=int(val["size"]),
                   landed_at=val["landed_at"], rows_bronze=int(val.get("rows_bronze", 0)),
                   rows_silver=int(val.get("rows_silver", 0)), reason=val.get("reason", ""),
                   raw_purged=val.get("raw_purged") == "1")


class Manifest:
    def __init__(self, s: Settings):
        self.ddb = s.dynamodb()

    def get(self, batch_id: str) -> Batch | None:
        item = self.ddb.get_item(TableName=MANIFEST, Key={"batch_id": {"S": batch_id}}, ConsistentRead=True).get("Item")
        return Batch.from_item(item) if item else None

    def add(self, batch: Batch) -> bool:
        """Record a newly landed batch. False if the batch is already known (same source and bytes)."""
        item = {"batch_id": {"S": batch.batch_id}, "source": {"S": batch.source}, "state": {"S": LANDED},
                "raw_key": {"S": batch.raw_key}, "content_sha256": {"S": batch.content_sha256},
                "name_sha256": {"S": batch.name_sha256}, "size": {"N": str(batch.size)},
                "landed_at": {"S": batch.landed_at}, "updated_at": {"S": _now()}}
        try:
            self.ddb.put_item(TableName=MANIFEST, Item=item, ConditionExpression="attribute_not_exists(batch_id)")
            return True
        except botocore.exceptions.ClientError as err:
            if err.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise

    def advance(self, batch_id: str, state: str, **fields) -> None:
        """Move a batch to `state` and set extra fields (counts, reason, raw_key)."""
        names = {"#s": "state", "#u": "updated_at"}
        values = {":s": {"S": state}, ":u": {"S": _now()}}
        sets = ["#s = :s", "#u = :u"]
        for i, (key, value) in enumerate(fields.items()):
            names[f"#f{i}"] = key
            values[f":f{i}"] = {"N": str(value)} if isinstance(value, int) else {"S": str(value)}
            sets.append(f"#f{i} = :f{i}")
        self.ddb.update_item(TableName=MANIFEST, Key={"batch_id": {"S": batch_id}},
                             UpdateExpression="SET " + ", ".join(sets),
                             ExpressionAttributeNames=names, ExpressionAttributeValues=values)

    def mark_raw_purged(self, batch_id: str) -> None:
        self.ddb.update_item(TableName=MANIFEST, Key={"batch_id": {"S": batch_id}},
                             UpdateExpression="SET raw_purged = :y", ExpressionAttributeValues={":y": {"S": "1"}})

    def for_source(self, source: str) -> list[Batch]:
        """All batches of a source in landing order. A scan is fine at this scale (a GSI at larger ones)."""
        items, kwargs = [], {"TableName": MANIFEST, "FilterExpression": "#src = :src",
                             "ExpressionAttributeNames": {"#src": "source"},
                             "ExpressionAttributeValues": {":src": {"S": source}}, "ConsistentRead": True}
        while True:
            page = self.ddb.scan(**kwargs)
            items += page.get("Items", [])
            if "LastEvaluatedKey" not in page:
                break
            kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]
        return sorted((Batch.from_item(i) for i in items), key=lambda b: (b.landed_at, b.batch_id))


class LockLost(RuntimeError):
    """The lease expired and was taken while this writer still worked: redo the work."""


class LockHeld(RuntimeError):
    pass


@contextmanager
def table_lock(s: Settings, table: str, lease_seconds: int | None = None, wait_seconds: float = 600,
               renew_seconds: float = 30):
    """A lease lock per table (one writer per table, maintenance included).

    The holder renews its lease every `renew_seconds` while it works, so long work keeps the lock
    (a 300,000-member batch took 30 minutes in the benchmark) and a crashed holder blocks the
    table for at most `lease_seconds` (it used to be 15 minutes). If a renewal fails (the process
    stalled past the lease and another writer took over), leaving the block raises LockLost: the
    work is redone, which is safe because every write is idempotent by batch.

    Writers wait up to `wait_seconds` for a busy table (a maintenance run or another writer);
    maintenance passes 0 and skips the table instead.
    """
    lease_seconds = lease_seconds or int(os.environ.get("LOADER_LOCK_LEASE_SECONDS", "120"))
    ddb, owner = s.dynamodb(), uuid.uuid4().hex
    deadline = time.time() + wait_seconds
    while True:
        now = int(time.time())
        try:
            ddb.put_item(TableName=LOCKS,
                         Item={"lock_name": {"S": table}, "owner": {"S": owner},
                               "expires_at": {"N": str(now + lease_seconds)}},
                         ConditionExpression="attribute_not_exists(lock_name) OR expires_at < :now",
                         ExpressionAttributeValues={":now": {"N": str(now)}})
            break
        except botocore.exceptions.ClientError as err:
            if err.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
            if time.time() >= deadline:
                raise LockHeld(f"table {table} is locked by another writer") from None
            time.sleep(min(2.0, max(0.1, deadline - time.time())))
    stop, lost = threading.Event(), threading.Event()

    def renew() -> None:
        while not stop.wait(renew_seconds):
            try:
                ddb.update_item(TableName=LOCKS, Key={"lock_name": {"S": table}},
                                UpdateExpression="SET expires_at = :t", ConditionExpression="#o = :o",
                                ExpressionAttributeNames={"#o": "owner"},
                                ExpressionAttributeValues={":t": {"N": str(int(time.time()) + lease_seconds)},
                                                           ":o": {"S": owner}})
            except botocore.exceptions.ClientError as err:
                if err.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    lost.set()
                    log.error("lease on %s lost while held", table)
                    return
                log.warning("lease renewal on %s failed; retrying", table)

    heartbeat = threading.Thread(target=renew, name=f"lease-{table}", daemon=True)
    heartbeat.start()
    try:
        yield owner
    finally:
        stop.set()
        heartbeat.join()
        try:
            ddb.delete_item(TableName=LOCKS, Key={"lock_name": {"S": table}},
                            ConditionExpression="#o = :o", ExpressionAttributeNames={"#o": "owner"},
                            ExpressionAttributeValues={":o": {"S": owner}})
        except botocore.exceptions.ClientError as err:
            if err.response["Error"]["Code"] != "ConditionalCheckFailedException":  # lease taken over
                raise
    if lost.is_set():
        raise LockLost(f"the lease on {table} was lost while held; the work will be redone")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")
