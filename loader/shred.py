"""Erasure by crypto-shredding.

Order matters: the journal entry is written first, so a shred interrupted halfway is finished by the
next attempt with the same request ID, or by the next vault restore. Then the subjects' keys and
identity entries are deleted, which makes their identifiers unreadable everywhere in the lake. Last,
the raw files of every batch that mentioned them are deleted, so a replay cannot bring them back
(those batches stay in Bronze, encrypted, and replay skips them).
"""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from loader import journal, raw
from loader.identity import Identity
from loader.settings import Settings
from loader.state import Manifest
from loader.vault import KeyVault

BATCH_SUBJECTS, SHRED_EVENTS = "batch_subjects", "shred_events"


@dataclass
class ShredResult:
    request_id: str
    subject_ids: list[str]
    raw_files_deleted: int
    journal_key: str


# DynamoDB items are at most 400 KB: one item of a batch's subject IDs failed above about 12,000
# subjects (benchmark). IDs go into as many items as needed: <batch_id>, <batch_id>#1, ...
IDS_PER_ITEM = 8_000
# Entries matter only while the batch's raw file can still be deleted; DynamoDB drops them after.
TTL_DAYS_PAST_RAW_EXPIRY = 7


def record_batch_subjects(s: Settings, batch_id: str, subject_ids: list[str]) -> None:
    """Which subjects a batch mentions (IDs only). Written before the batch is committed."""
    ids = sorted(set(subject_ids))
    if not ids:
        return
    from loader.raw import EXPIRY_DAYS

    expires = int((datetime.now(timezone.utc) + timedelta(days=EXPIRY_DAYS + TTL_DAYS_PAST_RAW_EXPIRY)).timestamp())
    items = [{"batch_id": {"S": batch_id if n == 0 else f"{batch_id}#{n}"}, "subject_ids": {"SS": ids[i:i + IDS_PER_ITEM]},
              "expires_at": {"N": str(expires)}} for n, i in enumerate(range(0, len(ids), IDS_PER_ITEM))]
    ddb = s.dynamodb()
    for i in range(0, len(items), 25):
        request = {BATCH_SUBJECTS: [{"PutRequest": {"Item": item}} for item in items[i:i + 25]]}
        while request:
            request = ddb.batch_write_item(RequestItems=request).get("UnprocessedItems") or None


def batches_mentioning(s: Settings, subject_ids: set[str]) -> list[str]:
    ddb, out, kwargs = s.dynamodb(), set(), {"TableName": BATCH_SUBJECTS, "ConsistentRead": True}
    while True:
        page = ddb.scan(**kwargs)
        out |= {i["batch_id"]["S"].split("#", 1)[0] for i in page.get("Items", []) if subject_ids & set(i["subject_ids"]["SS"])}
        if "LastEvaluatedKey" not in page:
            return sorted(out)
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def shred(s: Settings, request_id: str, actor: str, reason: str, person_id: str | None = None,
          subject_ids: list[str] | None = None) -> ShredResult:
    identity = Identity(s)
    if person_id:
        subject_ids = identity.subjects_of(person_id) or (subject_ids or [])
    if not subject_ids:
        raise ValueError("nothing to shred: unknown person and no subject IDs")
    key = journal.append(s, request_id, subject_ids, actor, reason, person_id)
    KeyVault(s).delete(subject_ids)
    identity.forget(subject_ids)

    manifest, zone, deleted = Manifest(s), raw.RawZone(s), 0
    for batch_id in batches_mentioning(s, set(subject_ids)):
        batch = manifest.get(batch_id)
        if batch and batch.raw_key and not batch.raw_purged:
            zone.delete(batch.raw_key)
            manifest.mark_raw_purged(batch_id)
            deleted += 1
    s.dynamodb().put_item(TableName=SHRED_EVENTS, Item={
        "request_id": {"S": request_id}, "at": {"S": datetime.now(timezone.utc).isoformat()},
        "actor": {"S": actor}, "reason": {"S": reason}, "subjects": {"N": str(len(subject_ids))},
        "raw_files_deleted": {"N": str(deleted)}, "journal_key": {"S": key}})
    return ShredResult(request_id, sorted(subject_ids), deleted, key)
