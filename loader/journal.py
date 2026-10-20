"""The write-once journal: one entry per erasure request, and a copy of every decrypt
audit event.

Entries live in their own bucket with S3 Object Lock in COMPLIANCE mode, so nobody, including the
storage administrator, can delete or rewrite them before retention ends (1 day locally; 7 years in
production). Entries hold subject and person IDs, time, request ID, actor and a reason code: no PHI.
The writer's storage user may put and read, never delete. Decrypt audit events go under
`decrypt-audit/`, one object per event and status, so the history of a call cannot be rewritten.
"""
import json
from datetime import datetime, timezone

from loader.settings import Settings

LOGICAL = "shred-journal"


def bucket(s: Settings) -> str:
    return s.bucket(LOGICAL)


def client(s: Settings):
    """The journal writer's identity: put and read, never delete."""
    return s.s3("shred-journal")


def append(s: Settings, request_id: str, subject_ids: list[str], actor: str, reason: str,
           person_id: str | None = None) -> str:
    now = datetime.now(timezone.utc)
    key = f"entries/{now:%Y/%m/%d}/{now:%H%M%S%f}-{request_id}.json"
    entry = {"request_id": request_id, "at": now.isoformat(), "subject_ids": sorted(subject_ids),
             "person_id": person_id, "actor": actor, "reason": reason}
    client(s).put_object(Bucket=bucket(s), Key=key, Body=json.dumps(entry).encode(), ContentType="application/json")
    return key


def entries(s: Settings) -> list[dict]:
    c, out, name = client(s), [], bucket(s)
    for page in c.get_paginator("list_objects_v2").paginate(Bucket=name, Prefix="entries/"):
        for obj in page.get("Contents", []):
            out.append(json.loads(c.get_object(Bucket=name, Key=obj["Key"])["Body"].read()))
    return sorted(out, key=lambda e: e["at"])


def shredded_subjects(s: Settings) -> set[str]:
    return {sid for e in entries(s) for sid in e["subject_ids"]}


def record_decrypt(s: Settings, event: dict) -> str:
    """A write-once copy of one decrypt audit event (IDs, role, purpose, counts; never values)."""
    now = datetime.now(timezone.utc)
    key = f"decrypt-audit/{now:%Y/%m/%d}/{event['event_id']}-{now:%H%M%S%f}-{event['status']}.json"
    client(s).put_object(Bucket=bucket(s), Key=key, Body=json.dumps(event, sort_keys=True).encode(),
                         ContentType="application/json")
    return key


def decrypt_events(s: Settings, event_id: str, day: datetime | None = None) -> list[dict]:
    """The write-once audit history of one decrypt call (on `day`, today by default)."""
    c, name, day = client(s), bucket(s), day or datetime.now(timezone.utc)
    out = []
    for page in c.get_paginator("list_objects_v2").paginate(Bucket=name, Prefix=f"decrypt-audit/{day:%Y/%m/%d}/{event_id}-"):
        for obj in page.get("Contents", []):
            out.append(json.loads(c.get_object(Bucket=name, Key=obj["Key"])["Body"].read()))
    return out


def record_partner_event(s: Settings, partner: str, event: dict) -> str:
    """A write-once record of a partner freeze, unfreeze, report, approval or delete: who, when, why,
    the approval and the report hash (no PHI). Written before the action it records."""
    now = datetime.now(timezone.utc)
    key = f"partners/{partner}/{now:%Y%m%dT%H%M%S%f}-{event['event']}.json"
    body = {"partner": partner, "at": now.isoformat(), **event}
    client(s).put_object(Bucket=bucket(s), Key=key, Body=json.dumps(body, sort_keys=True).encode(),
                         ContentType="application/json")
    return key


def partner_events(s: Settings, partner: str) -> list[dict]:
    c, name, out = client(s), bucket(s), []
    for page in c.get_paginator("list_objects_v2").paginate(Bucket=name, Prefix=f"partners/{partner}/"):
        for obj in page.get("Contents", []):
            out.append(json.loads(c.get_object(Bucket=name, Key=obj["Key"])["Body"].read()))
    return sorted(out, key=lambda e: e["at"])
