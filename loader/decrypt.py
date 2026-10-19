"""The audited re-identification path: the only way plaintext identifiers leave the system.

A caller names specific subjects, a role and a purpose. The call is checked against the policy in
stack/bootstrap/model.json (`decrypt_policy`): the role must exist, the purpose must be one the role
allows, and the number of subjects is capped per call. Every call, allowed or refused, is written to
the decrypt audit log first: who, which role, why, which subjects, columns and how many rows; never
values. Each event is first written to the write-once journal (Object Lock: it cannot be deleted or
rewritten), then to the queryable table; if the journal write fails, nothing is decrypted.
Shredded subjects come back as unreadable (None), never as an error that reveals anything.

Here the caller's identity is passed in; in production it comes from the authenticated session (SSO)
and this runs as a small service in trusted compute next to the key vault.
"""
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from loader import crypto, duck, journal, tables
from loader.contracts import Contract
from loader.settings import Settings
from loader.vault import KeyVault

AUDIT = "decrypt_audit"
POLICY = json.loads((Path(__file__).parents[1] / "stack/bootstrap/model.json").read_text())["decrypt_policy"]


class Refused(PermissionError):
    pass


@dataclass
class Result:
    event_id: str
    rows: list[dict]
    unreadable: int  # identifier values whose subject was shredded


def _audit(s: Settings, event: dict) -> None:
    journal.record_decrypt(s, event)  # write-once first: a failure stops the call before any decrypt
    item = {k: ({"N": str(v)} if isinstance(v, int) else {"SS": sorted(v)} if isinstance(v, list) and v
                else {"S": str(v)}) for k, v in event.items() if v not in (None, [])}
    s.dynamodb().put_item(TableName=AUDIT, Item=item)


def reidentify(s: Settings, caller: str, role: str, purpose: str, contract: Contract,
               subject_ids: list[str], columns: list[str] | None = None) -> Result:
    event = {"event_id": uuid.uuid4().hex, "at": datetime.now(timezone.utc).isoformat(), "caller": caller,
             "role": role, "purpose": purpose, "source": contract.source, "subject_ids": list(subject_ids),
             "status": "refused"}
    rules = POLICY["roles"].get(role)
    idents = [c.name for c in contract.identifiers]
    columns = columns or idents
    problem = (
        "unknown role" if rules is None else
        "purpose not allowed for this role" if purpose not in rules["purposes"] else
        f"more than {rules['max_subjects']} subjects in one call" if len(subject_ids) > rules["max_subjects"] else
        "no subjects named" if not subject_ids else
        "column is not an identifier of this source" if not set(columns) <= set(idents) else None)
    if problem:
        _audit(s, {**event, "reason": problem})
        raise Refused(problem)
    _audit(s, {**event, "status": "started", "columns": columns})

    con = duck.connect(s, tables.CATALOG, principal="decrypt_service")
    ids = ", ".join(duck.sql_str(i) for i in subject_ids)
    pk = contract.primary_key
    found = con.execute(f"SELECT subject_id, {', '.join(pk + columns)} FROM {tables.qualified(contract, tables.SILVER)} "
                        f"WHERE subject_id IN ({ids}) ORDER BY {', '.join(pk)}").fetchall()
    keys = KeyVault(s).get(subject_ids)
    rows, unreadable = [], 0
    for sid, *values in found:
        row = {"subject_id": sid, **dict(zip(pk, values[:len(pk)]))}
        for col, blob in zip(columns, values[len(pk):]):
            if blob is None:
                row[col] = None
            elif sid in keys:
                row[col] = crypto.decrypt(keys[sid], sid, col, bytes(blob))
            else:
                row[col], unreadable = None, unreadable + 1
        rows.append(row)
    _audit(s, {**event, "status": "done", "columns": columns, "rows": len(rows), "unreadable": unreadable})
    return Result(event["event_id"], rows, unreadable)
