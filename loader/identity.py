"""Identity service: source identifiers -> per-source subject IDs -> person IDs.

- A subject is one source identity (Acme member 123). Its ID is random and is never merged.
- Identifiers resolve to a subject through a blind index: HMAC-SHA256 with a secret pepper over the
  source name and the normalized key values. Without the pepper the index reveals nothing; with it,
  the same identifiers always find the same subject.
- A person groups subjects believed to be the same human. Linking and splitting only edit these
  links, never lake data or keys.

Nothing here is stored in the lake; the tables live in DynamoDB next to the key vault.
"""
import base64
import hashlib
import hmac
import os
import uuid

import botocore.exceptions

from loader.parallel import map_io
from loader.settings import Settings

IDENTITY_MAP, SUBJECTS, PERSONS = "identity_map", "subjects", "persons"


def normalize(value: str) -> str:
    """Identifiers compare case-insensitively and without surrounding whitespace."""
    return " ".join(str(value).split()).upper()


def blind_index(pepper: bytes, source: str, key: tuple) -> str:
    message = "\x1e".join([source, *(normalize(v) for v in key)]).encode()
    return hmac.new(pepper, message, hashlib.sha256).hexdigest()


class Identity:
    def __init__(self, s: Settings, pepper_b64: str | None = None):
        self.ddb = s.dynamodb()
        self.pepper = base64.b64decode(pepper_b64 or os.environ["LOADER_IDENTITY_PEPPER"])

    # -- resolution
    def resolve(self, source: str, keys: list[tuple]) -> dict[tuple, str]:
        """Subject ID for every distinct key, creating subjects (each with its own person) as needed."""
        indexes = {key: blind_index(self.pepper, source, key) for key in set(keys)}
        found = self._batch_get(IDENTITY_MAP, "blind_index", list(set(indexes.values())))
        out = {key: found[index]["subject_id"]["S"] for key, index in indexes.items() if index in found}
        missing = [(key, index) for key, index in indexes.items() if index not in found]
        for (key, _), subject_id in zip(missing, map_io(lambda item: self._create(source, item[1]), missing)):
            out[key] = subject_id
        return out

    def _create(self, source: str, index: str) -> str:
        subject_id, person_id = uuid.uuid4().hex, uuid.uuid4().hex
        # The subject record (with its index, for shredding) exists before the index points at it.
        self.ddb.put_item(TableName=SUBJECTS, Item={
            "subject_id": {"S": subject_id}, "source": {"S": source}, "person_id": {"S": person_id},
            "blind_index": {"S": index}})
        try:
            self.ddb.put_item(TableName=IDENTITY_MAP, Item={
                "blind_index": {"S": index}, "subject_id": {"S": subject_id}, "source": {"S": source}},
                ConditionExpression="attribute_not_exists(blind_index)")
        except botocore.exceptions.ClientError as err:
            if err.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
            # Another writer created this subject first: use theirs.
            self.ddb.delete_item(TableName=SUBJECTS, Key={"subject_id": {"S": subject_id}})
            item = self.ddb.get_item(TableName=IDENTITY_MAP, Key={"blind_index": {"S": index}},
                                     ConsistentRead=True)["Item"]
            return item["subject_id"]["S"]
        self.ddb.put_item(TableName=PERSONS, Item={"person_id": {"S": person_id}, "subject_ids": {"SS": [subject_id]}})
        return subject_id

    # -- people
    def person_of(self, subject_id: str) -> str | None:
        item = self.ddb.get_item(TableName=SUBJECTS, Key={"subject_id": {"S": subject_id}}, ConsistentRead=True).get("Item")
        return item["person_id"]["S"] if item else None

    def subjects_of(self, person_id: str) -> list[str]:
        item = self.ddb.get_item(TableName=PERSONS, Key={"person_id": {"S": person_id}}, ConsistentRead=True).get("Item")
        return sorted(item["subject_ids"]["SS"]) if item else []

    def link(self, subject_ids: list[str]) -> str:
        """Make these subjects one person (the first subject's person). Returns that person ID."""
        target = self.person_of(subject_ids[0])
        members = set(self.subjects_of(target))
        for sid in subject_ids[1:]:
            old = self.person_of(sid)
            if old and old != target:
                members |= set(self.subjects_of(old))
                self.ddb.delete_item(TableName=PERSONS, Key={"person_id": {"S": old}})
        for sid in members:
            self._set_person(sid, target)
        self.ddb.put_item(TableName=PERSONS, Item={"person_id": {"S": target}, "subject_ids": {"SS": sorted(members)}})
        return target

    def split(self, subject_id: str) -> str:
        """Detach one subject into a person of its own. Returns the new person ID."""
        old = self.person_of(subject_id)
        rest = [s for s in self.subjects_of(old) if s != subject_id]
        new = uuid.uuid4().hex
        self._set_person(subject_id, new)
        self.ddb.put_item(TableName=PERSONS, Item={"person_id": {"S": new}, "subject_ids": {"SS": [subject_id]}})
        if rest:
            self.ddb.put_item(TableName=PERSONS, Item={"person_id": {"S": old}, "subject_ids": {"SS": rest}})
        else:
            self.ddb.delete_item(TableName=PERSONS, Key={"person_id": {"S": old}})
        return new

    def _set_person(self, subject_id: str, person_id: str) -> None:
        self.ddb.update_item(TableName=SUBJECTS, Key={"subject_id": {"S": subject_id}},
                             UpdateExpression="SET person_id = :p", ExpressionAttributeValues={":p": {"S": person_id}})

    # -- erasure
    def forget(self, subject_ids: list[str]) -> None:
        """Remove subjects, their blind indexes and their person links (part of a shred)."""
        for sid in subject_ids:
            item = self.ddb.get_item(TableName=SUBJECTS, Key={"subject_id": {"S": sid}}, ConsistentRead=True).get("Item")
            if not item:
                continue
            self.ddb.delete_item(TableName=IDENTITY_MAP, Key={"blind_index": item["blind_index"]})
            person = item["person_id"]["S"]
            rest = [s for s in self.subjects_of(person) if s != sid]
            if rest:
                self.ddb.put_item(TableName=PERSONS, Item={"person_id": {"S": person}, "subject_ids": {"SS": rest}})
            else:
                self.ddb.delete_item(TableName=PERSONS, Key={"person_id": {"S": person}})
            self.ddb.delete_item(TableName=SUBJECTS, Key={"subject_id": {"S": sid}})

    def _batch_get(self, table: str, key: str, values: list[str]) -> dict[str, dict]:
        out = {}
        for i in range(0, len(values), 100):
            request = {table: {"Keys": [{key: {"S": v}} for v in values[i:i + 100]], "ConsistentRead": True}}
            while request:
                resp = self.ddb.batch_get_item(RequestItems=request)
                for item in resp["Responses"].get(table, []):
                    out[item[key]["S"]] = item
                request = resp.get("UnprocessedKeys") or None
        return out
