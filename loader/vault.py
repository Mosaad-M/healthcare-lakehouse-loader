"""Key vault: one data key per subject.

Key hierarchy: each subject key is wrapped by its source's key-encryption key (KEK); each KEK is
wrapped by the master key (KMS on AWS) and kept in its own table. A batch unwraps its source's KEK
once (one KMS request, cached in the process for a few minutes) and its subjects' keys locally.
Subject keys from before the hierarchy (wrapped by the master key directly, no `kek_id`) stay
readable. KEKs are never deleted: erasure deletes subject keys, as before.

Deleting a subject's key is the erasure: every copy of that subject's identifiers, in every layer,
snapshot and backup of the lake, becomes unreadable at once.

Backups and the restore gate: the vault is backed up (losing it would make all PHI unreadable), so a
restore could bring shredded keys back. A restore therefore locks the vault, re-applies every entry
of the shred journal, and only then unlocks it; while locked, the vault serves no key.
"""
import base64
import json
import threading
import time

import botocore.exceptions

from loader import crypto
from loader.parallel import map_io

from loader.settings import Settings

KEY_VAULT, VAULT_STATE, KEKS = "key_vault", "vault_state", "key_encryption_keys"
KEK_VERSION = 1  # rotation would add version 2 and re-wrap subject keys in the background
KEK_CACHE_SECONDS = 900
_kek_cache: dict[str, tuple[bytes, float]] = {}
_kek_lock = threading.Lock()


def kek_id(source: str) -> str:
    return f"{source}#{KEK_VERSION}"
GATE = {"name": {"S": "restore"}}


class VaultLocked(RuntimeError):
    """A restore is in progress: the shred journal has not been re-applied yet."""


class KeyVault:
    def __init__(self, s: Settings, provider: crypto.KeyProvider | None = None):
        self.ddb = s.dynamodb()
        self.provider = provider or s.key_provider()

    def _check_gate(self) -> None:
        item = self.ddb.get_item(TableName=VAULT_STATE, Key=GATE, ConsistentRead=True).get("Item")
        if item and item["state"]["S"] != "open":
            raise VaultLocked("key vault restore pending: shred journal not yet replayed")

    # -- key-encryption keys
    def _kek(self, kid: str, create_for: str | None = None) -> bytes | None:
        """The unwrapped KEK `kid`: from this process's cache, the table, or (with `create_for`, the
        source) newly made. None if it does not exist and none was asked for."""
        now = time.monotonic()
        with _kek_lock:
            cached = _kek_cache.get(kid)
            if cached and cached[1] > now:
                return cached[0]
        item = self.ddb.get_item(TableName=KEKS, Key={"kek_id": {"S": kid}}, ConsistentRead=True).get("Item")
        if item is None and create_for:
            kek = crypto.new_data_key()
            try:
                self.ddb.put_item(TableName=KEKS, Item={
                    "kek_id": {"S": kid}, "source": {"S": create_for},
                    "wrapped_kek": {"B": self.provider.wrap(kek, f"kek:{kid}")}},
                    ConditionExpression="attribute_not_exists(kek_id)")
            except botocore.exceptions.ClientError as err:
                if err.response["Error"]["Code"] != "ConditionalCheckFailedException":
                    raise
                return self._kek(kid)  # another writer made it first
        elif item is None:
            return None
        else:
            kek = self.provider.unwrap(item["wrapped_kek"]["B"], f"kek:{kid}")
        with _kek_lock:
            _kek_cache[kid] = (kek, now + KEK_CACHE_SECONDS)
        return kek

    # -- subject keys
    def get(self, subject_ids: list[str]) -> dict[str, bytes]:
        """Data keys of the subjects that still have one (shredded subjects are absent)."""
        self._check_gate()
        items = {}
        unique = sorted({sid for sid in subject_ids if sid})  # rows without a subject have no key
        for i in range(0, len(unique), 100):
            request = {KEY_VAULT: {"Keys": [{"subject_id": {"S": sid}} for sid in unique[i:i + 100]],
                                   "ConsistentRead": True}}
            while request:
                resp = self.ddb.batch_get_item(RequestItems=request)
                for item in resp["Responses"].get(KEY_VAULT, []):
                    items[item["subject_id"]["S"]] = item
                request = resp.get("UnprocessedKeys") or None
        # Again after reading: a restore that started meanwhile may have written back shredded keys
        # (it locks the gate before it writes any).
        self._check_gate()
        out, legacy = {}, []
        for sid, item in items.items():
            if "kek_id" in item:
                kid = item["kek_id"]["S"]
                out[sid] = crypto.unwrap_with_kek(self._kek(kid), kid, sid, item["wrapped_key"]["B"])
            else:
                legacy.append(sid)
        # Keys from before the hierarchy: one master-key unwrap each (a KMS request on AWS).
        unwrap = lambda sid: self.provider.unwrap(items[sid]["wrapped_key"]["B"], sid)  # noqa: E731
        keys = map_io(unwrap, legacy) if getattr(self.provider, "remote", False) else [unwrap(sid) for sid in legacy]
        out.update(zip(legacy, keys))
        return out

    def get_or_create(self, subject_ids: list[str], source: str) -> dict[str, bytes]:
        """Keys of `subject_ids` (all subjects of `source`), creating the missing ones under the
        source's KEK."""
        keys = self.get(subject_ids)
        missing = sorted(set(subject_ids) - set(keys))
        if not missing:
            return keys
        kid = kek_id(source)
        kek = self._kek(kid, create_for=source)

        def create(sid: str) -> dict[str, bytes]:
            data_key = crypto.new_data_key()
            try:
                self.ddb.put_item(TableName=KEY_VAULT, Item={
                    "subject_id": {"S": sid}, "kek_id": {"S": kid},
                    "wrapped_key": {"B": crypto.wrap_with_kek(kek, kid, sid, data_key)}},
                    ConditionExpression="attribute_not_exists(subject_id)")
                return {sid: data_key}
            except botocore.exceptions.ClientError as err:
                if err.response["Error"]["Code"] != "ConditionalCheckFailedException":
                    raise
                return self.get([sid])  # created concurrently by another writer

        for created in map_io(create, missing):
            keys.update(created)
        return keys

    def delete(self, subject_ids: list[str]) -> None:
        for sid in subject_ids:
            self.ddb.delete_item(TableName=KEY_VAULT, Key={"subject_id": {"S": sid}})

    # -- backup and restore
    def _scan(self, table: str) -> list[dict]:
        items, kwargs = [], {"TableName": table, "ConsistentRead": True}
        while True:
            page = self.ddb.scan(**kwargs)
            items += page.get("Items", [])
            if "LastEvaluatedKey" not in page:
                return items
            kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]

    def backup(self) -> str:
        """Every wrapped subject key and KEK, as JSON (stands in for DynamoDB point-in-time recovery in
        this stack). Everything in it is wrapped: it is useless without the master key."""
        b64 = lambda v: base64.b64encode(v).decode()  # noqa: E731
        return json.dumps({
            "keks": [{"kek_id": i["kek_id"]["S"], "source": i["source"]["S"], "wrapped_kek": b64(i["wrapped_kek"]["B"])}
                     for i in self._scan(KEKS)],
            "keys": [{"subject_id": i["subject_id"]["S"], "wrapped_key": b64(i["wrapped_key"]["B"]),
                      **({"kek_id": i["kek_id"]["S"]} if "kek_id" in i else {})} for i in self._scan(KEY_VAULT)]})

    def restore(self, backup: str, shredded_subject_ids, before_replay=None) -> int:
        """Load a backup, re-apply the shred journal, then reopen the vault. Returns keys restored.

        `shredded_subject_ids` is a callable returning every subject ID in the shred journal.
        `before_replay` is a test hook called while the vault is locked.
        """
        self.ddb.put_item(TableName=VAULT_STATE, Item={**GATE, "state": {"S": "pending_replay"}})
        data = json.loads(backup)
        if isinstance(data, list):  # backups from before the key hierarchy: subject keys only
            data = {"keks": [], "keys": data}
        for kek in data["keks"]:
            self.ddb.put_item(TableName=KEKS, Item={
                "kek_id": {"S": kek["kek_id"]}, "source": {"S": kek["source"]},
                "wrapped_kek": {"B": base64.b64decode(kek["wrapped_kek"])}})
        restored = 0
        for item in data["keys"]:
            self.ddb.put_item(TableName=KEY_VAULT, Item={
                "subject_id": {"S": item["subject_id"]}, "wrapped_key": {"B": base64.b64decode(item["wrapped_key"])},
                **({"kek_id": {"S": item["kek_id"]}} if "kek_id" in item else {})})
            restored += 1
        if before_replay:
            before_replay()
        self.delete(sorted(shredded_subject_ids()))
        self.ddb.put_item(TableName=VAULT_STATE, Item={**GATE, "state": {"S": "open"}})
        return restored
