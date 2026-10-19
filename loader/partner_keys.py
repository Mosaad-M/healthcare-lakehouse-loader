"""A master key per partner: it wraps the key-encryption keys (KEKs) of that partner's sources.

Disabling a partner's master key (a freeze) makes every KEK of the partner unwrappable, and so every
identifier of the partner unreadable, in every layer, snapshot, backup and copy, at once and
reversibly. Deleting it (only after an approved delete, loader/offboarding.py) makes that permanent.

- AWS: one KMS key per partner (Terraform, alias `<LOADER_PARTNER_KEY_ALIAS_PREFIX><partner>`). The
  loader's roles may use, disable and enable it; only the partner-deletion role may schedule its
  deletion (infra/sandbox/kms.tf).
- Locally: a random 32-byte key per partner in the state volume, with its state.

A wrapped KEK is bound to its partner and KEK ID (AES-GCM associated data locally; the KMS
encryption context on AWS), so it cannot be moved to another partner.
"""
import base64
import fcntl
import json
import os
import time
from contextlib import contextmanager

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

ENABLED, DISABLED, PENDING_DELETION, DELETED = "enabled", "disabled", "pending_deletion", "deleted"
VERSION, NONCE = b"\x01", 12


class Unavailable(RuntimeError):
    """The partner's master key is disabled, pending deletion or gone: its KEKs cannot be unwrapped."""


class LocalPartnerKeys:
    def __init__(self, state_dir):
        self.path = state_dir / "partner-keys.json"

    @contextmanager
    def _locked(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path.with_suffix(".lock"), "w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            keys = json.loads(self.path.read_text()) if self.path.exists() else {}
            yield keys
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(json.dumps(keys, indent=1))
            os.chmod(tmp, 0o600)
            tmp.replace(self.path)

    def ensure(self, partner: str) -> str:
        """The partner's key ID, creating the key if it has none. Returns the ID."""
        with self._locked() as keys:
            if partner not in keys:
                keys[partner] = {"key": base64.b64encode(AESGCM.generate_key(256)).decode(), "state": ENABLED,
                                 "key_id": f"local:{partner}"}
            return keys[partner]["key_id"]

    def state(self, partner: str) -> str:
        keys = json.loads(self.path.read_text()) if self.path.exists() else {}
        return keys[partner]["state"] if partner in keys else DELETED

    def key_id(self, partner: str) -> str:
        return f"local:{partner}"

    def _key(self, partner: str) -> bytes:
        keys = json.loads(self.path.read_text()) if self.path.exists() else {}
        entry = keys.get(partner)
        if not entry or entry["state"] != ENABLED:
            raise Unavailable(f"master key of partner {partner} is {entry['state'] if entry else DELETED}")
        return base64.b64decode(entry["key"])

    def wrap(self, partner: str, kek_id: str, kek: bytes) -> bytes:
        nonce = os.urandom(NONCE)
        return VERSION + nonce + AESGCM(self._key(partner)).encrypt(nonce, kek, _aad(partner, kek_id))

    def unwrap(self, partner: str, kek_id: str, wrapped: bytes) -> bytes:
        if wrapped[:1] != VERSION:
            raise ValueError("unknown wrapped KEK version")
        return AESGCM(self._key(partner)).decrypt(wrapped[1:1 + NONCE], wrapped[1 + NONCE:], _aad(partner, kek_id))

    def set_state(self, partner: str, state: str) -> None:
        """Disable or enable (freeze, unfreeze), or delete (an approved delete)."""
        with self._locked() as keys:
            if partner not in keys:
                raise KeyError(f"partner {partner} has no master key")
            if state == DELETED:
                del keys[partner]
            else:
                keys[partner]["state"] = state


class KmsPartnerKeys:
    def __init__(self, region: str, alias_prefix: str):
        import boto3

        self.kms, self.prefix = boto3.client("kms", region_name=region), alias_prefix

    def alias(self, partner: str) -> str:
        return f"{self.prefix}{partner}"

    def ensure(self, partner: str) -> str:
        """Terraform creates partner keys; a missing one is a deployment error."""
        return self.key_id(partner)

    def key_id(self, partner: str) -> str:
        return self.kms.describe_key(KeyId=self.alias(partner))["KeyMetadata"]["Arn"]

    def state(self, partner: str) -> str:
        try:
            meta = self.kms.describe_key(KeyId=self.alias(partner))["KeyMetadata"]
        except self.kms.exceptions.NotFoundException:
            return DELETED
        return {"Enabled": ENABLED, "Disabled": DISABLED, "PendingDeletion": PENDING_DELETION}.get(meta["KeyState"], DISABLED)

    def wrap(self, partner: str, kek_id: str, kek: bytes) -> bytes:
        return self.kms.encrypt(KeyId=self.alias(partner), Plaintext=kek, EncryptionContext=_context(partner, kek_id))["CiphertextBlob"]

    def unwrap(self, partner: str, kek_id: str, wrapped: bytes) -> bytes:
        try:
            return self.kms.decrypt(KeyId=self.alias(partner), CiphertextBlob=wrapped,
                                    EncryptionContext=_context(partner, kek_id))["Plaintext"]
        except (self.kms.exceptions.DisabledException, self.kms.exceptions.KMSInvalidStateException,
                self.kms.exceptions.NotFoundException) as err:
            raise Unavailable(f"master key of partner {partner}: {type(err).__name__}") from None

    def set_state(self, partner: str, state: str) -> None:
        """Disable or enable only. Scheduling deletion is not this identity's to do: the key policy
        reserves it for the partner-deletion role, assumed by an operator after approval."""
        key = self.key_id(partner)
        if state == DISABLED:
            self.kms.disable_key(KeyId=key)
        elif state == ENABLED:
            self.kms.enable_key(KeyId=key)
        else:
            raise PermissionError(f"the loader cannot set a partner key to {state}")


def _aad(partner: str, kek_id: str) -> bytes:
    return f"partner-kek\x00{partner}\x00{kek_id}".encode()


def _context(partner: str, kek_id: str) -> dict:
    return {"purpose": "partner-kek", "partner": partner, "kek_id": kek_id}


# Whether a partner's key is usable, cached briefly: a freeze reaches running processes within this
# long, and a batch does not ask KMS for the key state more than once in it.
STATE_TTL_SECONDS = 60
_state_cache: dict[str, tuple[str, float]] = {}


def usable(keys, partner: str) -> bool:
    now = time.monotonic()
    cached = _state_cache.get(partner)
    if not cached or cached[1] <= now:
        cached = _state_cache[partner] = (keys.state(partner), now + STATE_TTL_SECONDS)
    return cached[0] == ENABLED


def forget(partner: str) -> None:
    """Drop this process's cached state and KEKs for the partner (after a freeze, unfreeze or delete);
    other processes see the change within STATE_TTL_SECONDS."""
    from loader import vault

    _state_cache.pop(partner, None)
    with vault._kek_lock:
        for kid in [k for k, v in vault._kek_cache.items() if v[2] == partner]:
            vault._kek_cache.pop(kid, None)
