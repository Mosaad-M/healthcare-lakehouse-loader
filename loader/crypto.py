"""Envelope encryption for identifiers.

- A master key (KMS on AWS; a local key from the environment locally) wraps one data key per
  subject. Wrapped keys live in the key vault; the master key never does.
- Every identifier value is encrypted with its subject's data key: AES-256-GCM, a fresh 96-bit nonce,
  and associated data binding the ciphertext to its subject and column, so a value copied to another
  subject's row or another column fails to decrypt.

Ciphertext layout: version byte (1) | nonce (12) | ciphertext and tag.
"""
import base64
import os
from typing import Protocol

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

VERSION = b"\x01"
NONCE = 12


class KeyProvider(Protocol):
    """Wraps and unwraps data keys. Implementations: LocalKeyProvider (local stack), KMS (AWS)."""

    def wrap(self, data_key: bytes, context: str) -> bytes: ...

    def unwrap(self, wrapped: bytes, context: str) -> bytes: ...


class LocalKeyProvider:
    """Master key from LOADER_MASTER_KEY (base64, 32 bytes). Development only: on AWS the master
    key stays inside KMS and this becomes kms:Encrypt / kms:Decrypt with an encryption context."""

    remote = False  # wrapping is a few microseconds of CPU here: threads would only add overhead

    def __init__(self, master_key_b64: str | None = None):
        raw = base64.b64decode(master_key_b64 or os.environ["LOADER_MASTER_KEY"])
        if len(raw) != 32:
            raise ValueError("LOADER_MASTER_KEY must be 32 bytes (base64)")
        self._aead = AESGCM(raw)

    def wrap(self, data_key: bytes, context: str) -> bytes:
        nonce = os.urandom(NONCE)
        return VERSION + nonce + self._aead.encrypt(nonce, data_key, context.encode())

    def unwrap(self, wrapped: bytes, context: str) -> bytes:
        if wrapped[:1] != VERSION:
            raise ValueError("unknown wrapped key version")
        return self._aead.decrypt(wrapped[1:1 + NONCE], wrapped[1 + NONCE:], context.encode())


class KmsKeyProvider:
    """The master key stays in AWS KMS; data keys are wrapped with kms:Encrypt and an encryption
    context naming the subject, so a wrapped key only unwraps for its own subject (and every unwrap
    is in CloudTrail)."""

    remote = True  # every wrap and unwrap is a KMS request: run them concurrently

    def __init__(self, key_id: str, region: str):
        import boto3

        self.key_id, self._kms = key_id, boto3.client("kms", region_name=region)

    def wrap(self, data_key: bytes, context: str) -> bytes:
        return self._kms.encrypt(KeyId=self.key_id, Plaintext=data_key,
                                 EncryptionContext={"subject_id": context})["CiphertextBlob"]

    def unwrap(self, wrapped: bytes, context: str) -> bytes:
        return self._kms.decrypt(KeyId=self.key_id, CiphertextBlob=wrapped,
                                 EncryptionContext={"subject_id": context})["Plaintext"]


# Key hierarchy: subject data keys are wrapped by their source's key-encryption key (KEK),
# and only the KEK by the master key (KMS on AWS). A batch then makes one KMS request instead of one
# per subject (KMS was about $1,300 of $1,900 a month at the benchmark target; see the README).
def wrap_with_kek(kek: bytes, kek_id: str, subject_id: str, data_key: bytes) -> bytes:
    nonce = os.urandom(NONCE)
    return VERSION + nonce + AESGCM(kek).encrypt(nonce, data_key, _kek_aad(kek_id, subject_id))


def unwrap_with_kek(kek: bytes, kek_id: str, subject_id: str, wrapped: bytes) -> bytes:
    if wrapped[:1] != VERSION:
        raise ValueError("unknown wrapped key version")
    return AESGCM(kek).decrypt(wrapped[1:1 + NONCE], wrapped[1 + NONCE:], _kek_aad(kek_id, subject_id))


def _kek_aad(kek_id: str, subject_id: str) -> bytes:
    """A wrapped subject key opens only under its own KEK and for its own subject."""
    return f"subject-key\x00{kek_id}\x00{subject_id}".encode()


def new_data_key() -> bytes:
    return AESGCM.generate_key(bit_length=256)


def associated_data(subject_id: str, column: str) -> bytes:
    return f"{subject_id}\x00{column}".encode()


class SubjectCipher:
    """Encrypts many values for one subject: one key schedule and one AAD per column, instead of one
    per value (about 1.7x faster). Each value still gets its own random nonce from os.urandom: a
    pooled nonce buffer would be faster still, but could repeat nonces across a fork."""

    def __init__(self, data_key: bytes, subject_id: str):
        self._aead, self._subject_id, self._aad = AESGCM(data_key), subject_id, {}

    def encrypt(self, column: str, plaintext: str) -> bytes:
        aad = self._aad.get(column)
        if aad is None:
            aad = self._aad[column] = associated_data(self._subject_id, column)
        nonce = os.urandom(NONCE)
        return VERSION + nonce + self._aead.encrypt(nonce, plaintext.encode(), aad)


def encrypt(data_key: bytes, subject_id: str, column: str, plaintext: str) -> bytes:
    nonce = os.urandom(NONCE)
    return VERSION + nonce + AESGCM(data_key).encrypt(nonce, plaintext.encode(), associated_data(subject_id, column))


def decrypt(data_key: bytes, subject_id: str, column: str, ciphertext: bytes) -> str:
    if ciphertext[:1] != VERSION:
        raise ValueError("unknown ciphertext version")
    return AESGCM(data_key).decrypt(ciphertext[1:1 + NONCE], ciphertext[1 + NONCE:],
                                    associated_data(subject_id, column)).decode()
