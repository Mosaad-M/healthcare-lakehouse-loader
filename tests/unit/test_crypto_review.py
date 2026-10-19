"""Properties from the crypto review (docs/SECURITY.md)."""
import base64
import os

import pytest
from cryptography.exceptions import InvalidTag

from loader import crypto, identity, vault


def test_nonces_do_not_repeat_at_volume():
    """Random 96-bit nonces: 200,000 values under one key, no repeat (the review's bound: far below
    2^32 messages per key, the NIST limit for random nonces)."""
    cipher = crypto.SubjectCipher(crypto.new_data_key(), "subject")
    nonces = {cipher.encrypt("name", "x")[1:1 + crypto.NONCE] for _ in range(200_000)}
    assert len(nonces) == 200_000


def test_nonces_differ_across_a_fork():
    cipher = crypto.SubjectCipher(crypto.new_data_key(), "subject")
    read, write = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.write(write, cipher.encrypt("name", "x")[1:1 + crypto.NONCE])
        os._exit(0)
    os.waitpid(pid, 0)
    assert os.read(read, crypto.NONCE) != cipher.encrypt("name", "x")[1:1 + crypto.NONCE]


@pytest.mark.parametrize("subject, column", [("other", "name"), ("subject", "other_column")])
def test_a_value_moved_to_another_subject_or_column_does_not_decrypt(subject, column):
    key = crypto.new_data_key()
    blob = crypto.encrypt(key, "subject", "name", "Jane Synthetic")
    with pytest.raises(InvalidTag):
        crypto.decrypt(key, subject, column, blob)


def test_a_wrapped_key_opens_only_under_its_kek_and_for_its_subject():
    kek, data_key = crypto.new_data_key(), crypto.new_data_key()
    wrapped = crypto.wrap_with_kek(kek, "src#1", "subject", data_key)
    assert crypto.unwrap_with_kek(kek, "src#1", "subject", wrapped) == data_key
    for kek_id, subject in (("src#2", "subject"), ("src#1", "other")):
        with pytest.raises(InvalidTag):
            crypto.unwrap_with_kek(kek, kek_id, subject, wrapped)


def test_the_local_master_key_checks_the_version():
    provider = crypto.LocalKeyProvider(base64.b64encode(os.urandom(32)).decode())
    wrapped = provider.wrap(b"k" * 32, "kek:src#1")
    assert provider.unwrap(wrapped, "kek:src#1") == b"k" * 32
    with pytest.raises(ValueError, match="version"):
        provider.unwrap(b"\x02" + wrapped[1:], "kek:src#1")


def test_blind_index_fields_cannot_run_together():
    """The separator (\\x1e) is whitespace to str.split, so normalize() removes it from values: two
    fields can never read as one."""
    pepper = os.urandom(32)
    assert identity.blind_index(pepper, "src", ("a\x1eb",)) != identity.blind_index(pepper, "src", ("a", "b"))
    assert identity.blind_index(pepper, "src", ("A B",)) == identity.blind_index(pepper, "src", (" a  b ",))


class GateFlipsDuringRead:
    """A DynamoDB stand-in: the gate is open when checked, and a restore locks it while keys are read."""

    def __init__(self):
        self.state = "open"

    def get_item(self, TableName, Key, ConsistentRead):
        return {"Item": {"name": {"S": "restore"}, "state": {"S": self.state}}}

    def batch_get_item(self, RequestItems):
        self.state = "pending_replay"  # the restore started and wrote keys back
        return {"Responses": {vault.KEY_VAULT: [{"subject_id": {"S": "s1"}, "wrapped_key": {"B": b"x"}}]}}


def test_a_restore_that_starts_during_a_read_serves_no_keys():
    v = vault.KeyVault.__new__(vault.KeyVault)
    v.ddb, v.provider = GateFlipsDuringRead(), None
    with pytest.raises(vault.VaultLocked):
        v.get(["s1"])
