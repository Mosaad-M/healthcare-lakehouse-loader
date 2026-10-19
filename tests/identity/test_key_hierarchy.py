"""Key hierarchy: subject keys are wrapped by their source's KEK, the KEK by the master key, so a
batch makes one master-key (KMS) request instead of one per subject."""
import uuid

import pytest

from loader import crypto, vault
from loader.vault import KEKS, KEY_VAULT, KeyVault


class Counting:
    """The configured key provider, counting master-key operations (each one a KMS request on AWS)."""

    def __init__(self, inner):
        self.inner, self.wraps, self.unwraps = inner, 0, 0
        self.remote = getattr(inner, "remote", False)

    def wrap(self, key, context):
        self.wraps += 1
        return self.inner.wrap(key, context)

    def unwrap(self, wrapped, context):
        self.unwraps += 1
        return self.inner.unwrap(wrapped, context)


@pytest.fixture
def source(s, run_id):
    name = f"kektest_{run_id}_{uuid.uuid4().hex[:4]}"
    yield name
    s.dynamodb().delete_item(TableName=KEKS, Key={"kek_id": {"S": vault.kek_id(name)}})


def fresh_process():
    vault._kek_cache.clear()  # as a new task would start


def test_a_batch_makes_one_master_key_request(s, source):
    provider = Counting(s.key_provider())
    sids = [uuid.uuid4().hex for _ in range(200)]
    fresh_process()
    created = KeyVault(s, provider).get_or_create(sids, source)
    assert (provider.wraps, provider.unwraps) == (1, 0)  # the source's KEK, once
    fresh_process()
    provider.wraps = 0
    assert KeyVault(s, provider).get(sids) == created
    assert (provider.wraps, provider.unwraps) == (0, 1)
    item = s.dynamodb().get_item(TableName=KEY_VAULT, Key={"subject_id": {"S": sids[0]}})["Item"]
    assert item["kek_id"]["S"] == vault.kek_id(source)


def test_keys_from_before_the_hierarchy_stay_readable(s):
    provider = s.key_provider()
    sid, key = uuid.uuid4().hex, crypto.new_data_key()
    s.dynamodb().put_item(TableName=KEY_VAULT, Item={"subject_id": {"S": sid}, "wrapped_key": {"B": provider.wrap(key, sid)}})
    assert KeyVault(s).get([sid]) == {sid: key}
    KeyVault(s).delete([sid])


def test_a_restore_brings_back_the_keks(s, source):
    v = KeyVault(s)
    sid = uuid.uuid4().hex
    original = v.get_or_create([sid], source)
    backup = v.backup()
    s.dynamodb().delete_item(TableName=KEKS, Key={"kek_id": {"S": vault.kek_id(source)}})
    fresh_process()
    with pytest.raises(Exception):
        v.get([sid])  # without its KEK the key cannot be unwrapped
    v.restore(backup, lambda: [])
    fresh_process()
    assert v.get([sid]) == original
