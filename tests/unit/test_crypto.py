"""Envelope encryption: values bound to their subject and column; keys bound to their subject."""
import base64
import os

import pytest
from cryptography.exceptions import InvalidTag

from loader import crypto
from loader.identity import blind_index, normalize

SID, OTHER = "a" * 32, "b" * 32


def test_value_round_trip_and_fresh_nonces():
    key = crypto.new_data_key()
    one, two = crypto.encrypt(key, SID, "member_name", "Avery Fakeworth"), crypto.encrypt(key, SID, "member_name", "Avery Fakeworth")
    assert one != two  # randomized: equal plaintexts do not produce equal ciphertexts
    assert crypto.decrypt(key, SID, "member_name", one) == "Avery Fakeworth"
    assert b"Avery" not in one


@pytest.mark.parametrize("subject, column, key_changes", [
    (OTHER, "member_name", False),  # copied to another subject's row
    (SID, "member_id", False),      # copied to another column
    (SID, "member_name", True),     # another subject's key
])
def test_ciphertext_does_not_decrypt_out_of_place(subject, column, key_changes):
    key = crypto.new_data_key()
    blob = crypto.encrypt(key, SID, "member_name", "Avery Fakeworth")
    with pytest.raises(InvalidTag):
        crypto.decrypt(crypto.new_data_key() if key_changes else key, subject, column, blob)


def test_tampering_is_detected():
    key = crypto.new_data_key()
    blob = bytearray(crypto.encrypt(key, SID, "c", "x"))
    blob[-1] ^= 1
    with pytest.raises(InvalidTag):
        crypto.decrypt(key, SID, "c", bytes(blob))


def test_wrapped_keys_are_bound_to_their_subject():
    provider = crypto.LocalKeyProvider(base64.b64encode(os.urandom(32)).decode())
    data_key = crypto.new_data_key()
    wrapped = provider.wrap(data_key, SID)
    assert provider.unwrap(wrapped, SID) == data_key
    with pytest.raises(InvalidTag):
        provider.unwrap(wrapped, OTHER)
    with pytest.raises(InvalidTag):
        crypto.LocalKeyProvider(base64.b64encode(os.urandom(32)).decode()).unwrap(wrapped, SID)


def test_blind_index_is_keyed_normalized_and_per_source():
    pepper = os.urandom(32)
    assert blind_index(pepper, "acme", (" mrn-001 ",)) == blind_index(pepper, "acme", ("MRN-001",))
    assert blind_index(pepper, "acme", ("MRN-001",)) != blind_index(pepper, "lab", ("MRN-001",))
    assert blind_index(pepper, "acme", ("MRN-001",)) != blind_index(os.urandom(32), "acme", ("MRN-001",))
    assert normalize("  Avery   Fakeworth ") == "AVERY FAKEWORTH"


def test_a_subject_cipher_matches_the_one_value_format():
    """The batch path (one key schedule per subject) writes what decrypt() reads, AAD included."""
    key = crypto.new_data_key()
    cipher = crypto.SubjectCipher(key, "subject-1")
    blobs = [cipher.encrypt("member_name", f"Synthetic Member {i}") for i in range(1000)]
    assert [crypto.decrypt(key, "subject-1", "member_name", b) for b in blobs] == [f"Synthetic Member {i}" for i in range(1000)]
    assert len({b[1:1 + crypto.NONCE] for b in blobs}) == 1000  # a fresh nonce every time
    with pytest.raises(InvalidTag):  # bound to its subject and column
        crypto.decrypt(key, "subject-2", "member_name", blobs[0])
    with pytest.raises(InvalidTag):
        crypto.decrypt(key, "subject-1", "member_id", blobs[0])


def test_a_kek_wrapped_key_opens_only_for_its_subject_and_kek():
    kek, key = crypto.new_data_key(), crypto.new_data_key()
    wrapped = crypto.wrap_with_kek(kek, "src#1", "subject-1", key)
    assert crypto.unwrap_with_kek(kek, "src#1", "subject-1", wrapped) == key
    for kek_id, subject in (("src#1", "subject-2"), ("other#1", "subject-1")):
        with pytest.raises(InvalidTag):
            crypto.unwrap_with_kek(kek, kek_id, subject, wrapped)
    with pytest.raises(InvalidTag):
        crypto.unwrap_with_kek(crypto.new_data_key(), "src#1", "subject-1", wrapped)
