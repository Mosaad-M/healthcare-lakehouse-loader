"""Identity service and key vault against DynamoDB."""
import threading
import uuid

import pytest

from loader import journal
from loader.identity import Identity
from loader.vault import KeyVault, VaultLocked


@pytest.fixture
def source(run_id):
    return f"idtest_{run_id}_{uuid.uuid4().hex[:4]}"


def test_same_identifiers_find_the_same_subject(s, source):
    ids = Identity(s)
    first = ids.resolve(source, [("MRN-001",), ("MRN-002",)])
    again = ids.resolve(source, [(" mrn-001 ",), ("MRN-002",)])
    assert first[("MRN-001",)] == again[(" mrn-001 ",)]
    assert first[("MRN-001",)] != first[("MRN-002",)]
    assert ids.resolve(f"{source}_other", [("MRN-001",)])[("MRN-001",)] != first[("MRN-001",)]


def test_concurrent_first_sightings_create_one_subject(s, source):
    results, ids = [], Identity(s)
    threads = [threading.Thread(target=lambda: results.append(ids.resolve(source, [("NEW-1",)])[("NEW-1",)]))
               for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(set(results)) == 1


def test_link_and_split_change_only_person_links(s, source):
    ids = Identity(s)
    a = ids.resolve(source, [("A",)])[("A",)]
    b = ids.resolve(f"{source}_lab", [("B",)])[("B",)]
    assert ids.person_of(a) != ids.person_of(b)
    person = ids.link([a, b])
    assert ids.person_of(a) == ids.person_of(b) == person
    assert ids.subjects_of(person) == sorted([a, b])
    new = ids.split(b)
    assert ids.subjects_of(person) == [a] and ids.subjects_of(new) == [b]


def test_forgotten_subjects_are_not_found_again(s, source):
    ids = Identity(s)
    sid = ids.resolve(source, [("GONE",)])[("GONE",)]
    ids.forget([sid])
    assert ids.person_of(sid) is None
    assert ids.resolve(source, [("GONE",)])[("GONE",)] != sid  # a new, unrelated subject


def test_vault_restore_reapplies_the_journal_before_serving_keys(s, run_id):
    vault = KeyVault(s)
    keep, shredded = uuid.uuid4().hex, uuid.uuid4().hex
    original = vault.get_or_create([keep, shredded], f"restoretest_{run_id}")
    backup = vault.backup()  # taken before the shred, as a nightly backup would be
    journal.append(s, f"test-{run_id}-{uuid.uuid4().hex[:6]}", [shredded], actor="test", reason="test")
    vault.delete([shredded])

    locked = []

    def during_restore():
        with pytest.raises(VaultLocked):
            vault.get([keep])
        locked.append(True)

    assert vault.restore(backup, lambda: journal.shredded_subjects(s), before_replay=during_restore) >= 2
    assert locked, "the vault was not locked while the journal was replayed"
    keys = vault.get([keep, shredded])
    assert keys == {keep: original[keep]}, "a restore brought a shredded key back"


def test_journal_entries_cannot_be_deleted(s, run_id):
    import botocore.exceptions

    key = journal.append(s, f"test-{run_id}-{uuid.uuid4().hex[:6]}", [uuid.uuid4().hex], actor="test", reason="test")
    name = journal.bucket(s)
    admin = s.s3("admin")
    version = admin.list_object_versions(Bucket=name, Prefix=key)["Versions"][0]["VersionId"]
    # Neither the journal's own writer nor the storage administrator can remove the entry
    # (locally both are distinct identities; on AWS the task role, which cannot bypass the lock).
    for who in ("shred-journal", "admin"):
        with pytest.raises(botocore.exceptions.ClientError, match="AccessDenied"):
            s.s3(who).delete_object(Bucket=name, Key=key, VersionId=version)
    assert admin.get_object(Bucket=name, Key=key)["Body"].read()
