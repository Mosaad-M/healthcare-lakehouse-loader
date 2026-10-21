"""Erasing a person makes their identifiers unreadable everywhere, for them only.

Fresh sources per run: claims and lab results for the same synthetic people. The canary member is one
person with a subject in each source; linking makes them one person, and shredding that person
must leave Bronze, Silver and older snapshots unreadable for them while everyone else still decrypts.
"""
import uuid
from datetime import date

import pytest

from flows.ingest import ingest
from loader import crypto, decrypt, duck, journal, pipeline, raw, synthetic, tables
from loader.identity import Identity
from loader.shred import shred
from loader.state import Manifest
from loader.vault import KeyVault
from tests.conftest import scan_all
from tests.pipeline.conftest import deploy_copy, drop, remove_source


@pytest.fixture(scope="module")
def world(s, run_id, contracts_dir, canaries):
    claims = deploy_copy(s, contracts_dir, "acme_claims", f"erase_claims_{run_id}")
    labs = deploy_copy(s, contracts_dir, "lab_results", f"erase_labs_{run_id}")
    drop(s, claims, "claims.csv", synthetic.claims_csv(canaries, date(2026, 9, 1), days=3, per_day=7, members=7))
    drop(s, labs, "labs.ndjson", synthetic.lab_ndjson(canaries, 25, members=5))
    for c in (claims, labs):
        ingest(source=c.source, contracts_dir=str(contracts_dir))
    ids = Identity(s)
    canary_claims = ids.resolve(claims.source, [(canaries.member_id,)])[(canaries.member_id,)]
    canary_labs = ids.resolve(labs.source, [(canaries.mrn,)])[(canaries.mrn,)]
    other = synthetic.member(canaries, 1)
    other_claims = ids.resolve(claims.source, [(other["id"],)])[(other["id"],)]
    yield {"claims": claims, "labs": labs, "canary": [canary_claims, canary_labs], "other": other_claims,
           "backup": KeyVault(s).backup()}
    for c in (claims, labs):
        remove_source(s, c)


def silver_snapshot(s, contract) -> int:
    con = duck.connect(s, tables.CATALOG, principal=contract.principal)
    return con.execute(f"SELECT snapshot_id FROM iceberg_snapshots('{tables.qualified(contract, tables.SILVER)}') "
                       "ORDER BY timestamp_ms DESC LIMIT 1").fetchone()[0]


def ciphertext_of(s, contract, layer: str, subject_id: str, snapshot: int | None = None) -> list[bytes]:
    con = duck.connect(s, tables.CATALOG, principal=contract.principal)
    at = f" AT (VERSION => {snapshot})" if snapshot else ""
    col = contract.identifiers[-1].name
    rows = con.execute(f"SELECT {col} FROM {tables.qualified(contract, layer)}{at} "
                       f"WHERE subject_id = {duck.sql_str(subject_id)}").fetchall()
    return [bytes(r[0]) for r in rows]


def test_identifiers_are_stored_only_as_ciphertext_and_decrypt_through_the_audited_path(s, world, canaries):
    claims = world["claims"]
    blobs = ciphertext_of(s, claims, tables.SILVER, world["canary"][0])
    assert blobs and not any(canaries.name.encode() in b for b in blobs)
    result = decrypt.reidentify(s, caller="alice@example.org", role="care_coordinator", purpose="treatment",
                                contract=claims, subject_ids=[world["canary"][0]])
    assert {r["member_name"] for r in result.rows} == {canaries.name}
    assert result.unreadable == 0
    audit = s.dynamodb().get_item(TableName=decrypt.AUDIT, Key={"event_id": {"S": result.event_id}})["Item"]
    assert audit["status"]["S"] == "done" and audit["caller"]["S"] == "alice@example.org"
    assert int(audit["rows"]["N"]) == len(result.rows)
    assert not any(c in str(audit) for c in canaries.all()), "the audit log holds values"


@pytest.mark.parametrize("role, purpose, many, reason", [
    ("data_scientist", "research", False, "unknown role"),
    ("care_coordinator", "marketing", False, "purpose not allowed"),
    ("care_coordinator", "treatment", True, "more than 20 subjects"),
])
def test_decrypt_refuses_and_audits(s, world, role, purpose, many, reason):
    subjects = [uuid.uuid4().hex for _ in range(21)] if many else [world["other"]]
    with pytest.raises(decrypt.Refused, match=reason):
        decrypt.reidentify(s, caller="mallory@example.org", role=role, purpose=purpose,
                           contract=world["claims"], subject_ids=subjects)
    refused = [i for i in scan_all(s, decrypt.AUDIT)
               if i["caller"]["S"] == "mallory@example.org" and i["status"]["S"] == "refused"
               and reason.split()[0] in i["reason"]["S"]]
    assert refused


def test_shredding_a_person_makes_every_copy_unreadable_for_them_only(s, world, canaries, run_id):
    claims, labs = world["claims"], world["labs"]
    person = Identity(s).link(world["canary"])
    before = {c.source: silver_snapshot(s, c) for c in (claims, labs)}
    batches = [b for c in (claims, labs) for b in Manifest(s).for_source(c.source)]

    result = shred(s, request_id=f"req-{run_id}", actor="privacy-officer@example.org",
                   reason="patient_request", person_id=person)
    assert result.subject_ids == sorted(world["canary"])

    keys = KeyVault(s)
    assert keys.get(world["canary"]) == {}
    assert world["other"] in keys.get([world["other"]])
    # Ciphertext is still there in Bronze, Silver and the pre-shred snapshot, and nothing can read it.
    for contract, sid in ((claims, world["canary"][0]), (labs, world["canary"][1])):
        for layer, snap in ((tables.BRONZE, None), (tables.SILVER, None), (tables.SILVER, before[contract.source])):
            assert ciphertext_of(s, contract, layer, sid, snap), f"no rows found in {layer}"
    gone = decrypt.reidentify(s, "privacy-officer@example.org", "privacy_officer", "patient_request",
                              claims, [world["canary"][0], world["other"]])
    by_subject = {}
    for row in gone.rows:
        by_subject.setdefault(row["subject_id"], set()).add(row["member_name"])
    assert by_subject[world["canary"][0]] == {None} and gone.unreadable > 0
    assert None not in by_subject[world["other"]]

    # The identity map forgot them: the same identifiers now resolve to a new, unrelated subject.
    assert Identity(s).resolve(claims.source, [(canaries.member_id,)])[(canaries.member_id,)] not in world["canary"]
    # The raw files that mentioned them are gone, so a replay cannot bring them back.
    zone = raw.RawZone(s)
    for b in batches:
        assert Manifest(s).get(b.batch_id).raw_purged
        assert not zone.keys(b.raw_key)
    assert any(e["request_id"] == f"req-{run_id}" for e in journal.entries(s))


def test_replay_after_a_shred_keeps_the_person_unreadable(s, world):
    claims = world["claims"]
    assert pipeline.replay(s, claims) == 1
    assert KeyVault(s).get(world["canary"]) == {}
    assert ciphertext_of(s, claims, tables.SILVER, world["canary"][0])


def test_restoring_an_old_vault_backup_does_not_bring_keys_back(s, world):
    """The backup in `world` was taken before the shred."""
    vault = KeyVault(s)
    vault.restore(world["backup"], lambda: journal.shredded_subjects(s))
    assert vault.get(world["canary"]) == {}
    other = vault.get([world["other"]])[world["other"]]
    blob = ciphertext_of(s, world["claims"], tables.SILVER, world["other"])[0]
    assert crypto.decrypt(other, world["other"], world["claims"].identifiers[-1].name, blob)


def test_every_decrypt_event_is_kept_write_once(s, world, canaries):
    """Each status of a call is its own Object Lock entry: the history cannot be deleted or rewritten
    by the journal's writer (or anyone, until retention ends)."""
    import botocore.exceptions

    result = decrypt.reidentify(s, caller="audit@example.org", role="care_coordinator", purpose="treatment",
                                contract=world["claims"], subject_ids=[world["other"]])
    history = journal.decrypt_events(s, result.event_id)
    assert [e["status"] for e in sorted(history, key=lambda e: ("started", "done").index(e["status"]))] == ["started", "done"]
    assert all(e["caller"] == "audit@example.org" and e["subject_ids"] == [world["other"]] for e in history)
    assert not any(c in str(history) for c in canaries.all()), "the audit trail holds values"
    c = journal.client(s)
    key = c.list_objects_v2(Bucket=journal.bucket(s), Prefix="decrypt-audit/")["Contents"][0]["Key"]
    version = c.head_object(Bucket=journal.bucket(s), Key=key).get("VersionId")
    with pytest.raises(botocore.exceptions.ClientError):
        c.delete_object(Bucket=journal.bucket(s), Key=key, **({"VersionId": version} if version else {}))


def test_no_decrypt_without_the_write_once_audit(s, world, monkeypatch):
    def unavailable(*_):
        raise RuntimeError("journal unavailable")

    monkeypatch.setattr(journal, "record_decrypt", unavailable)
    monkeypatch.setattr(KeyVault, "get", lambda *_: pytest.fail("decrypted without an audit entry"))
    with pytest.raises(RuntimeError, match="journal unavailable"):
        decrypt.reidentify(s, caller="audit@example.org", role="care_coordinator", purpose="treatment",
                           contract=world["claims"], subject_ids=[world["other"]])
