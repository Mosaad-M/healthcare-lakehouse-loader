"""Per-partner isolation and offboarding, end to end: each partner's master key wraps only its own
KEKs; its drop identity reaches only its own drop prefixes; a freeze makes its identifiers unreadable
and stops its loads, reversibly; the report inventories what a delete would remove; a delete needs a
freeze, two approvers and the approved report, removes everything in the report, resumes after an
interruption, and leaves other partners untouched."""
import json
import uuid
from datetime import date

import boto3
import botocore.exceptions
import pytest
from cryptography.exceptions import InvalidTag

from loader import (admin, contracts, decrypt, duck, journal, offboarding, partner_keys, pipeline, raw, synthetic,
                    tables)
from loader.state import SILVER_COMMITTED, Manifest
from loader.vault import KEKS, KeyVault
from tests.pipeline.conftest import deploy_copy, drop, remove_source

# Throwaway partners need their master keys made on the fly, which only the local stack does (on AWS a
# partner's KMS key comes from Terraform); tests/aws/test_partner_keys.py checks the AWS side.
pytestmark = pytest.mark.local


@pytest.fixture
def partners(s, contracts_dir, canaries):
    """Two fresh partners, each with one loaded claims source."""
    tag = uuid.uuid4().hex[:6]
    made = {}
    for name in ("a", "b"):
        partner = f"part{name}_{tag}"
        contract = deploy_copy(s, contracts_dir, "acme_claims", f"{partner}_claims", partner=partner)
        drop(s, contract, "claims.csv", synthetic.claims_csv(canaries, date(2026, 6, 1), days=2, per_day=5, seed=3))
        pipeline.ingest(s, contract)
        made[name] = contract
    yield made["a"], made["b"]
    for contract in made.values():
        remove_source(s, contract)
        s.dynamodb().delete_item(TableName=offboarding.PARTNERS, Key={"partner": {"S": contract.partner}})
        try:
            s.partner_keys().set_state(contract.partner, partner_keys.DELETED)
        except KeyError:
            pass
        admin.rustfs_admin(s, "DELETE", f"/remove-user?accessKey={offboarding.inbound_user(contract.partner)}")
        partner_keys.forget(contract.partner)


def subjects(s, contract) -> list[str]:
    con = duck.connect(s, tables.CATALOG, principal=contract.principal)
    return [r[0] for r in con.execute(f"SELECT DISTINCT subject_id FROM {tables.qualified(contract, tables.SILVER)}").fetchall()]


def readable(s, contract) -> int:
    """How many identifier values of the contract's subjects decrypt (the audited path)."""
    sids = subjects(s, contract)[:20]
    result = decrypt.reidentify(s, "test@example.org", "care_coordinator", "treatment", contract, sids)
    return sum(1 for row in result.rows for c in contract.identifiers if row.get(c.name) is not None)


def denied(fn) -> bool:
    try:
        fn()
        return False
    except botocore.exceptions.ClientError as err:
        return err.response["Error"]["Code"] in ("AccessDenied", "403", "InvalidAccessKeyId")


def inbound(s, partner):
    key, secret = s.storage_user(offboarding.inbound_user(partner))
    return boto3.client("s3", endpoint_url=s.s3_endpoint, region_name=s.region, aws_access_key_id=key,
                        aws_secret_access_key=secret)


def test_each_partner_key_wraps_only_its_own_keks(s, partners):
    a, b = partners
    keks = {i["source"]["S"]: i for i in offboarding._scan(s, KEKS)}
    assert keks[a.source]["partner"]["S"] == a.partner and keks[b.source]["partner"]["S"] == b.partner
    keys = s.partner_keys()
    kid = keks[a.source]["kek_id"]["S"]
    with pytest.raises(InvalidTag):  # bound to its partner: another partner's key cannot open it
        keys.unwrap(b.partner, kid, keks[a.source]["wrapped_kek"]["B"])


def test_a_partner_can_only_drop_into_its_own_sources(s, partners):
    a, b = partners
    client, bucket = inbound(s, a.partner), s.bucket(raw.LOGICAL)
    client.put_object(Bucket=bucket, Key=f"{raw.drop_prefix(a)}from-a.csv", Body=b"x")
    assert denied(lambda: client.put_object(Bucket=bucket, Key=f"{raw.drop_prefix(b)}from-a.csv", Body=b"x"))
    assert denied(lambda: client.get_object(Bucket=bucket, Key=f"{raw.drop_prefix(a)}from-a.csv"))
    raw.RawZone(s).delete(f"{raw.drop_prefix(a)}from-a.csv")


def test_a_freeze_stops_everything_and_unfreeze_restores_it(s, partners, contracts_dir):
    a, b = partners
    all_contracts = contracts.load_all(contracts_dir)
    before = readable(s, a)
    assert before > 0
    out = offboarding.freeze(s, a.partner, all_contracts, "ops@example.org", "contract_ended")
    assert out["master_key"] == partner_keys.DISABLED and a.source in out["sources"]
    assert readable(s, a) == 0, "a frozen partner's identifiers must be unreadable"
    assert readable(s, b) > 0, "other partners are untouched"
    with pytest.raises(offboarding.PartnerFrozen):
        pipeline.ingest(s, a)
    assert denied(lambda: inbound(s, a.partner).put_object(Bucket=s.bucket(raw.LOGICAL),
                                                          Key=f"{raw.drop_prefix(a)}late.csv", Body=b"x"))
    offboarding.unfreeze(s, a.partner, all_contracts, "ops@example.org", "reinstated")
    assert readable(s, a) == before
    pipeline.ingest(s, a)  # loads again
    events = [e["event"] for e in journal.partner_events(s, a.partner)]
    assert events[-2:] == ["freeze", "unfreeze"]


def test_the_report_lists_what_a_delete_would_remove(s, partners, contracts_dir):
    a, _ = partners
    all_contracts = contracts.load_all(contracts_dir)
    first = offboarding.report(s, a.partner, all_contracts)
    inv = first["inventory"]
    assert inv["sources"] == [a.source]
    assert {t["table"] for t in inv["tables"]} == {tables.qualified(a, layer).replace("ice.", "phi.", 1)
                                                   for layer in (tables.BRONZE, tables.SILVER)}
    assert all(t["rows"] > 0 for t in inv["tables"])
    assert sum(r["objects"] for r in inv["raw"]) >= 1 and inv["batches"] == 1
    assert inv["subjects"] > 0 and inv["subject_keys"] == inv["subjects"] and len(inv["keks"]) == 1
    assert inv["principals"] == [a.principal] and inv["master_key"]["state"] == partner_keys.ENABLED
    assert offboarding.report(s, a.partner, all_contracts)["report_hash"] == first["report_hash"]
    assert not any(c in json.dumps(first) for c in ("Jane", "Synthetic"))  # names and counts only


def test_the_gold_models_built_from_a_source_are_found():
    found = {d["model"] for d in offboarding.gold_dependents(["acme_claims"])}
    assert found == {"phi.gold.fact_claim", "deid.gold.claims_monthly", "deid.gold.provider_activity",
                     "phi.gold.member_claims"}
    assert {d["model"] for d in offboarding.gold_dependents(["lab_results"])} == {"deid.gold.lab_monthly"}


def test_gold_models_built_through_other_models_or_from_bronze_are_found():
    """dim_provider reads Bronze; fact_claim reads dim_provider; the marts read fact_claim. A delete
    report that missed any of them would let approvers sign off on less than the delete removes."""
    found = {d["model"]: d["reads"] for d in offboarding.gold_dependents(["provider_directory"])}
    assert set(found) == {"phi.gold.dim_provider", "phi.gold.fact_claim", "deid.gold.claims_monthly",
                          "deid.gold.provider_activity"}
    assert found["deid.gold.claims_monthly"] == ["bronze.provider_directory__providers"]


def test_a_delete_refuses_without_freeze_approvals_or_the_approved_report(s, partners, contracts_dir):
    a, _ = partners
    all_contracts = contracts.load_all(contracts_dir)
    digest = offboarding.report(s, a.partner, all_contracts)["report_hash"]
    args = dict(request="LEGAL-1", report_hash=digest, gold="keep", actor="ops@example.org")
    with pytest.raises(offboarding.Refused, match="frozen"):
        offboarding.delete(s, a.partner, all_contracts, approvers=["x@example.org", "y@example.org"], **args)
    offboarding.freeze(s, a.partner, all_contracts, "ops@example.org", "contract_ended")
    with pytest.raises(offboarding.Refused, match="two different approvers"):
        offboarding.delete(s, a.partner, all_contracts, approvers=["x@example.org", "X@example.org"], **args)
    with pytest.raises(offboarding.Refused, match="changed since the approved report"):
        offboarding.delete(s, a.partner, all_contracts, approvers=["x@example.org", "y@example.org"],
                           **{**args, "report_hash": "0" * 64})
    assert offboarding.state(s, a.partner)["state"] == offboarding.FROZEN  # nothing happened


def test_an_approved_delete_removes_everything_and_leaves_other_partners(s, partners, contracts_dir, monkeypatch, canaries):
    a, b = partners
    all_contracts = contracts.load_all(contracts_dir)
    backup = KeyVault(s).backup()
    offboarding.freeze(s, a.partner, all_contracts, "ops@example.org", "contract_ended")
    digest = offboarding.report(s, a.partner, all_contracts)["report_hash"]
    approval = dict(request="LEGAL-2", approvers=["cfo@example.org", "counsel@example.org"], report_hash=digest,
                    gold="keep", actor="ops@example.org")

    real_raw = offboarding._STEP["raw"]
    monkeypatch.setitem(offboarding._STEP, "raw", lambda *_: (_ for _ in ()).throw(RuntimeError("interrupted")))
    with pytest.raises(RuntimeError, match="interrupted"):
        offboarding.delete(s, a.partner, all_contracts, **approval)
    st = offboarding.state(s, a.partner)
    assert st["state"] == offboarding.DELETING and {"master_key", "gold", "tables"} <= set(st["done_steps"])
    with pytest.raises(offboarding.Refused, match="in progress"):
        offboarding.delete(s, a.partner, all_contracts, **{**approval, "request": "OTHER"})
    monkeypatch.setitem(offboarding._STEP, "raw", real_raw)
    out = offboarding.delete(s, a.partner, all_contracts, **approval)  # resumes
    assert out["state"] == offboarding.DELETED

    inv = offboarding.report(s, a.partner, all_contracts)["inventory"]
    assert inv["tables"] == [] and all(r["objects"] == 0 for r in inv["raw"])
    assert inv["subjects"] == inv["subject_keys"] == inv["batches"] == 0 and inv["keks"] == []
    assert inv["master_key"]["state"] == partner_keys.DELETED
    assert not Manifest(s).for_source(a.source)
    assert s.principal_secret(a.principal) is None
    events = [e["event"] for e in journal.partner_events(s, a.partner)]
    assert events.index("delete-approved") < events.index("delete-completed")
    approved = next(e for e in journal.partner_events(s, a.partner) if e["event"] == "delete-approved")
    assert approved["approvers"] == ["cfo@example.org", "counsel@example.org"] and approved["report_hash"] == digest

    # A vault backup from before cannot bring the partner back: its master key is gone.
    KeyVault(s).restore(backup, lambda: journal.shredded_subjects(s))
    keks = [i for i in offboarding._scan(s, KEKS) if i["source"]["S"] == a.source]
    with pytest.raises(partner_keys.Unavailable):
        s.partner_keys().unwrap(a.partner, keks[0]["kek_id"]["S"], keks[0]["wrapped_kek"]["B"])

    assert readable(s, b) > 0  # the other partner still loads and decrypts
    drop(s, b, "more.csv", synthetic.claims_csv(canaries, date(2026, 6, 10), days=1, per_day=3, seed=9))
    pipeline.ingest(s, b)
    assert {x.state for x in Manifest(s).for_source(b.source)} == {SILVER_COMMITTED}


def test_rewrap_moves_shared_keks_under_the_partner_key(s):
    source, partner = f"rewrap_{uuid.uuid4().hex[:6]}", "rewrap_partner"
    vault = KeyVault(s)
    sids = [uuid.uuid4().hex for _ in range(3)]
    before = vault.get_or_create(sids, source)  # no partner: the shared master key, as before partner keys
    item = s.dynamodb().get_item(TableName=KEKS, Key={"kek_id": {"S": f"{source}#1"}})["Item"]
    assert "partner" not in item
    s.partner_keys().ensure(partner)
    try:
        assert vault.rewrap_keks({source: partner})["moved"] >= 1
        item = s.dynamodb().get_item(TableName=KEKS, Key={"kek_id": {"S": f"{source}#1"}})["Item"]
        assert item["partner"]["S"] == partner
        assert KeyVault(s).get(sids) == before
        assert vault.rewrap_keks({source: partner})["moved"] == 0  # idempotent
    finally:
        vault.delete(sids)
        s.dynamodb().delete_item(TableName=KEKS, Key={"kek_id": {"S": f"{source}#1"}})
        s.partner_keys().set_state(partner, partner_keys.DELETED)
