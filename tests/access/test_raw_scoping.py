"""Raw-zone access is scoped to one source: a load of one source, with the credentials it is given,
cannot list, read, write or delete another source's files, whichever partner it belongs to."""
import uuid

import botocore.exceptions
import pytest

from loader import contracts, partners, raw


def denied(fn) -> bool:
    try:
        fn()
        return False
    except botocore.exceptions.ClientError as err:
        return err.response["Error"]["Code"] in ("AccessDenied", "403")


@pytest.fixture
def two_sources(s):
    """A file dropped for acme_claims and one for lab_results (another partner)."""
    all_contracts = contracts.load_all()
    mine, theirs = all_contracts["acme_claims"], all_contracts["lab_results"]
    assert mine.partner != theirs.partner
    base, name = raw.RawZone(s), f"scope-{uuid.uuid4().hex[:8]}.csv"
    keys = [f"{raw.drop_prefix(c)}{name}" for c in (mine, theirs)]
    for key in keys:
        base.put(key, b"synthetic")
    yield mine, theirs, keys
    for key in keys:
        base.delete(key)


def test_a_load_reaches_only_its_own_source(s, two_sources):
    mine, theirs, (my_key, their_key) = two_sources
    zone = raw.RawZone.for_contract(s, mine)
    assert zone.read(my_key) == b"synthetic"
    assert my_key in zone.keys(raw.drop_prefix(mine))
    assert denied(lambda: zone.read(their_key))
    assert denied(lambda: zone.keys(raw.drop_prefix(theirs)))
    assert denied(lambda: zone.keys("drops/"))  # not even the list of other sources
    assert denied(lambda: zone.put(f"{raw.drop_prefix(theirs)}planted.csv", b"x"))
    assert denied(lambda: zone.delete(their_key))
    assert denied(lambda: zone.s3.copy_object(Bucket=zone.bucket, Key=f"{raw.drop_prefix(mine)}copied.csv",
                                              CopySource={"Bucket": zone.bucket, "Key": their_key}))


def test_an_email_source_also_reaches_its_mailbox_only(s):
    mail = contracts.load_all()["acme_lab_mail"]
    scope = partners.raw_scope(mail)[partners.RAW]
    assert f"inbox/{mail.fetch.mailbox}/" in scope and not any(p == "inbox/" for p in scope)
    zone = raw.RawZone.for_contract(s, mail)
    assert denied(lambda: zone.keys("inbox/"))
