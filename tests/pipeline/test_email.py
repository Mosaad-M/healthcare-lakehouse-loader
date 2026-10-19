"""An email source end to end: SMTP into the raw inbox (SES inbound on AWS), attachments from
allowed senders loaded, everything else quarantined, message bodies never kept."""
import smtplib
import uuid
from email.message import EmailMessage

import pytest

from flows.ingest import ingest
from loader import duck, raw, settings, tables
from loader.state import SILVER_COMMITTED, Manifest
from tests.pipeline.conftest import deploy_copy, remove_source

SENDER = "results@acmelab.example"


def csv_results(canaries, offset: int, count: int = 20) -> bytes:
    lines = ["result_id,patient_mrn,collected_on,test_code,value,unit"]
    for i in range(offset, offset + count):
        mrn = canaries.mrn if i == offset else f"MRN{canaries.tag}{i:04d}"
        lines.append(f"L{canaries.tag}{i:05d},{mrn},2026-09-{1 + i % 28:02d},2345-7,{80 + i % 50}.5,mg/dL")
    return ("\n".join(lines) + "\n").encode()


def send(mailbox: str, sender: str, attachments: list[tuple[str, bytes]], body: str = "Nightly results.") -> None:
    message = EmailMessage()
    message["From"], message["To"], message["Subject"] = sender, mailbox, "Results"
    message.set_content(body)
    for name, content in attachments:
        message.add_attachment(content, maintype="text", subtype="csv", filename=name)
    s = settings.load()
    if s.is_aws:  # no SES receipt rule in the sandbox: store the message as SES's S3 action would
        raw.RawZone(s).put(f"inbox/{mailbox.lower()}/{uuid.uuid4().hex}", message.as_bytes())
        return
    with smtplib.SMTP("smtp-in", 2525, timeout=10) as smtp:
        smtp.send_message(message)


@pytest.fixture(scope="module")
def lab(s, run_id, contracts_dir):
    mailbox = f"lab-{run_id}@inbound.example.org"
    contract = deploy_copy(s, contracts_dir, "acme_lab_mail", f"labmail_{run_id}", fetch={
        "kind": "email", "mailbox": mailbox, "allowed_senders": [SENDER], "attachment_glob": "*.csv"})
    yield contract
    remove_source(s, contract)
    zone = raw.RawZone(s)
    for prefix in (f"inbox/{mailbox}/", f"quarantine/inbox/{mailbox}/"):
        for key in zone.keys(prefix):
            zone.delete(key)


def run(contract, contracts_dir):
    ingest(source=contract.source, contracts_dir=str(contracts_dir))


def test_attachments_from_the_lab_are_loaded_and_the_message_is_not_kept(s, lab, canaries, contracts_dir):
    # A patient's name in the file name and body must go nowhere (they are not stored).
    send(lab.fetch.mailbox, SENDER, [(f"results_{canaries.name}.csv", csv_results(canaries, 0))],
         body=f"Results for {canaries.name} attached.")
    run(lab, contracts_dir)
    batches = Manifest(s).for_source(lab.source)
    assert [(b.state, b.rows_silver) for b in batches] == [(SILVER_COMMITTED, 20)]
    zone = raw.RawZone(s)
    assert not zone.keys(f"inbox/{lab.fetch.mailbox}/"), "the message was kept after extraction"
    assert canaries.name not in batches[0].raw_key


def test_a_resent_attachment_is_not_loaded_twice(s, lab, canaries, contracts_dir):
    send(lab.fetch.mailbox, SENDER, [("results.csv", csv_results(canaries, 0))], body="Resending.")
    run(lab, contracts_dir)
    assert len(Manifest(s).for_source(lab.source)) == 1


def test_two_attachments_make_two_batches(s, lab, canaries, contracts_dir):
    send(lab.fetch.mailbox, SENDER, [("a.csv", csv_results(canaries, 100)), ("b.csv", csv_results(canaries, 200)),
                                     ("notes.txt", b"not a csv")])
    run(lab, contracts_dir)
    con = duck.connect(s, tables.CATALOG, principal=lab.principal)
    assert con.execute(f"SELECT count(*) FROM {tables.qualified(lab, tables.SILVER)}").fetchone() == (60,)


@pytest.mark.parametrize("sender, attachments", [
    ("someone@elsewhere.example", [("results.csv", b"result_id\n1\n")]),
    (SENDER, []),
])
def test_other_mail_is_quarantined_and_loads_nothing(s, lab, contracts_dir, sender, attachments):
    before = len(Manifest(s).for_source(lab.source))
    send(lab.fetch.mailbox, sender, attachments, body="Hello")
    run(lab, contracts_dir)
    assert len(Manifest(s).for_source(lab.source)) == before
    zone = raw.RawZone(s)
    assert not zone.keys(f"inbox/{lab.fetch.mailbox}/")
    assert zone.keys(f"quarantine/inbox/{lab.fetch.mailbox}/")
