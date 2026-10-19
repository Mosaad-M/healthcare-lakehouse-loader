"""The operator CLI end to end (it had no test). Re-identification, linking, erasure and vault
backup and restore are run as an operator runs them."""
import json
import uuid
from datetime import date

import pytest

from flows.ingest import ingest
from loader import cli, synthetic
from loader.identity import Identity
from tests.pipeline.conftest import deploy_copy, drop, remove_source


@pytest.fixture(scope="module")
def claims(s, run_id, contracts_dir, canaries):
    contract = deploy_copy(s, contracts_dir, "acme_claims", f"cli_claims_{run_id}")
    drop(s, contract, "claims.csv", synthetic.claims_csv(canaries, date(2026, 9, 1), days=2, per_day=5, members=5))
    ingest(source=contract.source, contracts_dir=str(contracts_dir))
    yield contract
    remove_source(s, contract)


def run(capsys, contracts_dir, *args) -> str:
    cli.main(["--contracts-dir", str(contracts_dir), *args])
    return capsys.readouterr().out


def test_operator_paths(s, claims, canaries, contracts_dir, capsys, tmp_path):
    subject = Identity(s).resolve(claims.source, [(canaries.member_id,)])[(canaries.member_id,)]
    reid = ["reidentify", "--source", claims.source, "--subject", subject, "--caller", "ops@example.org"]

    out = json.loads(run(capsys, contracts_dir, *reid, "--role", "care_coordinator", "--purpose", "treatment"))
    assert out["audit_event"] and {r["member_name"] for r in out["rows"]} == {canaries.name}

    with pytest.raises(SystemExit, match="refused"):  # a role that may not decrypt for this purpose
        run(capsys, contracts_dir, *reid, "--role", "billing_specialist", "--purpose", "treatment")

    other = Identity(s).resolve(f"{claims.source}_other", [("X-1",)])[("X-1",)]
    person = run(capsys, contracts_dir, "link", "--subject", subject, "--subject", other).strip()
    assert Identity(s).person_of(subject) == Identity(s).person_of(other) == person

    backup = tmp_path / "vault.json"
    backup.write_text(run(capsys, contracts_dir, "vault-backup"))
    assert {"keks", "keys"} <= set(json.loads(backup.read_text()))

    shredded = json.loads(run(capsys, contracts_dir, "shred", "--subject", subject, "--request", f"t-{uuid.uuid4().hex[:6]}",
                              "--actor", "privacy@example.org", "--reason", "patient_request"))
    assert shredded["subjects"] == 1

    assert "shred journal re-applied" in run(capsys, contracts_dir, "vault-restore", str(backup))
    after = json.loads(run(capsys, contracts_dir, *reid, "--role", "privacy_officer", "--purpose", "patient_request"))
    assert after["unreadable"] and not any(canaries.name in json.dumps(r) for r in after["rows"])
