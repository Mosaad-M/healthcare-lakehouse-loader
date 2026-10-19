"""A partner API source (http_api fetch), end to end against the mock partner in compose.

Each fetch asks for the last 7 days, lands every page as one raw file and loads it as one batch;
Silver replaces that date window, so a partner's corrections replace what it sent before.
"""
import json
import os
import urllib.request

import pytest

from flows.ingest import IngestFailed, ingest
from loader import contracts, duck, raw, tables
from loader.state import SILVER_COMMITTED, Manifest
from tests.pipeline.conftest import deploy_copy, remove_source

MOCK = os.environ.get("PARTNER_API_URL", "http://partner-api:8080")  # an ECS service on AWS
DAYS, PER_DAY = 7, 6


def configure(**state) -> dict:
    req = urllib.request.Request(f"{MOCK}/admin/config", data=json.dumps(state).encode(), method="POST",
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as resp:
        return json.load(resp)


@pytest.fixture(scope="module")
def api(s, run_id, contracts_dir):
    configure(tag=run_id, version=1, per_day=PER_DAY, fail_next=0)
    example = contracts.load_all()["acme_encounters"].fetch.model_dump()
    contract = deploy_copy(s, contracts_dir, "acme_encounters", f"encounters_{run_id}",
                           fetch={**example, "url": f"{MOCK}/v1/encounters"})
    yield contract
    remove_source(s, contract)


def run(contract, contracts_dir):
    ingest(source=contract.source, contracts_dir=str(contracts_dir))


def silver(s, contract, sql: str):
    con = duck.connect(s, tables.CATALOG, principal=contract.principal)
    return con.execute(sql.format(t=tables.qualified(contract, tables.SILVER))).fetchone()


def test_a_fetch_loads_the_window_as_one_batch(s, api, contracts_dir):
    run(api, contracts_dir)
    batches = Manifest(s).for_source(api.source)
    assert [b.state for b in batches] == [SILVER_COMMITTED]  # 42 records over 2 pages, one batch
    assert batches[0].rows_silver == DAYS * PER_DAY
    assert silver(s, api, "SELECT count(DISTINCT encounter_date) FROM {t}") == (DAYS,)
    landed = raw.RawZone(s).read(batches[0].raw_key)
    assert landed.startswith(b"[") and landed.count(b'"next_page"') == 2  # pages kept as received


def test_an_unchanged_refetch_adds_nothing(s, api, contracts_dir):
    run(api, contracts_dir)
    assert len(Manifest(s).for_source(api.source)) == 1  # same bytes, same batch: a no-op


def test_restated_amounts_replace_the_window(s, api, contracts_dir):
    before = silver(s, api, "SELECT count(*), sum(charge_amount) FROM {t}")
    configure(version=2)
    run(api, contracts_dir)
    after = silver(s, api, "SELECT count(*), sum(charge_amount) FROM {t}")
    assert after[0] == before[0] == DAYS * PER_DAY
    assert after[1] != before[1]
    assert [b.state for b in Manifest(s).for_source(api.source)] == [SILVER_COMMITTED] * 2


def test_transient_errors_are_retried(s, api, contracts_dir):
    configure(version=3, fail_next=2)
    run(api, contracts_dir)
    assert len(Manifest(s).for_source(api.source)) == 3


def test_a_refused_token_fails_cleanly_and_is_never_logged(s, run_id, contracts_dir, monkeypatch):
    good = s.source_secret("acme_partner_api")
    bad = f"wrong-{run_id}-token"
    monkeypatch.setenv("LOADER_SOURCE_SECRET_ACME_PARTNER_API", bad)
    with pytest.raises(IngestFailed, match="HTTP 401") as exc:
        ingest(source=f"encounters_{run_id}", contracts_dir=str(contracts_dir))
    assert bad not in str(exc.value) and good not in str(exc.value)


def test_patient_identifiers_are_stored_only_as_ciphertext(s, api, canaries):
    con = duck.connect(s, tables.CATALOG, principal=api.principal)
    rows = con.execute(f"SELECT patient_mrn, patient_name FROM {tables.qualified(api, tables.SILVER)}").fetchall()
    assert rows and all(isinstance(v, (bytes, bytearray)) for r in rows for v in r)
    blob = b"".join(bytes(v) for r in rows for v in r)
    assert not [c for c in canaries.all() if c.encode() in blob]
