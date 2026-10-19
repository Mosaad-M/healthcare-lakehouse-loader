"""Every contract in contracts/ is valid, and the rules that protect PHI are enforced at load time."""
import copy

import pytest
import yaml
from pydantic import ValidationError

from loader import contracts, tables
from loader.contracts import Contract

BASE = {
    "source": "demo", "entity": "rows", "owner": "team@example.org", "format": {"type": "csv"},
    "columns": [{"name": "id", "type": "string", "phi": "none"},
                {"name": "ssn", "type": "string", "phi": "identifier", "action": "drop"},
                {"name": "seen_on", "type": "date"},
                {"name": "amount", "type": "decimal(12,2)", "phi": "none"}],
    "primary_key": ["id"], "silver": {"semantics": "window", "window_column": "seen_on"},
}


def variant(**changes) -> dict:
    data = copy.deepcopy(BASE)
    data.update(changes)
    return data


def test_repository_contracts_are_valid():
    loaded = contracts.load_all()
    assert set(loaded) == {"acme_claims", "provider_directory", "lab_results", "acme_encounters", "acme_lab_mail",
                           "acme_clinic_patients", "acme_vitals"}
    for contract in loaded.values():
        if contract.identifiers:
            assert contract.subject, f"{contract.source} keeps identifiers without a subject key"


def test_unclassified_columns_default_to_phi_and_stay_out_of_metadata():
    contract = Contract.model_validate(BASE)
    props = tables.properties(contract, tables.SILVER)
    assert contract.columns[2].phi.value == "phi"
    assert props["write.metadata.metrics.default"] == "counts"
    assert "write.metadata.metrics.column.seen_on" not in props
    assert props["write.metadata.metrics.column.amount"] == "truncate(16)"
    assert contract.columns[3].type == "decimal(12, 2)"  # Iceberg's canonical spelling


def test_bronze_is_partitioned_by_ingest_day_never_by_data():
    contract = Contract.model_validate(BASE)
    schema = tables.iceberg_schema(tables.columns(contract, tables.BRONZE))
    spec = tables.partition_spec(schema, tables.BRONZE)
    source = next(f["name"] for f in schema["fields"] if f["id"] == spec["fields"][0]["source-id"])
    assert source == "_ingested_at"
    assert tables.partition_spec(schema, tables.SILVER)["fields"] == []


@pytest.mark.parametrize("data, error", [
    (variant(primary_key=["ssn"]), "must be a kept column"),
    (variant(columns=BASE["columns"] + [{"name": "mrn", "type": "string", "phi": "identifier"}],
             primary_key=["mrn"]), "is an identifier"),
    (variant(silver={"semantics": "window"}), "window_column"),
    (variant(silver={"semantics": "window", "window_column": "amount"}), "window_column"),
    (variant(columns=BASE["columns"] + [{"name": "pk_bucket", "type": "int"}]), "reserved"),
    (variant(columns=BASE["columns"] + [{"name": "_batch_id", "type": "string"}]), "should match pattern"),
    (variant(columns=BASE["columns"] + [{"name": "blob", "type": "map<string,string>"}]), "unsupported"),
    (variant(columns=BASE["columns"] + [{"name": "id", "type": "string"}]), "duplicate"),
    (variant(unexpected="x"), "Extra inputs"),
    (variant(columns=BASE["columns"] + [{"name": "email", "type": "string", "phi": "identifier"}]),
     "need a subject key"),
    (variant(columns=BASE["columns"] + [{"name": "email", "type": "string", "phi": "identifier"}],
             subject={"key": ["id"]}), "must be a kept identifier"),
    (variant(columns=BASE["columns"] + [{"name": "subject_id", "type": "string"}]), "reserved"),
    (variant(fetch={"kind": "stream"}), "stream sources land ndjson"),
    (variant(fetch={"kind": "stream"}, format={"type": "ndjson"}), "append semantics"),
])
def test_invalid_contracts_are_rejected(data, error):
    with pytest.raises(ValidationError, match=error):
        Contract.model_validate(yaml.safe_load(yaml.safe_dump(data)))


def test_the_loader_and_the_lifecycle_agree_on_raw_expiry():
    import json
    from pathlib import Path

    from loader import raw
    model = json.loads((Path(__file__).parents[2] / "stack" / "bootstrap" / "model.json").read_text())
    assert raw.EXPIRY_DAYS == model["raw_expiry_days"]
