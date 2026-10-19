"""Drop synthetic files for the three example sources, run their ingest deployments, show the result.

    make demo
"""
import uuid
from datetime import date

from prefect.deployments import run_deployment

from loader import contracts, duck, raw, settings, synthetic, tables
from loader.state import Manifest


def main() -> None:
    s, tag = settings.load(), uuid.uuid4().hex[:8]
    canaries = synthetic.Canaries(tag)
    files = {
        "acme_claims": synthetic.claims_csv(canaries, date(2026, 9, 1), days=7, per_day=20, seed=int(tag[:4], 16)),
        "provider_directory": synthetic.providers_csv(50, seed=int(tag[:4], 16)),
        "lab_results": synthetic.lab_ndjson(canaries, 200, seed=int(tag[:4], 16), id_offset=int(tag[:4], 16) * 1000),
    }
    zone = raw.RawZone(s)
    for source, body in files.items():
        contract = contracts.load_all()[source]
        zone.drop(contract, f"demo_{tag}.{contract.format.type}", body)
        state = run_deployment(f"ingest/{source}", timeout=300).state
        con = duck.connect(s, tables.CATALOG, principal=contract.principal)
        rows = {layer: con.execute(f"SELECT count(*) FROM {tables.qualified(contract, layer)}").fetchone()[0]
                for layer in (tables.BRONZE, tables.SILVER)}
        latest = Manifest(s).for_source(source)[-1]
        print(f"{source:20} run {state.type.value:9} batch {latest.state:16} "
              f"Bronze {rows['bronze']:6} rows, Silver {rows['silver']:6} rows ({contract.silver.semantics})")


if __name__ == "__main__":
    main()
