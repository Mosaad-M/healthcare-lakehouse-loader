"""Concurrent ingests of one source: a scheduled run and a manual one, or two workers, must
load every batch exactly once. Locks serialize the writes; batch IDs make repeats idempotent."""
import threading
from datetime import date


from flows.ingest import ingest
from loader import duck, synthetic, tables
from loader.state import SILVER_COMMITTED, Manifest
from tests.pipeline.conftest import deploy_copy, drop, remove_source


def test_parallel_ingests_load_each_batch_once(s, run_id, contracts_dir, canaries):
    claims = deploy_copy(s, contracts_dir, "acme_claims", f"parallel_{run_id}")
    try:
        expected = 0
        for week in range(4):  # four files, each a separate window of dates
            body = synthetic.claims_csv(canaries, date(2026, 6, 1 + 7 * week), days=3, per_day=8, seed=week + 20,
                                        id_offset=1000 * week)
            expected += body.count(b"\n") - 1
            drop(s, claims, f"week{week}.csv", body)
        errors = []

        def run():
            try:
                ingest(source=claims.source, contracts_dir=str(contracts_dir))
            except Exception as err:  # noqa: BLE001
                errors.append(repr(err))

        workers = [threading.Thread(target=run) for _ in range(3)]
        for w in workers:
            w.start()
        for w in workers:
            w.join()
        assert not errors, errors
        batches = Manifest(s).for_source(claims.source)
        assert len(batches) == 4 and {b.state for b in batches} == {SILVER_COMMITTED}
        con = duck.connect(s, tables.CATALOG, principal=claims.principal)
        bronze, silver = (tables.qualified(claims, layer) for layer in (tables.BRONZE, tables.SILVER))
        assert con.execute(f"SELECT count(*), count(DISTINCT (_batch_id, _row_number)) FROM {bronze}").fetchone() == (expected, expected)
        assert con.execute(f"SELECT count(*), count(DISTINCT claim_id) FROM {silver}").fetchone() == (expected, expected)
    finally:
        remove_source(s, claims)
