"""Crash recovery: an ingest killed at any step is finished by the next run, with every row
loaded once. The crash is a real process exit (os._exit: no cleanup, locks left held) at four points
between the steps whose order the design relies on.
"""
import csv
import io
import os
import subprocess
import sys
from datetime import date
from decimal import Decimal

import pytest

from flows.ingest import ingest
from loader import duck, synthetic, tables
from loader.state import SILVER_COMMITTED, Manifest
from tests.pipeline.conftest import deploy_copy, drop, remove_source

# Where the process dies. Each is the moment just after (or before) one durable step.
CRASHES = {
    # The file is copied to its landed key; the manifest does not know the batch yet.
    "before_manifest": "from loader.state import Manifest\nManifest.add = lambda *a, **k: os._exit(9)",
    # Bronze is committed; the manifest still says LANDED (and the Bronze lock is left held).
    "after_bronze_commit": "from loader import bronze\nreal = bronze.write\n"
                           "bronze.write = lambda *a, **k: (real(*a, **k), os._exit(9))",
    # Silver is committed; the manifest still says BRONZE_COMMITTED (the Silver lock is left held).
    "after_silver_commit": "from loader import silver\nreal = silver.apply\n"
                           "silver.apply = lambda *a, **k: (real(*a, **k), os._exit(9))",
    # Bronze's statements ran in an open transaction that is never committed.
    "before_commit": "from loader import duck\n"
                     "def no_commit(connect, statements, **k):\n"
                     "    con = connect(); con.execute('BEGIN')\n"
                     "    for st in statements: con.execute(st)\n"
                     "    os._exit(9)\n"
                     "duck.run_txn = no_commit",
}


def crash_ingest(contract_path, point: str) -> int:
    script = (f"import os\n{CRASHES[point]}\n"
              "from pathlib import Path\nfrom loader import contracts, pipeline, settings\n"
              f"pipeline.ingest(settings.load(), contracts.load(Path({str(contract_path)!r})))\n")
    env = {**os.environ, "LOADER_LOCK_LEASE_SECONDS": "10"}  # a crashed holder's lock ends soon
    return subprocess.run([sys.executable, "-c", script], env=env).returncode


@pytest.mark.parametrize("point", sorted(CRASHES))
def test_a_crashed_ingest_is_finished_by_the_next_run(s, run_id, contracts_dir, canaries, point):
    claims = deploy_copy(s, contracts_dir, "acme_claims", f"crash_{point}_{run_id}")
    body = synthetic.claims_csv(canaries, date(2026, 9, 1), days=3, per_day=10, seed=7)
    rows = list(csv.DictReader(io.StringIO(body.decode())))
    try:
        drop(s, claims, "claims.csv", body)
        assert crash_ingest(contracts_dir / f"{claims.source}.yaml", point) == 9
        ingest(source=claims.source, contracts_dir=str(contracts_dir))

        batches = Manifest(s).for_source(claims.source)
        assert [b.state for b in batches] == [SILVER_COMMITTED]
        con = duck.connect(s, tables.CATALOG, principal=claims.principal)
        bronze, silver = (tables.qualified(claims, layer) for layer in (tables.BRONZE, tables.SILVER))
        assert con.execute(f"SELECT count(*), count(DISTINCT _row_number) FROM {bronze}").fetchone() == (len(rows), len(rows))
        assert con.execute(f"SELECT count(*), count(DISTINCT claim_id) FROM {silver}").fetchone() == (len(rows), len(rows))
        billed = con.execute(f"SELECT sum(billed_amount) FROM {silver}").fetchone()[0]
        assert billed == sum((Decimal(r["billed_amount"]) for r in rows), Decimal("0"))
    finally:
        remove_source(s, claims)
