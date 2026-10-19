"""One measured benchmark step, in its own process (so peak memory is this step's alone).

    python -m bench.step --name batch_rows --ingest          ingest what is in the drop prefix
    python -m bench.step --name x --deploy | --remove        set up or remove the source

Prints one line `RESULT {json}` with stage times, call counts and peak memory.
"""
import argparse
import json
import time

from bench import instrument, sources
from loader import pipeline, settings


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--name", required=True)
    p.add_argument("--deploy", action="store_true")
    p.add_argument("--remove", action="store_true")
    p.add_argument("--ingest", action="store_true")
    p.add_argument("--maintain", action="store_true", help="run table maintenance on the source's tables")
    p.add_argument("--label", default="")
    a = p.parse_args()
    s, c = settings.load(), sources.contract(a.name)
    if a.remove:
        sources.remove_source(s, c)
    if a.deploy:
        sources.deploy_source(s, c)
    if a.maintain:
        from dataclasses import asdict

        from loader import maintenance, tables

        start = time.perf_counter()
        runs = maintenance.maintain_all(s, only=[f"{tables.CATALOG}.{layer}.{c.table}" for layer in (tables.BRONZE, tables.SILVER)])
        print("RESULT " + json.dumps({"name": a.name, "label": a.label, "wall_s": round(time.perf_counter() - start, 3),
                                      "maintenance": [{"table": r.table, "actions": [x.kind for x in r.actions],
                                                       "skipped": r.skipped, "before": asdict(r.before),
                                                       "after": asdict(r.after) if r.after else None} for r in runs]},
                                     default=str), flush=True)
    if a.ingest:
        instrument.install()
        start = time.perf_counter()
        summary = pipeline.ingest(s, c)
        wall = time.perf_counter() - start
        print("RESULT " + json.dumps(instrument.report({
            "name": a.name, "label": a.label, "wall_s": round(wall, 3), "batches": len(summary.loaded),
            "quarantined": len(summary.quarantined)})), flush=True)


if __name__ == "__main__":
    main()
