"""Run a benchmark scenario on the host: every step is its own container (the `bench` service,
capped like the ECS task), so a step that runs out of memory or time ends the sweep, not the run.

    python3 bench/run.py rows | new_subjects | known_subjects | silver_growth | silver_cdc | small_files [--max N]

Results: bench/results/<scenario>.jsonl, one line per step (measurements, or how it broke).
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "bench" / "results" / os.environ.get("BENCH_RESULTS", "local")
COMPOSE = ["docker", "compose", "-f", "stack/compose.yaml", "--env-file", ".env", "--profile", "tools",
           "run", "--rm", "-T", "bench"]
STEP_TIMEOUT = 3 * 3600  # a step slower than this has broken for a daily load


class Broke(Exception):
    pass


def local_step(args: list[str], timeout: int) -> tuple[int, str, str]:
    """`python -m <args>` in the capped `bench` container: (exit code, stdout, stderr)."""
    container = f"bench-step-{int(time.time() * 1000)}"
    try:
        proc = subprocess.run(COMPOSE[:-1] + ["--name", container, "bench", "python", "-m", *args], cwd=ROOT,
                              capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        subprocess.run(["docker", "rm", "-f", container], capture_output=True)
        raise Broke(f"timeout after {timeout} s")
    return proc.returncode, proc.stdout, proc.stderr


# Where a step runs: here, or (scripts/sandbox.py bench) a toolbox task on AWS with the same cap.
STEP = local_step


def bench(*args: str, timeout: int = STEP_TIMEOUT) -> dict | None:
    """Run `python -m <args>` capped like the ECS task; return its RESULT, raise Broke on failure."""
    start = time.time()
    code, stdout, stderr = STEP(list(args), timeout)
    if code == 137:
        # Locally the Docker VM running out of memory kills it the same way (check its dmesg: global_oom).
        raise Broke("out of memory (killed: the 8 GB cap, or locally the Docker VM)")
    if code != 0:
        tail = [line for line in stderr.splitlines() if line.strip()][-3:]
        raise Broke(f"exit {code}: {' | '.join(tail)[:400]}")
    for line in stdout.splitlines():
        if line.startswith("RESULT "):
            result = json.loads(line[7:])
            result["container_s"] = round(time.time() - start, 1)
            return result
    return None


def record(scenario: str, entry: dict) -> None:
    RESULTS.mkdir(parents=True, exist_ok=True)
    entry = {"at": datetime.now(timezone.utc).isoformat(timespec="seconds"), **entry}
    with open(RESULTS / f"{scenario}.jsonl", "a") as out:
        out.write(json.dumps(entry) + "\n")
    print(json.dumps({k: entry.get(k) for k in ("at", "params", "wall_s", "peak_rss_mb", "broke") if k in entry}), flush=True)


def fresh(name: str) -> None:
    bench("bench.step", "--name", name, "--remove", "--deploy")


def remove(name: str) -> None:
    bench("bench.step", "--name", name, "--remove")


def generate(name: str, rows: int, subjects: int, **kw) -> None:
    args = ["bench.generate", "--name", name, "--rows", str(rows), "--subjects", str(subjects)]
    for k, v in kw.items():
        args += [f"--{k.replace('_', '-')}"] + ([str(v)] if v != "" else [])  # "" marks a flag
    bench(*args)


def ingest(name: str, label: str = "") -> dict:
    return bench("bench.step", "--name", name, "--ingest", "--label", label)


def sweep(scenario: str, steps) -> None:
    """Each step is (params, callable returning a result); stop at the first step that breaks."""
    for params, run_step in steps:
        try:
            result = run_step()
            record(scenario, {"params": params, **(result or {})})
        except Broke as err:
            record(scenario, {"params": params, "broke": str(err)})
            return


def scenario_rows(top: int):
    """Rows per batch with a fixed 1,000 members: DuckDB, the encryption loop and memory."""
    for rows in (10_000, 100_000, 1_000_000, 3_000_000, 10_000_000, 30_000_000, 100_000_000):
        if rows > top:
            return
        def run_step(rows=rows):
            fresh("rows")
            generate("rows", rows, 1000)
            return ingest("rows", f"{rows} rows")
        yield {"rows": rows, "subjects": 1000}, run_step


def scenario_new_subjects(top: int):
    """First-time members per batch (rows = members): identity and key creation."""
    for subjects in (1_000, 3_000, 10_000, 30_000, 100_000, 300_000, 1_000_000):
        if subjects > top:
            return
        def run_step(subjects=subjects):
            fresh("new_subjects")
            generate("new_subjects", subjects, subjects)
            return ingest("new_subjects", f"{subjects} new members")
        yield {"rows": subjects, "new_subjects": subjects}, run_step


def scenario_known_subjects(top: int):
    """Members seen before (keys exist): key reads, one unwrap (a KMS call on AWS) per member."""
    for subjects in (1_000, 3_000, 10_000, 30_000, 100_000, 300_000, 1_000_000):
        if subjects > top:
            return
        def run_step(subjects=subjects):
            fresh("known_subjects")
            generate("known_subjects", subjects, subjects)
            ingest("known_subjects", "warm-up: create the members")
            generate("known_subjects", subjects, subjects, offset=subjects, first_day=60)
            return ingest("known_subjects", f"{subjects} known members")
        yield {"rows": subjects, "known_subjects": subjects}, run_step


def scenario_silver_growth(top: int):
    """1M-row batches into one table, each a new 30-day window: merge cost as Silver grows."""
    fresh("silver_growth")
    batches = max(1, top // 1_000_000)
    for b in range(batches):
        def run_step(b=b):
            generate("silver_growth", 1_000_000, 1000, offset=b * 1_000_000, first_day=b * 30)
            return ingest("silver_growth", f"batch {b + 1}, Silver {(b + 1)} M rows")
        yield {"batch": b + 1, "rows": 1_000_000, "silver_rows_after": (b + 1) * 1_000_000}, run_step


def scenario_silver_append(top: int):
    """As silver_growth, with append semantics: a MERGE on the primary key against all of Silver."""
    fresh("silver_append")
    for b in range(max(1, top // 1_000_000)):
        def run_step(b=b):
            generate("silver_append", 1_000_000, 1000, offset=b * 1_000_000, first_day=b * 30)
            return ingest("silver_append", f"batch {b + 1}, Silver {b + 1} M rows")
        yield {"batch": b + 1, "rows": 1_000_000, "silver_rows_after": (b + 1) * 1_000_000}, run_step


def scenario_silver_append_random(top: int):
    """As silver_append, with claim IDs in random order: file statistics no longer skip old files."""
    fresh("silver_append_random")
    for b in range(max(1, top // 1_000_000)):
        def run_step(b=b):
            generate("silver_append_random", 1_000_000, 1000, offset=b * 1_000_000, first_day=b * 30, random_keys="")
            return ingest("silver_append_random", f"batch {b + 1}, Silver {b + 1} M rows")
        yield {"batch": b + 1, "rows": 1_000_000, "silver_rows_after": (b + 1) * 1_000_000}, run_step


def scenario_silver_cdc(top: int):
    """cdc semantics: Silver loaded with 3M rows, then batches of 1M changes spread over every key
    (every third key; 1 in 20 a delete): the cost of applying a change batch to a whole table."""
    fresh("silver_cdc")
    for b in range(3):
        def load(b=b):
            generate("silver_cdc", 1_000_000, 1000, offset=b * 1_000_000, cdc_op="I", cdc_seq=1 + b * 1_000_000)
            return ingest("silver_cdc", f"insert batch {b + 1}, Silver {b + 1} M rows")
        yield {"step": f"insert {b + 1}", "rows": 1_000_000}, load
    for c in range(max(1, top // 1_000_000)):
        def change(c=c):
            generate("silver_cdc", 1_000_000, 1000, stride=3, cdc_op="U", cdc_seq=(10 + c) * 10_000_000)
            return ingest("silver_cdc", f"change batch {c + 1} on 3 M rows")
        yield {"step": f"change {c + 1}", "rows": 1_000_000}, change


def scenario_soak(top: int):
    """As small_files, with table maintenance every 50 batches: does it keep commits bounded?"""
    fresh("soak")
    for b in range(top):
        def run_step(b=b):
            generate("soak", 1000, 100, offset=b * 1000, first_day=b)
            result = ingest("soak", f"batch {b + 1}")
            if (b + 1) % 50 == 0:
                result["maintenance"] = bench("bench.step", "--name", "soak", "--maintain", "--label", f"after batch {b + 1}")
            return result
        yield {"batch": b + 1, "rows": 1000}, run_step


def scenario_small_files(top: int):
    """Many 1,000-row batches into one table without maintenance: per-commit overhead and metadata."""
    fresh("small_files")
    for b in range(top):
        def run_step(b=b):
            generate("small_files", 1000, 100, offset=b * 1000, first_day=b)
            return ingest("small_files", f"batch {b + 1}")
        yield {"batch": b + 1, "rows": 1000}, run_step


SCENARIOS = {"rows": (scenario_rows, 100_000_000), "new_subjects": (scenario_new_subjects, 1_000_000),
             "known_subjects": (scenario_known_subjects, 1_000_000),
             "silver_growth": (scenario_silver_growth, 100_000_000), "silver_append": (scenario_silver_append, 100_000_000),
             "silver_append_random": (scenario_silver_append_random, 100_000_000),
             "silver_cdc": (scenario_silver_cdc, 3_000_000),
             "small_files": (scenario_small_files, 2000), "soak": (scenario_soak, 500)}

def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("scenario", choices=sorted(SCENARIOS))
    p.add_argument("--max", type=int, help="upper bound for the swept value (rows, members, batches)")
    p.add_argument("--keep", action="store_true", help="leave the benchmark source in place")
    a = p.parse_args(argv)
    make, default_top = SCENARIOS[a.scenario]
    sweep(a.scenario, make(a.max or default_top))
    if not a.keep:
        remove(a.scenario)


if __name__ == "__main__":
    main()
