"""Summarize a results directory (bench/results/local or aws): measured limits, unit costs, and the AWS cost of
a target volume (default: a mid-size payer). Pure arithmetic over the measurements; prices in
bench/cost.py.

    python3 bench/report.py [--results bench/results/local] [--out summary.md]   (prints when no --out)
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from cost import PRICES, firehose_dollars, usage

# The benchmark target: a mid-size payer, about 5M members, 20 sources, 50M rows a day, 10 TB in Silver.
TARGET = {"members": 5_000_000, "sources": 20, "rows_per_day": 50_000_000, "silver_tb": 10,
          # Claims-like feeds mention a few percent of members a day; an eligibility file mentions all.
          "members_per_batch": 250_000, "eligibility_sources": 2, "new_members_per_day": 5_000}


def load(results: Path, scenario: str) -> list[dict]:
    path = results / f"{scenario}.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()] if path.exists() else []


def fit(points: list[tuple[float, float]]) -> tuple[float, float]:
    """Least-squares line y = a + b x."""
    n = len(points)
    if n < 2:
        return (points[0][1], 0.0) if points else (0.0, 0.0)
    mx, my = sum(x for x, _ in points) / n, sum(y for _, y in points) / n
    sxx = sum((x - mx) ** 2 for x, _ in points)
    b = sum((x - mx) * (y - my) for x, y in points) / sxx if sxx else 0.0
    return my - b * mx, b


def t(result: dict, key: str) -> float:
    return result.get("times", {}).get(key, 0.0)


def table(headers: list[str], rows: list[list]) -> str:
    out = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    out += ["| " + " | ".join(str(c) for c in row) + " |" for row in rows]
    return "\n".join(out)


def fmt_s(seconds: float) -> str:
    if seconds >= 3600:
        return f"{seconds / 3600:.1f} h"
    if seconds >= 60:
        return f"{seconds / 60:.1f} min"
    return f"{seconds:.1f} s"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--results", default=str(Path(__file__).parent / "results" / "local"))
    p.add_argument("--out", help="write the summary here instead of printing it")
    a = p.parse_args()
    results = Path(a.results)
    md = ["# Scale and cost benchmarks", "",
          "Local runs, each step in a container capped like the loader's ECS task (2 vCPU, 8 GB, no swap). "
          "Times are local; AWS costs are measured request counts times list prices (bench/cost.py). "
          "Synthetic claims data only.", ""]

    rows = load(results, "rows")
    if rows:
        md += ["## Rows per batch (1,000 members per batch)", "",
               table(["rows", "wall", "Bronze", "Silver", "peak memory", "outcome"],
                     [[f"{r['params']['rows']:,}", fmt_s(r.get("wall_s", 0)) if "wall_s" in r else "",
                       fmt_s(t(r, "stage.bronze")) if "times" in r else "", fmt_s(t(r, "stage.silver")) if "times" in r else "",
                       f"{r.get('peak_rss_mb', 0):,.0f} MB" if "peak_rss_mb" in r else "", r.get("broke", "ok")] for r in rows]), ""]
        ok = [r for r in rows if "wall_s" in r]
        _, mb_per_row = fit([(r["params"]["rows"], r["peak_rss_mb"]) for r in ok])
        _, s_per_row = fit([(r["params"]["rows"], r["wall_s"] - t(r, "identity.resolve") - t(r, "vault.get_or_create")) for r in ok])
        md += [f"Fitted: {mb_per_row * 1024:.2f} KB of memory and {s_per_row * 1e6:.1f} µs of loader time per row "
               f"(member handling excluded). At 8 GB, a batch of about {8 * 1024 / mb_per_row / 1e6:.1f}M rows fits.", ""]

    for scenario, title in (("new_subjects", "First-time members per batch"), ("known_subjects", "Known members per batch")):
        res = load(results, scenario)
        if not res:
            continue
        param = "new_subjects" if scenario == "new_subjects" else "known_subjects"
        ok = [r for r in res if "wall_s" in r]
        md += [f"## {title}", "",
               table(["members", "wall", "identity", "keys", "DynamoDB items (w/r)", "key wraps/unwraps", "outcome"],
                     [[f"{r['params'][param]:,}", fmt_s(r.get("wall_s", 0)) if "wall_s" in r else "",
                       fmt_s(t(r, "identity.resolve")) if "times" in r else "", fmt_s(t(r, "vault.get_or_create")) if "times" in r else "",
                       f"{r.get('counts', {}).get('ddb.items.write', 0):,} / {r.get('counts', {}).get('ddb.items.read', 0):,}",
                       f"{r.get('counts', {}).get('keys.wrap', 0):,} / {r.get('counts', {}).get('keys.unwrap', 0):,}",
                       r.get("broke", "ok")] for r in res]), ""]
        if ok:
            _, per = fit([(r["params"][param], t(r, "identity.resolve") + t(r, "vault.get_or_create")) for r in ok])
            md += [f"Fitted: {per * 1000:.2f} ms per member; a million such members take {fmt_s(per * 1e6)}.", ""]

    growth = load(results, "silver_growth")
    if growth:
        ok = [r for r in growth if "wall_s" in r]
        md += ["## Silver growth (1M-row batches, each a new window)", "",
               table(["Silver rows after", "wall", "Bronze", "Silver", "outcome"],
                     [[f"{r['params']['silver_rows_after']:,}", fmt_s(r.get("wall_s", 0)) if "wall_s" in r else "",
                       fmt_s(t(r, "stage.bronze")) if "times" in r else "", fmt_s(t(r, "stage.silver")) if "times" in r else "",
                       r.get("broke", "ok")] for r in growth if r["params"]["batch"] % 5 == 0 or "broke" in r or r["params"]["batch"] == 1]), ""]
        if len(ok) > 2:
            _, slope = fit([(r["params"]["silver_rows_after"], t(r, "stage.silver")) for r in ok[1:]])
            md += [f"Fitted: Silver time grows {slope * 1e6 * 1000:.2f} ms per million rows already in Silver.", ""]

    small = load(results, "small_files")
    if small:
        ok = [r for r in small if "wall_s" in r]
        md += ["## Many small batches (1,000 rows each, no maintenance)", "",
               table(["batch", "wall", "Silver", "catalog requests", "storage GETs", "outcome"],
                     [[r["params"]["batch"], fmt_s(r.get("wall_s", 0)) if "wall_s" in r else "", fmt_s(t(r, "stage.silver")) if "times" in r else "",
                       sum(v for k, v in r.get("counts", {}).items() if k.startswith("http.catalog.")),
                       r.get("counts", {}).get("http.storage.GET", 0), r.get("broke", "ok")]
                      for r in small if r["params"]["batch"] in (1, 10, 50, 100, 150, 200, 250, 300) or "broke" in r]), ""]
        if len(ok) > 10:
            _, slope = fit([(r["params"]["batch"], r["wall_s"]) for r in ok])
            md += [f"Fitted: each earlier commit adds {slope * 1000:.1f} ms to the next batch until maintenance runs.", ""]

    md += cost_section(results)
    text = "\n".join(md) + "\n"
    if a.out:
        Path(a.out).write_text(text)
    else:
        print(text, end="")
    print(f"wrote {a.out}")


def cost_section(results: Path) -> list[str]:
    """AWS usage cost of the target volume, from per-unit usage measured above."""
    rows, known, new = load(results, "rows"), load(results, "known_subjects"), load(results, "new_subjects")
    ok_rows = [r for r in rows if "wall_s" in r and r["params"]["rows"] >= 100_000]
    ok_known, ok_new = [r for r in known if "wall_s" in r], [r for r in new if "wall_s" in r]
    if not (ok_rows and ok_known and ok_new):
        return ["## AWS cost of the target", "", "Not enough measurements yet.", ""]
    big = max(ok_rows, key=lambda r: r["params"]["rows"])
    per_row = usage(big).scaled(1 / big["params"]["rows"])
    k = max(ok_known, key=lambda r: r["params"]["known_subjects"])
    per_known = usage(k).scaled(1 / k["params"]["known_subjects"])
    n = max(ok_new, key=lambda r: r["params"]["new_subjects"])
    per_new = usage(n).scaled(1 / n["params"]["new_subjects"])
    tg = TARGET
    daily_mentions = (tg["sources"] - tg["eligibility_sources"]) * tg["members_per_batch"] + tg["eligibility_sources"] * tg["members"]
    day = per_row.scaled(tg["rows_per_day"]) + per_known.scaled(daily_mentions) + per_new.scaled(tg["new_members_per_day"])
    backfill = per_new.scaled(tg["members"] * tg["sources"])  # every source's members are subjects of that source
    d, b = day.dollars(), backfill.dollars()
    storage = tg["silver_tb"] * 1024 * PRICES["s3.gb_month"]
    lines = ["## AWS cost of the target (usage only)", "",
             f"Target: {tg['members']:,} members, {tg['sources']} sources ({tg['eligibility_sources']} of them full eligibility files), "
             f"{tg['rows_per_day']:,} rows a day, {tg['members_per_batch']:,} members mentioned per claims batch, "
             f"{tg['silver_tb']} TB in Silver.", "",
             table(["", "S3 requests", "KMS", "DynamoDB", "compute", "total"],
                   [["per day", *(f"${d[x]:,.2f}" for x in ("s3_requests", "kms", "dynamodb", "compute", "total"))],
                    ["per month", *(f"${d[x] * 30:,.0f}" for x in ("s3_requests", "kms", "dynamodb", "compute", "total"))],
                    ["initial backfill (all members, all sources)", *(f"${b[x]:,.0f}" for x in ("s3_requests", "kms", "dynamodb", "compute", "total"))]]),
             "",
             f"Plus storage: {tg['silver_tb']} TB in S3 Standard is ${storage:,.0f} a month (Bronze and snapshots add to it).",
             f"Key requests a day: {day.kms_requests:,.0f} (one per member per batch); at KMS's 50,000 requests/s that "
             f"alone is {day.kms_requests / 50_000 / 60:.0f} minutes of the account's shared quota.",
             f"Backfill time at the measured per-member rate, one loader: {fmt_s(backfill.task_seconds)}.",
             f"Streaming: Firehose rounds each record up to 5 KB, so 50M events of 300 bytes cost "
             f"${firehose_dollars(50_000_000, 300):,.2f} a day instead of ${50_000_000 * 300 / 1024 ** 3 * PRICES['firehose.gb']:,.2f}.",
             "", "Fixed infrastructure (Polaris and Prefect on Fargate, RDS, NAT gateway, Doris hosts) is separate and does not grow with data.", ""]
    return lines


if __name__ == "__main__":
    main()
