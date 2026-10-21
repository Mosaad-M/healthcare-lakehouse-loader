"""Write synthetic claims files into a benchmark source's drop prefix, generated inside DuckDB.

Member m (0 <= m < subjects) appears on rows i with i % subjects == m, so every batch of at least
`subjects` rows mentions exactly `subjects` distinct members. Claim IDs start at `offset`, so later
batches add new claims; service dates spread over `days` days from day `first_day` of 2026 (a
batch with a later first day does not restate earlier windows, so Silver grows). Ascending claim IDs
let file statistics skip old files in a key MERGE; --random-keys takes that away, as real keys do.
"""
import argparse

import duckdb

from loader import duck, raw, settings
from bench import sources


def generate(s, source: str, rows: int, subjects: int, offset: int = 0, days: int = 30, files: int = 1,
             first_day: int = 0, random_keys: bool = False, stride: int = 1, cdc_op: str | None = None,
             cdc_seq: int = 0) -> list[str]:
    """`stride` spreads a batch's keys over a larger key range (key i * stride). `cdc_op` adds a change
    feed's columns: that op ('I' or 'U'; with 'U' every 20th row is a 'D'), sequence cdc_seq + i."""
    con = duckdb.connect()
    duck.load_extensions(con, s)
    con.execute(s.duckdb_s3_secret("bench_admin", f"s3://{s.bucket(raw.LOGICAL)}/", "admin"))
    cdc = (f", CASE WHEN '{cdc_op}' = 'U' AND i % 20 = 0 THEN 'D' ELSE '{cdc_op}' END AS cdc_op, "
           f"{cdc_seq} + i AS cdc_seq") if cdc_op else ""
    keys, per_file = [], -(-rows // files)
    for f in range(files):
        start, count = offset + f * per_file, min(per_file, rows - f * per_file)
        if count <= 0:
            break
        key = f"drops/{source}/bench-{start:012d}-{count}.csv"
        con.execute(f"""
            COPY (
              SELECT {"md5(i::VARCHAR)" if random_keys else "'C' || lpad(i::VARCHAR, 12, '0')"} AS claim_id,
                     'M' || lpad((i % {subjects})::VARCHAR, 9, '0') AS member_id,
                     lpad(((i % {subjects}) * 7919 % 1000000000)::VARCHAR, 9, '0') AS member_ssn,
                     'Synthetic Member ' || (i % {subjects}) AS member_name,
                     DATE '2026-01-01' + ({first_day} + i % {days})::INTEGER AS service_date,
                     'Z' || lpad((i % 997)::VARCHAR, 3, '0') AS diagnosis_code,
                     lpad((99200 + i % 15)::VARCHAR, 5, '0') AS procedure_code,
                     lpad((1000000000 + i % 50000)::VARCHAR, 10, '0') AS provider_npi,
                     round(50 + (i % 5000) / 7.0, 2) AS billed_amount,
                     round(40 + (i % 5000) / 9.0, 2) AS paid_amount{cdc}
              FROM (SELECT i * {stride} AS i FROM range({start}, {start + count}) t(i))
            ) TO 's3://{s.bucket(raw.LOGICAL)}/{key}' (FORMAT csv, HEADER true)""")
        keys.append(key)
    return keys


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--name", required=True)
    p.add_argument("--rows", type=int, required=True)
    p.add_argument("--subjects", type=int, required=True)
    p.add_argument("--offset", type=int, default=0)
    p.add_argument("--days", type=int, default=30)
    p.add_argument("--files", type=int, default=1)
    p.add_argument("--first-day", type=int, default=0)
    p.add_argument("--random-keys", action="store_true", help="claim IDs in random order (md5), not ascending")
    p.add_argument("--stride", type=int, default=1)
    p.add_argument("--cdc-op", choices=["I", "U"])
    p.add_argument("--cdc-seq", type=int, default=0)
    a = p.parse_args()
    keys = generate(settings.load(), sources.contract(a.name).source, a.rows, a.subjects, a.offset, a.days, a.files,
                    a.first_day, a.random_keys, a.stride, a.cdc_op, a.cdc_seq)
    print(f"generated {a.rows} rows in {len(keys)} files")
