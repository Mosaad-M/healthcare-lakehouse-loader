"""Counters and timers for one benchmark step, installed in that step's process only: the loader
itself has no benchmark hooks.

- HTTP requests made by DuckDB (storage and the Iceberg REST catalog), from DuckDB's own log, by
  method and service. The raw log can hold request headers (credentials), so it is reduced to
  counts and deleted at once.
- AWS API calls made through boto3 (DynamoDB, S3 for the raw zone), by service and operation.
- Key wraps and unwraps (a KMS call each on AWS).
- Wall time per pipeline stage, and the process's peak memory.
"""
import collections
import csv
import functools
import re
import resource
import shutil
import tempfile
import time
from pathlib import Path

import boto3
import duckdb

from loader import bronze, crypto, fetch, identity, pipeline, silver, vault

COUNTS: collections.Counter = collections.Counter()
TIMES: collections.defaultdict = collections.defaultdict(float)
_LOG_ROOT = Path(tempfile.mkdtemp(prefix="bench-duckdb-logs-"))
_connections = 0


def _timed(owner, name: str, label: str) -> None:
    original = getattr(owner, name)

    @functools.wraps(original)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        try:
            return original(*args, **kwargs)
        finally:
            TIMES[label] += time.perf_counter() - start
    setattr(owner, name, wrapper)


def _counted(owner, name: str, label: str) -> None:
    original = getattr(owner, name)

    @functools.wraps(original)
    def wrapper(*args, **kwargs):
        COUNTS[label] += 1
        return original(*args, **kwargs)
    setattr(owner, name, wrapper)


def install() -> None:
    global _connections
    real_connect = duckdb.connect

    def connect(*args, **kwargs):
        global _connections
        con = real_connect(*args, **kwargs)
        _connections += 1
        con.execute(f"CALL enable_logging('HTTP', storage='file', storage_path='{_LOG_ROOT / str(_connections)}')")
        return con
    duckdb.connect = connect

    boto3.setup_default_session()

    def on_call(event_name, **_):
        _, service, operation = event_name.split(".", 2)
        COUNTS[f"aws.{service}.{operation}"] += 1

    def on_params(event_name, params=None, **_):
        _, service, operation = event_name.split(".", 2)
        if service == "dynamodb":  # billing is per item: a batch call reads or writes many
            request = (params or {}).get("RequestItems") or {}
            if operation == "BatchGetItem":
                items = sum(len(t.get("Keys", [])) for t in request.values())
            elif operation == "BatchWriteItem":
                items = sum(len(t) for t in request.values())
            else:
                items = 1
            kind = "write" if operation in ("PutItem", "UpdateItem", "DeleteItem", "BatchWriteItem") else "read"
            COUNTS[f"ddb.items.{kind}"] += items
    boto3.DEFAULT_SESSION.events.register("before-call.*.*", on_call)
    boto3.DEFAULT_SESSION.events.register("before-parameter-build.*.*", on_params)

    for provider in (crypto.LocalKeyProvider, crypto.KmsKeyProvider):
        _counted(provider, "wrap", "keys.wrap")
        _counted(provider, "unwrap", "keys.unwrap")

    # Stages, outermost first; inner ones are included in their callers' time.
    _timed(fetch, "fetch", "stage.fetch")
    _timed(pipeline, "land_drops", "stage.land")
    _timed(pipeline, "to_bronze", "stage.bronze")
    _timed(pipeline, "to_silver", "stage.silver")
    _timed(bronze, "stage", "bronze.validate_and_encrypt")
    _timed(bronze, "encrypt_identifiers", "bronze.encrypt_identifiers")
    _timed(identity.Identity, "resolve", "identity.resolve")
    _timed(vault.KeyVault, "get_or_create", "vault.get_or_create")
    _timed(silver, "apply", "silver.apply")


_REQUEST = re.compile(r"'request': \{'type': (\w+), 'url': '([^']*)'")


def http_counts() -> collections.Counter:
    out = collections.Counter()
    for entries in _LOG_ROOT.glob("*/duckdb_log_entries.csv"):
        with open(entries, newline="") as handle:
            for row in csv.DictReader(handle):
                match = _REQUEST.search(row.get("message") or "")
                if match:
                    method, url = match.groups()
                    out[f"http.{'catalog' if '/api/catalog' in url else 'storage'}.{method}"] += 1
    return out


def report(extra: dict) -> dict:
    """All measurements of this step as one JSON-able dict; removes the raw DuckDB logs."""
    counts = COUNTS + http_counts()
    shutil.rmtree(_LOG_ROOT, ignore_errors=True)
    return {**extra, "times": {k: round(v, 3) for k, v in sorted(TIMES.items())}, "counts": dict(sorted(counts.items())),
            "peak_rss_mb": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)}
