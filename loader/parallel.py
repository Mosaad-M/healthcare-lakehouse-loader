"""Run independent I/O-bound calls (DynamoDB, KMS) concurrently.

Subject resolution and key handling are one request per subject. Done one after another, that is
latency times subjects: about 21 ms per new subject locally (benchmark), so a million new members
took hours. boto3 clients are thread-safe, so a thread pool removes the latency without changing
what is written or what it costs (AWS bills per item, not per connection).
"""
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Iterable, TypeVar

T, R = TypeVar("T"), TypeVar("R")
WORKERS = int(os.environ.get("LOADER_IO_WORKERS", "32"))


def map_io(fn: Callable[[T], R], items: Iterable[T]) -> list[R]:
    """fn over items, concurrently, results in input order; the first error is raised."""
    items = list(items)
    if len(items) < 2 or WORKERS < 2:
        return [fn(item) for item in items]
    with ThreadPoolExecutor(max_workers=min(WORKERS, len(items))) as pool:
        return list(pool.map(fn, items))
