"""Receives change events from Debezium Server (HTTP sink) and writes them into the source's
staging bucket (as DMS does on AWS) as change files in AWS DMS's shape, so one contract serves both platforms: Parquet, one row
per change, the row's columns plus `Op` (I, U or D) and `change_seq` (here the change's log
position; DMS's $AR_H_CHANGE_SEQ on AWS).

Each event is appended to a spool file on a volume and fsynced before the 200 reply, so an
acknowledged event survives a restart; every few seconds the spool is written as one file. On AWS,
DMS writes the change files to its staging bucket and this service is not needed.
"""
import io
import json
import os
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from loader import contracts, settings

SPOOL = Path(os.environ.get("CDC_SPOOL", "/spool"))
SOURCE = os.environ.get("CDC_SOURCE", "acme_clinic_patients")
FLUSH_SECONDS = float(os.environ.get("CDC_FLUSH_SECONDS", "5"))
LOCK = threading.Lock()


OPS = {"c": "I", "r": "I", "u": "U", "d": "D"}  # Debezium's create, read (snapshot), update, delete


def as_dms(ndjson: bytes) -> bytes:
    """Debezium's flattened events (row plus __op, __lsn, ...) as a DMS-style Parquet change file."""
    rows = []
    for line in ndjson.splitlines():
        event = json.loads(line)
        row = {"Op": OPS[event["__op"]], "change_seq": str(event["__lsn"])}
        row.update({k: v for k, v in event.items() if not k.startswith("__")})
        rows.append(row)
    out = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows), out)
    return out.getvalue()


def current() -> Path:
    return SPOOL / "current.ndjson"


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):  # never log events: they carry identifiers
        pass

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if body.strip():
            with LOCK, open(current(), "ab") as spool:
                spool.write(body.strip().replace(b"\n", b" ") + b"\n")
                spool.flush()
                os.fsync(spool.fileno())
        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        self.send_response(200 if self.path == "/health" else 404)
        self.end_headers()


def flusher() -> None:
    s = settings.load()
    staged = contracts.load_all()[SOURCE].fetch
    s3, bucket = s.s3("cdc-sink"), s.bucket(staged.bucket)  # put-only, its own bucket (like DMS)
    while True:
        time.sleep(FLUSH_SECONDS)
        with LOCK:
            if not current().exists() or current().stat().st_size == 0:
                continue
            ready = SPOOL / f"ready-{datetime.now(timezone.utc):%Y%m%dT%H%M%S%f}.ndjson"
            current().rename(ready)
        for path in sorted(SPOOL.glob("ready-*.ndjson")):  # also anything left by a restart
            s3.put_object(Bucket=bucket, Key=f"{staged.prefix}cdc-{path.stem.removeprefix('ready-')}.parquet",
                          Body=as_dms(path.read_bytes()))
            path.unlink()


if __name__ == "__main__":
    SPOOL.mkdir(parents=True, exist_ok=True)
    threading.Thread(target=flusher, daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", 8090), Handler).serve_forever()
