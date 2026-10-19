"""A mock partner API for the http_api fetcher (synthetic data only).

    GET  /v1/encounters?from=YYYY-MM-DD&to=YYYY-MM-DD&page=N&page_size=M   bearer token required
    POST /admin/config  {"tag": "...", "version": 1, "per_day": 6, "fail_next": 0}
    GET  /health

Encounters are deterministic for (tag, version, date): bumping `version` restates every amount, as
a partner correcting recent days would. `fail_next` makes the next N data requests answer 503.
Patient 0 of every tag carries the canary identifiers (loader.synthetic.Canaries).
"""
import json
import os
import random
import threading
from datetime import date, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from loader.synthetic import Canaries, member

STATE = {"tag": "default0", "version": 1, "per_day": 6, "fail_next": 0, "patients": 30}
LOCK = threading.Lock()
TOKEN = os.environ["LOADER_SOURCE_SECRET_ACME_PARTNER_API"]
FACILITIES = ["F001", "F002", "F003", "F004"]
DIAGNOSES = ["E11.9", "I10", "J45.909", "M54.5", "Z00.00"]


def encounters(start: date, end: date) -> list[dict]:
    with LOCK:
        tag, version, per_day, patients = STATE["tag"], STATE["version"], STATE["per_day"], STATE["patients"]
    canaries, rows, day = Canaries(tag), [], start
    while day <= end:
        rnd = random.Random(f"{tag}-{version}-{day}")
        for i in range(per_day):
            m = member(canaries, (day.toordinal() + i) % patients)
            rows.append({"encounter_id": f"E{tag}{day:%Y%m%d}{i:03d}", "patient_mrn": m["mrn"], "patient_name": m["name"],
                         "encounter_date": day.isoformat(), "facility_id": rnd.choice(FACILITIES),
                         "diagnosis_code": rnd.choice(DIAGNOSES), "charge_amount": round(rnd.uniform(40, 900), 2)})
        day += timedelta(days=1)
    return rows


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):  # no request logging: query strings and paths stay out of logs
        pass

    def send(self, code: int, body: dict) -> None:
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        url = urlparse(self.path)
        if url.path == "/health":
            return self.send(200, {"ok": True})
        if url.path != "/v1/encounters":
            return self.send(404, {"error": "not found"})
        if self.headers.get("Authorization") != f"Bearer {TOKEN}":
            return self.send(401, {"error": "unauthorized"})
        with LOCK:
            if STATE["fail_next"] > 0:
                STATE["fail_next"] -= 1
                return self.send(503, {"error": "try again"})
        q = {k: v[0] for k, v in parse_qs(url.query).items()}
        rows = encounters(date.fromisoformat(q["from"]), date.fromisoformat(q["to"]))
        page, size = int(q.get("page", 1)), int(q.get("page_size", 100))
        chunk = rows[(page - 1) * size: page * size]
        self.send(200, {"data": chunk, "page": page, "next_page": page + 1 if page * size < len(rows) else None})

    def do_POST(self):
        if urlparse(self.path).path != "/admin/config":
            return self.send(404, {"error": "not found"})
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
        with LOCK:
            STATE.update({k: v for k, v in body.items() if k in STATE})
            return self.send(200, dict(STATE))


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
