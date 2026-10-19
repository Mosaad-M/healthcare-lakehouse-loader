"""Synthetic files for the three example sources. No real person is ever involved.

Every identifier is fake and recognizable: SSNs use area number 900 (never issued), emails use the
reserved `.invalid` domain, and names carry the canary tag. Tests search the lake, the manifest and
the logs for these canaries to prove dropped identifiers and rejected rows never get there.
"""
import json
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

DIAGNOSES = ["E11.9", "I10", "J45.909", "M54.5", "Z00.00", "K21.9", "F41.1"]
PROCEDURES = ["99213", "99214", "80053", "85025", "93000", "36415"]
SPECIALTIES = ["Family Medicine", "Cardiology", "Endocrinology", "Pediatrics", "Radiology"]
LOINC = [("2345-7", "mg/dL"), ("4548-4", "%"), ("2093-3", "mg/dL"), ("718-7", "g/dL")]


FIRST = ["Avery", "Jordan", "Riley", "Quinn", "Morgan", "Casey", "Rowan", "Emerson"]
LAST = ["Fakeworth", "Testerly", "Synthson", "Mockford", "Placeholt", "Dummer"]


@dataclass(frozen=True)
class Canaries:
    """Values that must never appear in plaintext outside the raw zone. Member 0 of every generated
    file carries them; other members get ordinary fake identifiers."""

    tag: str

    @property
    def member_id(self) -> str:
        return f"MC{self.tag}"

    @property
    def mrn(self) -> str:
        return f"MRNC{self.tag}"

    @property
    def ssn(self) -> str:
        n = int(self.tag[:6], 16) % 10**6
        return f"900-{n // 10**4:02d}-{n % 10**4:04d}"

    @property
    def name(self) -> str:
        return f"Zqxcanary {self.tag}"

    @property
    def email(self) -> str:
        return f"canary-{self.tag}@example.invalid"

    def all(self) -> list[str]:
        return [self.ssn, self.name, self.email, self.member_id, self.mrn]


def member(canaries: Canaries, i: int) -> dict:
    """Fake member i; member 0 is the canary."""
    if i == 0:
        return {"id": canaries.member_id, "ssn": canaries.ssn, "name": canaries.name,
                "mrn": canaries.mrn, "email": canaries.email}
    return {"id": f"M{canaries.tag}{i:04d}", "ssn": f"900-{i % 100:02d}-{i:04d}",
            "name": f"{FIRST[i % len(FIRST)]} {LAST[i % len(LAST)]}", "mrn": f"MRN{canaries.tag}{i:04d}",
            "email": f"member{i}-{canaries.tag}@example.invalid"}


def claims_csv(canaries: Canaries, start: date, days: int, per_day: int, seed: int = 1,
               id_offset: int = 0, members: int = 7) -> bytes:
    """Claims for `days` service dates from `start`, spread over `members` fake members. Claim ids are
    stable per (date, index), so a later file restating the same window carries the same ids with
    corrected amounts."""
    rnd = random.Random(seed)
    lines = ["claim_id,member_id,member_ssn,member_name,service_date,diagnosis_code,procedure_code,"
             "provider_npi,billed_amount,paid_amount"]
    for d in range(days):
        day = start + timedelta(days=d)
        for i in range(per_day):
            billed = round(rnd.uniform(50, 900), 2)
            m = member(canaries, (d * per_day + i) % members)
            lines.append(",".join([
                f"C{day:%Y%m%d}{i + id_offset:05d}", m["id"], m["ssn"], m["name"], day.isoformat(),
                rnd.choice(DIAGNOSES), rnd.choice(PROCEDURES), f"{1000000000 + rnd.randrange(50):010d}",
                f"{billed:.2f}", f"{billed * rnd.uniform(0.4, 0.9):.2f}"]))
    return ("\n".join(lines) + "\n").encode()


def providers_csv(count: int, seed: int = 1, active_ratio: float = 0.9) -> bytes:
    rnd = random.Random(seed)
    lines = ["npi,provider_name,specialty,state,active"]
    for i in range(count):
        lines.append(f"{1000000000 + i:010d},Clinic {i:03d} Group,{rnd.choice(SPECIALTIES)},"
                     f"{rnd.choice(['MA', 'NY', 'CA', 'TX'])},{str(rnd.random() < active_ratio).lower()}")
    return ("\n".join(lines) + "\n").encode()


def lab_ndjson(canaries: Canaries, count: int, seed: int = 1, id_offset: int = 0, members: int = 5) -> bytes:
    rnd = random.Random(seed)
    base = datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)
    rows = []
    for i in range(count):
        code, unit = rnd.choice(LOINC)
        m = member(canaries, (i + id_offset) % members)
        rows.append(json.dumps({
            "result_id": f"R{i + id_offset:07d}", "patient_mrn": m["mrn"], "patient_email": m["email"],
            "collected_at": (base + timedelta(minutes=37 * (i + id_offset))).isoformat().replace("+00:00", "Z"),
            "test_code": code, "value": round(rnd.uniform(1, 300), 1), "unit": unit}))
    return ("\n".join(rows) + "\n").encode()


def claims_with_bad_date(canaries: Canaries) -> bytes:
    """A claims file whose service_date holds an identifier in one row: it must be quarantined, and
    the identifier must not reach any log, reason or table."""
    good = claims_csv(canaries, date(2026, 9, 1), days=1, per_day=3).decode().splitlines()
    fields = good[2].split(",")
    fields[4] = canaries.ssn
    good[2] = ",".join(fields)
    return ("\n".join(good) + "\n").encode()


def claims_with_extra_column(canaries: Canaries) -> bytes:
    """A claims file with an undeclared column (possibly new PHI): quarantined."""
    lines = claims_csv(canaries, date(2026, 9, 1), days=1, per_day=2).decode().splitlines()
    lines[0] += ",member_phone"
    lines[1:] = [line + ",555-0100" for line in lines[1:]]
    return ("\n".join(lines) + "\n").encode()
