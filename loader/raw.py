"""The raw zone: partners drop files under drops/<source>/; the loader lands them unchanged.

    raw/drops/<source>/<anything>                                    where files arrive
    raw/landed/<source>/<entity>/ingest_date=<d>/batch_id=<id>/data.<ext>   exact bytes, for replay
    raw/quarantine/<source>/batch_id=<id>/data.<ext>                 rejected batches

Original file names never leave the drop prefix: partners put patient names in file names, and object
keys reach access logs. Only a hash of the name is kept. The raw bucket is plaintext and expires on a
short lifecycle. Loads reach it with credentials scoped to one source's prefixes
(RawZone.for_contract, loader/partners.py), never the lake's credentials; the unscoped zone is for
setup, tests and the local mail receiver.
"""
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone

from loader.contracts import Contract
from loader.settings import Settings

LOGICAL = "raw"
EXPIRY_DAYS = 30  # the lifecycle comes from stack/bootstrap/model.json (a test keeps them equal)
EXTENSIONS = {"csv": "csv", "ndjson": "ndjson", "parquet": "parquet", "json_pages": "json"}


@dataclass
class Landed:
    batch_id: str
    raw_key: str
    content_sha256: str
    name_sha256: str
    size: int
    landed_at: str


def drop_prefix(contract: Contract) -> str:
    return f"drops/{contract.source}/"


def batch_id(contract: Contract, content_sha256: str) -> str:
    """Same source and same bytes give the same batch: a re-dropped file is a no-op."""
    return f"{contract.source}-{content_sha256[:24]}"


class RawZone:
    def __init__(self, s: Settings, scoped: dict | None = None):
        self.bucket = s.bucket(LOGICAL)
        self.s3 = s.s3(scoped) if scoped else s.s3("loader-raw")

    @classmethod
    def for_contract(cls, s: Settings, contract: Contract) -> "RawZone":
        """Only this source's prefixes (and its mailbox or staging prefix)."""
        from loader import partners

        return cls(s, s.scoped_storage(partners.raw_scope(contract), contract.source))

    @classmethod
    def for_source(cls, s: Settings, source: str) -> "RawZone":
        """Only this source's drop, landed and quarantine prefixes (shred, offboarding)."""
        from loader import partners

        return cls(s, s.scoped_storage({partners.RAW: partners.source_prefixes(source)}, source))

    def uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"

    def put(self, key: str, body: bytes) -> None:
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body)

    def delete(self, key: str) -> None:
        self.s3.delete_object(Bucket=self.bucket, Key=key)

    def keys(self, prefix: str) -> list[str]:
        out = []
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix):
            out += [o["Key"] for o in page.get("Contents", [])]
        return out

    def read(self, key: str) -> bytes:
        return self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def drop(self, contract: Contract, name: str, body: bytes) -> None:
        """What a partner does: put a file in the source's drop prefix."""
        self.put(f"{drop_prefix(contract)}{name}", body)

    def pending_drops(self, contract: Contract) -> list[str]:
        """Keys waiting in the drop prefix, oldest first."""
        objects = []
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=drop_prefix(contract)):
            objects += [o for o in page.get("Contents", []) if not o["Key"].endswith("/")]
        return [o["Key"] for o in sorted(objects, key=lambda o: (o["LastModified"], o["Key"]))]

    def land(self, contract: Contract, drop_key: str) -> Landed:
        """Copy one dropped file to its landed key (server side) and return its identity.

        The drop is deleted by the caller only after the manifest records the batch.
        """
        # Hashed as it streams: the plaintext file never touches local disk.
        digest, size = hashlib.sha256(), 0
        for chunk in self.s3.get_object(Bucket=self.bucket, Key=drop_key)["Body"].iter_chunks(1 << 20):
            digest.update(chunk)
            size += len(chunk)
        content = digest.hexdigest()
        now = datetime.now(timezone.utc)
        bid = batch_id(contract, content)
        key = (f"landed/{contract.source}/{contract.entity}/ingest_date={now:%Y-%m-%d}/batch_id={bid}/"
               f"data.{EXTENSIONS[contract.format.type]}")
        self.s3.copy_object(Bucket=self.bucket, Key=key, CopySource={"Bucket": self.bucket, "Key": drop_key})
        return Landed(batch_id=bid, raw_key=key, content_sha256=content,
                      name_sha256=hashlib.sha256(drop_key.encode()).hexdigest(), size=size,
                      landed_at=now.isoformat(timespec="microseconds"))

    def quarantine(self, contract: Contract, raw_key: str, bid: str) -> str:
        key = f"quarantine/{contract.source}/batch_id={bid}/data.{EXTENSIONS[contract.format.type]}"
        self.s3.copy_object(Bucket=self.bucket, Key=key, CopySource={"Bucket": self.bucket, "Key": raw_key})
        self.delete(raw_key)
        return key
