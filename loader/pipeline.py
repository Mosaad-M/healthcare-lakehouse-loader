"""One ingest pass for a source: land new drops, then take every unfinished batch through Bronze and
Silver in landing order. Safe to rerun at any point: each step is idempotent and the manifest
records how far every batch got."""
import logging
from dataclasses import dataclass, field

import botocore.exceptions

from loader import bronze, fetch, integrity, raw, silver, tables
from loader.contracts import Contract
from loader.settings import Settings
from loader.state import (BRONZE_COMMITTED, DONE, LANDED, QUARANTINED, SILVER_COMMITTED, Batch, Manifest,
                          table_lock)

log = logging.getLogger(__name__)
# A writer that finds its table broken repairs it; after a rollback the pass starts over (batches were
# reset). More than this many in one pass means something keeps deleting files: stop and alert.
MAX_REPAIRS_PER_PASS = 3


@dataclass
class Summary:
    landed: int = 0
    duplicates: int = 0
    loaded: list[str] = field(default_factory=list)
    quarantined: list[str] = field(default_factory=list)


def land_drops(s: Settings, contract: Contract, summary: Summary) -> None:
    zone, manifest = raw.RawZone.for_contract(s, contract), Manifest(s)
    for drop_key in zone.pending_drops(contract):
        try:
            landed = zone.land(contract, drop_key)
        except botocore.exceptions.ClientError as err:
            if err.response["Error"]["Code"] not in ("NoSuchKey", "404"):
                raise
            # Another run (scheduled and manual, or two workers) landed and removed it first.
            log.info("drop already taken by another run")
            continue
        batch = Batch(batch_id=landed.batch_id, source=contract.source, state=LANDED, raw_key=landed.raw_key,
                      content_sha256=landed.content_sha256, name_sha256=landed.name_sha256,
                      size=landed.size, landed_at=landed.landed_at)
        if manifest.add(batch):
            summary.landed += 1
            log.info("landed batch %s (%d bytes)", batch.batch_id, batch.size)
        else:
            # The same bytes were dropped before: keep the first landing, discard this copy.
            known = manifest.get(batch.batch_id)
            if known and known.raw_key != landed.raw_key:
                zone.delete(landed.raw_key)
            summary.duplicates += 1
            log.info("duplicate drop of batch %s ignored", batch.batch_id)
        zone.delete(drop_key)


def to_bronze(s: Settings, contract: Contract, batch: Batch, summary: Summary) -> Batch:
    manifest = Manifest(s)
    try:
        with table_lock(s, f"{tables.CATALOG}.{tables.BRONZE}.{contract.table}"):
            rows = bronze.write(s, contract, batch)
    except bronze.Quarantine as reason:
        key = raw.RawZone.for_contract(s, contract).quarantine(contract, batch.raw_key, batch.batch_id)
        manifest.advance(batch.batch_id, QUARANTINED, reason=str(reason), raw_key=key)
        summary.quarantined.append(batch.batch_id)
        log.warning("batch %s quarantined: %s", batch.batch_id, reason)
        return manifest.get(batch.batch_id)
    manifest.advance(batch.batch_id, BRONZE_COMMITTED, rows_bronze=rows)
    log.info("batch %s: %d rows committed to Bronze", batch.batch_id, rows)
    return manifest.get(batch.batch_id)


def to_silver(s: Settings, contract: Contract, batch: Batch, summary: Summary) -> Batch:
    manifest = Manifest(s)
    with table_lock(s, f"{tables.CATALOG}.{tables.SILVER}.{contract.table}"):
        rows = silver.apply(s, contract, batch)
    manifest.advance(batch.batch_id, SILVER_COMMITTED, rows_silver=rows)
    summary.loaded.append(batch.batch_id)
    log.info("batch %s applied to Silver (%s); Silver has %d rows", batch.batch_id,
             contract.silver.semantics, rows)
    return manifest.get(batch.batch_id)


def ingest(s: Settings, contract: Contract) -> Summary:
    from loader import offboarding

    offboarding.check_active(s, contract)  # a frozen partner's sources are not loaded
    summary = Summary()
    fetch.fetch(s, contract)  # pull sources put their data in the drop prefix first
    land_drops(s, contract, summary)
    for repairs in range(MAX_REPAIRS_PER_PASS + 1):
        try:
            for batch in Manifest(s).for_source(contract.source):
                if batch.state in DONE:
                    continue
                if batch.state == LANDED:
                    batch = to_bronze(s, contract, batch, summary)
                if batch.state == BRONZE_COMMITTED:
                    to_silver(s, contract, batch, summary)
            return summary
        except integrity.TableRepaired as err:
            if repairs == MAX_REPAIRS_PER_PASS:
                raise
            log.error("ingest of %s: %s; starting over in landing order", contract.source, err)
    raise AssertionError("unreachable")


def replay(s: Settings, contract: Contract) -> int:
    """Rebuild Bronze and Silver for every loaded batch from the raw zone, in landing order.

    Only possible within the raw zone's retention. Identifiers are re-encrypted with fresh
    nonces, so ciphertext differs while subject IDs and decrypted values are identical. Returns the
    number of batches replayed.
    """
    from loader import offboarding

    offboarding.check_active(s, contract)
    summary, count = Summary(), 0
    for batch in Manifest(s).for_source(contract.source):
        if batch.state == QUARANTINED:
            continue
        if not batch.raw_purged:  # a shred deleted the raw file: Bronze keeps the batch as it is
            with table_lock(s, f"{tables.CATALOG}.{tables.BRONZE}.{contract.table}"):
                bronze.write(s, contract, batch)
        to_silver(s, contract, batch, summary)
        count += 1
    return count


__all__ = ["Summary", "ingest", "replay", "SILVER_COMMITTED"]
