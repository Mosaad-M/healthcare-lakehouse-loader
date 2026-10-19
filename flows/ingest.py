"""The ingest flow: one deployment per source contract, run on the contract's polling interval."""
from pathlib import Path

from prefect import flow, get_run_logger

from loader import contracts, duck, pipeline, settings


class IngestFailed(RuntimeError):
    """Raised with a redacted message only, so no data value reaches Prefect's logs or state."""


@flow(name="ingest", log_prints=False)
def ingest(source: str, contracts_dir: str = "contracts") -> None:
    logger = get_run_logger()
    contract = contracts.load_all(Path(contracts_dir))[source]
    try:
        summary = pipeline.ingest(settings.load(), contract)
    except Exception as err:  # noqa: BLE001 - every error is redacted before it leaves the flow
        message = duck.redact(err)
        logger.error("ingest of %s failed: %s", source, message)
        raise IngestFailed(message) from None
    logger.info("ingest of %s: %d landed, %d duplicate, %d loaded, %d quarantined", source,
                summary.landed, summary.duplicates, len(summary.loaded), len(summary.quarantined))
