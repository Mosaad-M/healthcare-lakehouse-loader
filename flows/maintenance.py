"""The maintenance flow: every table's health against its Polaris policies, actions run by Spark."""
from prefect import flow, get_run_logger

from loader import duck, maintenance, settings


class MaintenanceFailed(RuntimeError):
    """Raised with a redacted message only."""


@flow(name="maintenance", log_prints=False)
def maintain(only: list[str] | None = None) -> None:
    """Every table (the schedule), or only the tables named `catalog.namespace.table`."""
    logger = get_run_logger()
    try:
        runs = maintenance.maintain_all(settings.load(), only=only)
    except Exception as err:  # noqa: BLE001 - every error is redacted before it leaves the flow
        message = duck.redact(err)
        logger.error("maintenance failed: %s", message)
        raise MaintenanceFailed(message) from None
    for run in runs:
        if run.skipped:
            logger.warning("%s: %s", run.table, run.skipped)
        elif run.actions:
            logger.info("%s: %s; data files %d -> %d, delete files %d -> %d, snapshots %d -> %d", run.table,
                        ", ".join(a.kind for a in run.actions), run.before.data_files, run.after.data_files,
                        run.before.delete_files, run.after.delete_files, run.before.snapshots, run.after.snapshots)
