"""The Gold flow: rebuild Gold with dbt, then refresh the Doris copies from it."""
import os

from prefect import flow, get_run_logger

from loader import duck, gold, serving, settings


def doris_running() -> bool:
    import socket

    host = os.environ.get("DORIS_HOST")
    if not host:
        return False
    try:
        socket.create_connection((host, 9030), timeout=2).close()
        return True
    except OSError:
        return False


class GoldRefreshFailed(RuntimeError):
    """Raised with a redacted message only."""


@flow(name="gold", log_prints=False)
def refresh(serve: bool = True) -> None:
    logger = get_run_logger()
    s = settings.load()
    try:
        models = gold.run(s)
        if not models:
            logger.info("gold unchanged since the last run: nothing rebuilt or copied")
            return
        logger.info("gold rebuilt: %s", ", ".join(m.qualified for m in models))
        if serve and doris_running():
            copies = serving.refresh(s, models)
            logger.info("serving copies refreshed: %s", copies)
        elif serve:
            logger.warning("Doris is not running (stack started without the doris profile): serving skipped")
    except Exception as err:  # noqa: BLE001 - every error is redacted before it leaves the flow
        message = duck.redact(err)
        logger.error("gold refresh failed: %s", message)
        raise GoldRefreshFailed(message) from None
