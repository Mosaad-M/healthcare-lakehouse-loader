"""A trivial flow proving the worker can start a toolbox container that reaches the stack.

Flows return nothing: Prefect result persistence stays off so no data lands in Prefect's storage.
Flow-run containers get no admin credentials (no Polaris root, no RustFS admin key).
"""
from prefect import flow

from loader import settings
from loader.polaris import Polaris, expect


@flow(name="smoke")
def smoke() -> None:
    """Fails unless the loader principal can authenticate and see both catalogs."""
    s = settings.load()
    loader = Polaris(s, "loader")
    for catalog in ("phi", "deid"):
        expect(loader.rest("GET", f"{s.polaris_uri}/v1/config?warehouse={catalog}"), 200)
