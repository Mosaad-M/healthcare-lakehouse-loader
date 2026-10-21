import itertools
import os
import uuid

import pytest

from loader import duck, settings
from loader.polaris import Polaris

_counter = itertools.count()
# Tests load a batch and run Gold at once: no settling time for the incremental dimensions'
# horizon (loader/gold.py history_horizons; tests/unit/test_gold_history.py sets its own).
os.environ.setdefault("LOADER_GOLD_SETTLE_SECONDS", "0")


@pytest.fixture(scope="session")
def s() -> settings.Settings:
    return settings.load()


@pytest.fixture(scope="session")
def run_id() -> str:
    return uuid.uuid4().hex[:8]


@pytest.fixture(scope="session")
def contracts_dir(tmp_path_factory):
    """Per-run contract copies (see tests/pipeline/conftest.py: deploy_copy)."""
    return tmp_path_factory.mktemp("contracts")


@pytest.fixture(scope="session")
def canaries(run_id):
    from loader.synthetic import Canaries

    return Canaries(run_id)


@pytest.fixture(scope="session")
def ns(s, run_id):
    """A fresh namespace in the `phi` catalog per test session, removed afterwards."""
    name = f"test_{run_id}"
    con = duck.connect(s, "phi")
    con.execute(f"CREATE SCHEMA ice.{name}")
    yield name
    purge_namespace(Polaris(s, "loader"), "phi", name)


def scan_all(s, table: str) -> list[dict]:
    """Every item of a DynamoDB table (a single scan call stops at 1 MB)."""
    return [item for page in s.dynamodb().get_paginator("scan").paginate(TableName=table) for item in page["Items"]]


def purge_namespace(polaris: Polaris, catalog: str, namespace: str) -> None:
    """Drop every table with purge (data files removed), then the namespace."""
    url = polaris.table_url(catalog, namespace)
    status, body = polaris.rest("GET", f"{url}/tables")
    for ident in (body or {}).get("identifiers", []) if status == 200 else []:
        polaris.rest("DELETE", polaris.table_url(catalog, namespace, ident["name"]) + "?purgeRequested=true")
    polaris.rest("DELETE", url)


@pytest.fixture
def con(s):
    return duck.connect(s, "phi")


@pytest.fixture
def polaris(s) -> Polaris:
    return Polaris(s, "loader")


@pytest.fixture
def table(ns):
    """Unique table name per test. Yields (short_name, qualified_name)."""
    name = f"t{next(_counter)}"
    yield name, f"ice.{ns}.{name}"


def pytest_collection_modifyitems(config, items):
    """`aws` tests need the AWS sandbox; `local` tests need the local stack (Spark Connect)."""
    import os

    on_aws = os.environ.get("LOADER_PLATFORM") == "aws"
    for item in items:
        if "aws" in item.keywords and not on_aws:
            item.add_marker(pytest.mark.skip(reason="needs the AWS sandbox (make sandbox-test)"))
        if "local" in item.keywords and on_aws:
            item.add_marker(pytest.mark.skip(reason="local stack only"))
