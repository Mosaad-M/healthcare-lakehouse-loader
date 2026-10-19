"""Run inside the compose network: python -m stack.bootstrap.doris

Waits for the backend, sets the root password (from .env) on a fresh install, creates one Doris
catalog per Polaris catalog (as `doris_reader`, vended credentials), builds the serving copies from
Gold as the refresher, and applies users, grants and row policies. Idempotent.
"""
import os
import time

import pymysql

from loader import doris, gold, serving, settings
from stack.bootstrap.__main__ import MODEL, log


def root_connection(timeout: float = 1200):
    """Root with its password; on a fresh install root has none yet, so set it first. Waits for the
    frontend to accept connections (on AWS the host may still be pulling images)."""
    password, deadline = os.environ["DORIS_ROOT_PASSWORD"], time.time() + timeout
    while True:
        try:
            return doris.connect("root", password)
        except pymysql.err.OperationalError as err:
            if err.args and err.args[0] == 1045:  # access denied: a fresh install without a password
                con = doris.connect("root", "")
                doris.query(con, "SET PASSWORD FOR 'root' = PASSWORD(%s)", (password,))
                log("doris root password set")
                return doris.connect("root", password)
            if time.time() > deadline:
                raise
            time.sleep(10)


def wait_for_backend(con, timeout: float = 900) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        rows = doris.query(con, "SHOW BACKENDS")
        if rows and all(str(r[9]).lower() == "true" for r in rows):  # column 9: Alive
            return
        time.sleep(3)
    raise TimeoutError("Doris backend did not become alive")


def main() -> None:
    s = settings.load()
    con = root_connection()
    wait_for_backend(con)
    client_id, secret = s.principal("doris_reader")
    for catalog in MODEL["catalogs"]:
        doris.query(con, f"DROP CATALOG IF EXISTS lake_{catalog}")  # picks up a rotated secret
        doris.query(con, f"""CREATE CATALOG lake_{catalog} PROPERTIES (
            'type' = 'iceberg', 'iceberg.catalog.type' = 'rest', 'warehouse' = '{catalog}',
            'iceberg.rest.uri' = '{s.polaris_uri}', 'iceberg.rest.security.type' = 'oauth2',
            'iceberg.rest.oauth2.credential' = '{client_id}:{secret}',
            'iceberg.rest.oauth2.server-uri' = '{s.token_uri}',
            'iceberg.rest.oauth2.scope' = 'PRINCIPAL_ROLE:ALL',
            'iceberg.rest.vended-credentials-enabled' = 'true',
            's3.region' = 'us-east-1', 'enable.mapping.varbinary' = 'true',
            'meta.cache.iceberg.table.ttl-second' = '0')""")
        log(f"doris catalog lake_{catalog} created")
    serving.setup(con)
    copies = serving.refresh(s, gold.run(s))
    serving.apply_policies(con)  # after the copies exist: a policy needs its table
    log(f"doris serving copies ready: {copies}")
    log("doris done")


if __name__ == "__main__":
    main()
