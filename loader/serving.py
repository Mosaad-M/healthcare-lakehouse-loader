"""Serving copies in Doris: refreshed by the pipeline right after Gold, never by cache
expiry. Each copy is an internal Doris table rebuilt with INSERT OVERWRITE from the Gold table it
serves, and the Gold snapshot it holds is recorded. Copies hold de-identified data or still-encrypted
identifiers only: they are built from Gold, which never holds plaintext identifiers.
"""
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from loader import doris
from loader.gold import NAMESPACE, Model
from loader.polaris import Polaris
from loader.settings import Settings

DATABASE = "serving"
REFRESHER = "serving_refresher"
REFRESHES = "serving_refresh"
EXPORT_MARKS = "export_marks"  # DynamoDB: how far each export has copied
AUDIT_LOG = "__internal_schema.audit_log"
# Who ran what, against which tables, and how much it read and returned. Not the statement text or
# error messages: they can hold values a user typed.
AUDIT_FIELDS = ("query_id", "time", "user", "client_ip", "catalog", "db", "state", "error_code", "stmt_type",
                "queried_tables_and_views", "scan_rows", "return_rows", "sql_digest")
SQL = Path(__file__).parents[1] / "stack/doris"


def statements(path: Path) -> list[str]:
    text = re.sub(r"--[^\n]*", "", path.read_text())
    text = re.sub(r"\$\{(\w+)\}", lambda m: os.environ[m[1]], text)
    return [st.strip() for st in text.split(";") if st.strip()]


def apply_users(con) -> None:
    for st in statements(SQL / "users.sql"):
        doris.query(con, st)


def apply_grants(con) -> None:
    for st in statements(SQL / "grants.sql"):
        doris.query(con, st)


def apply_policies(con) -> None:
    for st in statements(SQL / "policies.sql"):
        doris.query(con, st)


def setup(con) -> None:
    """As root, once per install: the serving database, users (the refresher included) and row
    policies. The refresh itself runs as `serving_refresher`."""
    doris.query(con, f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    apply_users(con)


def _columns(con, table: str) -> list[str]:
    return [r[0] for r in doris.query(con, f"DESCRIBE {table}")]


# Text a spreadsheet would run as a formula: it starts with =, +, -, @, a tab or a carriage return.
FORMULA_START = r"^[-=+@\t\r]"


def safe_text(column: str) -> str:
    """Partner values reach people through these copies, often exported to spreadsheets, where a value
    such as =HYPERLINK(...) would run as a formula. Such values get a leading apostrophe (OWASP's
    advice for CSV injection), so they show as text. Bronze and Silver keep the value as received."""
    return f"CASE WHEN {column} REGEXP '{FORMULA_START}' THEN concat(\"'\", {column}) ELSE {column} END"


def _select(con, source: str) -> str:
    """Doris internal tables cannot hold VARBINARY: encrypted columns are copied as hex text of the
    same ciphertext (still unreadable without the subject's key). Text columns go through safe_text."""
    out = []
    for name, typ, *_ in doris.query(con, f"DESCRIBE {source}"):
        kind = typ.lower()
        if "binary" in kind:
            out.append(f"hex({name}) AS {name}")
        elif "char" in kind or "string" in kind or "text" in kind:
            out.append(f"{safe_text(name)} AS {name}")
        else:
            out.append(name)
    return ", ".join(out)


def refresh(s: Settings, models: list[Model]) -> dict[str, int]:
    """Rebuild every served copy from Gold, as `serving_refresher` (never root). Returns rows per copy."""
    from loader.gold import PRINCIPAL

    con = doris.connect(REFRESHER, os.environ["DORIS_REFRESHER_PASSWORD"])
    gold, out = Polaris(s, PRINCIPAL), {}
    for model in (m for m in models if m.meta.get("serve")):
        source = f"lake_{model.catalog}.{NAMESPACE}.{model.name}"
        copy = f"{DATABASE}.{model.name}"
        key = model.meta["key"]
        doris.query(con, f"REFRESH TABLE {source}")
        snapshot = gold.load_table(model.catalog, NAMESPACE, model.name)["metadata"].get("current-snapshot-id")
        exists = bool(doris.query(con, f"SHOW TABLES FROM {DATABASE} LIKE '{model.name}'"))
        if exists and _columns(con, copy) != _columns(con, source):
            doris.query(con, f"DROP TABLE {copy}")  # Gold's schema changed: rebuild the copy
            exists = False
        if not exists:
            if _columns(con, source)[0] != key:
                raise ValueError(f"{model.name}: the Doris key {key} must be the model's first column")
            doris.query(con, f"CREATE TABLE {copy} DUPLICATE KEY({key}) DISTRIBUTED BY HASH({key}) BUCKETS 4 "
                             f"PROPERTIES ('replication_num' = '1') AS SELECT {_select(con, source)} FROM {source}")
        else:
            doris.query(con, f"INSERT OVERWRITE TABLE {copy} SELECT {_select(con, source)} FROM {source}")
        rows = doris.query(con, f"SELECT count(*) FROM {copy}")[0][0]
        s.dynamodb().put_item(TableName=REFRESHES, Item={
            "copy": {"S": copy}, "gold_table": {"S": model.qualified}, "gold_snapshot": {"S": str(snapshot)},
            "rows": {"N": str(rows)}, "refreshed_at": {"S": datetime.now(timezone.utc).isoformat()}})
        out[copy] = rows
    apply_grants(con)
    return out


def grant_audit_read(con, timeout: float = 600) -> None:
    """As root: let the refresher read Doris's query audit table, which Doris creates a while after
    the backend joins (a grant on a missing table fails)."""
    deadline = time.time() + timeout
    while not doris.query(con, "SHOW TABLES FROM __internal_schema LIKE 'audit_log'"):
        if time.time() > deadline:
            raise TimeoutError("Doris did not create its audit table")
        time.sleep(5)
    doris.query(con, f"GRANT SELECT_PRIV ON internal.{AUDIT_LOG} TO ROLE 'refresher'")


def export_audit(s: Settings, margin_seconds: int = 300) -> int:
    """Copy Doris's query audit into the write-once journal (Object Lock), one object per day of
    queries, and return the rows copied. Doris keeps its audit table 30 days, and its administrator
    can change it; the journal keeps the copy for its retention. Queries newer than `margin_seconds`
    wait for the next run: Doris writes its audit table in batches, up to a minute late."""
    import pymysql

    from loader import journal

    con = doris.connect(REFRESHER, os.environ["DORIS_REFRESHER_PASSWORD"])
    ddb, key = s.dynamodb(), {"name": {"S": "doris_audit"}}
    item = ddb.get_item(TableName=EXPORT_MARKS, Key=key, ConsistentRead=True).get("Item")
    cutoff = doris.query(con, "SELECT date_sub(now(3), INTERVAL %s SECOND)", (margin_seconds,))[0][0]
    mark = (datetime.fromisoformat(item["through"]["S"]) if item
            else (doris.query(con, f"SELECT min(time) FROM {AUDIT_LOG}")[0][0] or cutoff) - timedelta(milliseconds=1))
    copied = 0
    while mark < cutoff:
        end = min(cutoff, mark + timedelta(days=1))
        with con.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(f"SELECT {', '.join(f'`{f}`' for f in AUDIT_FIELDS)} FROM {AUDIT_LOG} "
                        "WHERE time > %s AND time <= %s ORDER BY time, query_id", (mark, end))
            rows = cur.fetchall()
        if rows:
            body = "\n".join(json.dumps(r, default=str, sort_keys=True) for r in rows).encode()
            journal.client(s).put_object(Bucket=journal.bucket(s), ContentType="application/x-ndjson", Body=body,
                                         Key=f"doris-audit/{end:%Y/%m/%d}/{mark:%Y%m%dT%H%M%S%f}-{end:%Y%m%dT%H%M%S%f}.jsonl")
            copied += len(rows)
        ddb.put_item(TableName=EXPORT_MARKS, Item={**key, "through": {"S": end.isoformat()}})
        mark = end
    return copied
