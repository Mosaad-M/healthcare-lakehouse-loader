"""Serving copies in Doris: refreshed by the pipeline right after Gold, never by cache
expiry. Each copy is an internal Doris table rebuilt with INSERT OVERWRITE from the Gold table it
serves, and the Gold snapshot it holds is recorded. Copies hold de-identified data or still-encrypted
identifiers only: they are built from Gold, which never holds plaintext identifiers.
"""
import os
import re
from datetime import datetime, timezone
from pathlib import Path

from loader import doris
from loader.gold import NAMESPACE, Model
from loader.polaris import Polaris
from loader.settings import Settings

DATABASE = "serving"
REFRESHER = "serving_refresher"
REFRESHES = "serving_refresh"
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
