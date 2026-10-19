"""Doris over the MySQL protocol (FE port 9030)."""
import os

import pymysql


def connect(user: str = "root", password: str | None = None, database: str | None = None):
    if password is None:
        password = os.environ.get("DORIS_ROOT_PASSWORD", "") if user == "root" else ""
    return pymysql.connect(host=os.environ.get("DORIS_HOST", "doris-fe"), port=9030, user=user,
                           password=password, database=database, autocommit=True)


def query(con, sql: str, args=None) -> list[tuple]:
    with con.cursor() as cur:
        cur.execute(sql, args)
        return list(cur.fetchall()) if cur.description else []
