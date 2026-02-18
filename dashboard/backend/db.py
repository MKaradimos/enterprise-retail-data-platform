"""PostgreSQL helper — thin wrapper around psycopg2."""

from typing import List, Dict

import psycopg2
from psycopg2.extras import RealDictCursor

from config import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS


def get_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
    )


def query(sql: str, params=None) -> List[Dict]:
    """Run a SELECT and return list of dicts."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()
