# services/db/dependency_report_db.py
from __future__ import annotations

import os
import sqlite3
from typing import Sequence, Mapping, Any

DATA_DIR = "data"
DB_NAME = "dependency_report.sqlite3"
DB_PATH = os.path.join(DATA_DIR, DB_NAME)
TABLE_NAME = "order_count"


def default_db_path() -> str:
    """
    Returns the absolute path to dependency_report.sqlite3 and ensures that
    the data/ directory exists.
    """
    os.makedirs(DATA_DIR, exist_ok=True)
    return DB_PATH


def _ensure_schema(conn: sqlite3.Connection) -> None:
    """
    Create the order_count table if it does not exist.
    (No Master column – only per-tracker counts.)
    """
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS order_count (
            "Database Type" TEXT NOT NULL,
            "Permit"        INTEGER NOT NULL,
            "Land"          INTEGER NOT NULL,
            "Environment"   INTEGER NOT NULL,
            "Joint Pole"    INTEGER NOT NULL,
            "FAA"           INTEGER NOT NULL,
            "MiscTSK"       INTEGER NOT NULL,
            "Added On"      TEXT NOT NULL
        );
        """
    )
    conn.commit()


def insert_order_counts(rows: Sequence[Mapping[str, Any]]) -> None:
    """
    Insert one or more snapshot rows into order_count.

    Each row mapping must have keys:
      - "Database Type"
      - "Permit"
      - "Land"
      - "Environment"
      - "Joint Pole"
      - "FAA"
      - "MiscTSK"
      - "Added On"
    """
    if not rows:
        return

    db_path = default_db_path()
    conn = sqlite3.connect(db_path)
    try:
        _ensure_schema(conn)
        cur = conn.cursor()

        cur.executemany(
            """
            INSERT INTO order_count (
                "Database Type",
                "Permit",
                "Land",
                "Environment",
                "Joint Pole",
                "FAA",
                "MiscTSK",
                "Added On"
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    r["Database Type"],
                    int(r["Permit"]),
                    int(r["Land"]),
                    int(r["Environment"]),
                    int(r["Joint Pole"]),
                    int(r["FAA"]),
                    int(r["MiscTSK"]),
                    str(r["Added On"]),
                )
                for r in rows
            ],
        )
        conn.commit()
    finally:
        conn.close()
