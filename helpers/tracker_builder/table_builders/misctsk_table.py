# helpers/wmp_tracker_builder/table_builders/misctsk_table.py
from __future__ import annotations
import sqlite3
from typing import List, Tuple

COLUMNS: List[str] = [
    "Order",
    "Notification",          # <-- NEW COLUMN
    "Project Reporting Year",
    "MAT Code",
    "Program",
    "Sub-Category",
    "Div",
    "Region",
    "WPD",
    "CLICK Start Date",
    "CLICK End Date",
    "Notification Status",
    "SAP Status",
    "AP10",
    "AP25",
    "DS28",
    "DS73",
]

def _table_exists(cur: sqlite3.Cursor, name: str) -> bool:
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,))
    return cur.fetchone() is not None

def get_misc_tsk_table(db_path: str) -> Tuple[list[str], list[tuple]]:
    """
    Returns (columns, rows) for the MiscTSK view.
    If required tables are missing or empty, returns ([], []).
    """
    try:
        conn = sqlite3.connect(db_path)
    except Exception:
        return [], []

    with conn:
        cur = conn.cursor()

        # Need both tables
        if not _table_exists(cur, "miscTSK_tracker") or not _table_exists(cur, "mpp_data"):
            return [], []

        # If miscTSK_tracker has no rows, return empty
        cur.execute('SELECT 1 FROM miscTSK_tracker LIMIT 1')
        if cur.fetchone() is None:
            return COLUMNS, []

        # Build rows (LEFT JOIN to pull MPP context)
        cur.execute("""
            SELECT
                mt."Order" AS "Order",
                mpp."Notification" AS "Notification",
                mpp."Project Reporting Year" AS "Project Reporting Year",
                mpp."MAT" AS "MAT Code",
                mpp."Program" AS "Program",
                mpp."Sub-Category" AS "Sub-Category",
                mpp."Div" AS "Div",
                mpp."Region" AS "Region",
                mpp."Work Plan Date" AS "WPD",
                mpp."CLICK Start Date" AS "CLICK Start Date",
                mpp."CLICK End Date" AS "CLICK End Date",
                mt."Notification Status" AS "Notification Status",
                mt."SAP Status" AS "SAP Status",
                mt."AP10" AS "AP10",
                mt."AP25" AS "AP25",
                mt."DS28" AS "DS28",
                mt."DS73" AS "DS73"
            FROM miscTSK_tracker mt
            LEFT JOIN mpp_data mpp ON mpp."Order" = mt."Order"
            ORDER BY mt."Order"
        """)

        rows_raw = cur.fetchall()

    # Normalize Nones to empty strings for display
    rows = [tuple("" if v is None else v for v in r) for r in rows_raw]
    return COLUMNS, rows
