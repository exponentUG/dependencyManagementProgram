# helpers/wmp_tracker_builder/table_builders/environment_table.py
from __future__ import annotations
import sqlite3
from typing import List, Tuple

ENV_COLUMNS: List[str] = [
    "Order",
    "Notification",           # <-- NEW COLUMN
    "Project Reporting Year",
    "MAT Code",
    "Program",
    "Sub-Category",
    "Div",
    "Region",
    "WPD",                 # (mpp_data."Work Plan Date")
    "CLICK Start Date",    # (mpp_data."CLICK Start Date")
    "CLICK End Date",      # (mpp_data."CLICK End Date")
    "Notification Status",
    "SAP Status",
    "DS11",
    "PC21",
    "Environment Anticipated Out Date",
    "Environment Notes",
    "Action",
]

def _table_exists(cur: sqlite3.Cursor, name: str) -> bool:
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
    return cur.fetchone() is not None

def get_environment_table(db_path: str) -> Tuple[List[str], List[Tuple]]:
    """
    Returns (columns, rows) for the Environment tracker view.

    If required tables don't exist yet, returns ([], []) so the caller can show a friendly message.
    """
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        if not _table_exists(cur, "environment_tracker") or not _table_exists(cur, "mpp_data"):
            return [], []

        # LEFT JOIN to retain all env orders; fill blanks with '' in SELECT
        cur.execute("""
            SELECT
                et."Order",
                COALESCE(CAST(m."Notification" AS TEXT), '')      AS "Notification",
                COALESCE(m."Project Reporting Year", '')          AS "Project Reporting Year",
                COALESCE(m."MAT", '')                             AS "MAT Code",
                COALESCE(m."Program", '')                         AS "Program",
                COALESCE(m."Sub-Category", '')                    AS "Sub-Category",
                COALESCE(m."Div", '')                             AS "Div",
                COALESCE(m."Region", '')                          AS "Region",
                COALESCE(m."Work Plan Date", '')                  AS "WPD",
                COALESCE(m."CLICK Start Date", '')                AS "CLICK Start Date",
                COALESCE(m."CLICK End Date", '')                  AS "CLICK End Date",
                COALESCE(et."Notification Status", '')            AS "Notification Status",
                COALESCE(et."SAP Status", '')                     AS "SAP Status",
                COALESCE(et."DS11", '')                           AS "DS11",
                COALESCE(et."PC21", '')                           AS "PC21",
                COALESCE(et."Environment Anticipated Out Date", '') AS "Environment Anticipated Out Date",
                COALESCE(et."Environment Notes", '')              AS "Environment Notes",
                COALESCE(et."Action", '')                         AS "Action"
            FROM environment_tracker et
            LEFT JOIN mpp_data m ON m."Order" = et."Order"
            ORDER BY et."Order" ASC
        """)
        rows = cur.fetchall()

        return ENV_COLUMNS, rows
