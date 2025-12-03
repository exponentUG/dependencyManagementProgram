# helpers/wmp_tracker_builder/table_builders/environment_table.py
from __future__ import annotations
import sqlite3
from typing import List, Tuple

ENV_COLUMNS: List[str] = [
    "Order",
    "Notification",           # from mpp_data
    "Project Reporting Year",
    "MAT Code",
    "Program",
    "Sub-Category",
    "Div",
    "Region",
    "WPD",                    # (mpp_data."Work Plan Date")
    "CLICK Start Date",       # (mpp_data."CLICK Start Date")
    "CLICK End Date",         # (mpp_data."CLICK End Date")
    "Notification Status",
    "SAP Status",
    "Order User Status",      # <-- NEW COLUMN (from mpp_data)
    "Open Dependencies",      # <-- NEW COLUMN (from open_dependencies)
    "DS11",
    "PC21",
    "Environment Anticipated Out Date",
    "Environment Notes",
    "Action",
]


def _table_exists(cur: sqlite3.Cursor, name: str) -> bool:
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    )
    return cur.fetchone() is not None


def get_environment_table(db_path: str) -> Tuple[List[str], List[Tuple]]:
    """
    Returns (columns, rows) for the Environment tracker view.

    Requirements:
      - environment_tracker must exist (base table).
      - mpp_data must exist (for MPP fields & Order User Status).
      - open_dependencies is optional; if missing, "Open Dependencies" is ''.

    If required tables don't exist yet, returns ([], []) so the caller can
    show a friendly message.
    """
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        has_env = _table_exists(cur, "environment_tracker")
        has_mpp = _table_exists(cur, "mpp_data")
        has_open = _table_exists(cur, "open_dependencies")

        # environment_tracker and mpp_data are required for this view
        if not has_env or not has_mpp:
            return [], []

        if has_open:
            # With open_dependencies joined
            sql = """
                SELECT
                    et."Order",
                    COALESCE(CAST(m."Notification" AS TEXT), '')       AS "Notification",
                    COALESCE(m."Project Reporting Year", '')           AS "Project Reporting Year",
                    COALESCE(m."MAT", '')                              AS "MAT Code",
                    COALESCE(m."Program", '')                          AS "Program",
                    COALESCE(m."Sub-Category", '')                     AS "Sub-Category",
                    COALESCE(m."Div", '')                              AS "Div",
                    COALESCE(m."Region", '')                           AS "Region",
                    COALESCE(m."Work Plan Date", '')                   AS "WPD",
                    COALESCE(m."CLICK Start Date", '')                 AS "CLICK Start Date",
                    COALESCE(m."CLICK End Date", '')                   AS "CLICK End Date",
                    COALESCE(et."Notification Status", '')             AS "Notification Status",
                    COALESCE(et."SAP Status", '')                      AS "SAP Status",
                    COALESCE(m."Order User Status", '')                AS "Order User Status",
                    COALESCE(od."Open Dependencies", '')               AS "Open Dependencies",
                    COALESCE(et."DS11", '')                            AS "DS11",
                    COALESCE(et."PC21", '')                            AS "PC21",
                    COALESCE(et."Environment Anticipated Out Date", '') AS "Environment Anticipated Out Date",
                    COALESCE(et."Environment Notes", '')               AS "Environment Notes",
                    COALESCE(et."Action", '')                          AS "Action"
                FROM environment_tracker et
                LEFT JOIN mpp_data m
                    ON m."Order" = et."Order"
                LEFT JOIN open_dependencies od
                    ON od."Order" = et."Order"
                ORDER BY et."Order" ASC
            """
        else:
            # Without open_dependencies; Open Dependencies column is blank
            sql = """
                SELECT
                    et."Order",
                    COALESCE(CAST(m."Notification" AS TEXT), '')       AS "Notification",
                    COALESCE(m."Project Reporting Year", '')           AS "Project Reporting Year",
                    COALESCE(m."MAT", '')                              AS "MAT Code",
                    COALESCE(m."Program", '')                          AS "Program",
                    COALESCE(m."Sub-Category", '')                     AS "Sub-Category",
                    COALESCE(m."Div", '')                              AS "Div",
                    COALESCE(m."Region", '')                           AS "Region",
                    COALESCE(m."Work Plan Date", '')                   AS "WPD",
                    COALESCE(m."CLICK Start Date", '')                 AS "CLICK Start Date",
                    COALESCE(m."CLICK End Date", '')                   AS "CLICK End Date",
                    COALESCE(et."Notification Status", '')             AS "Notification Status",
                    COALESCE(et."SAP Status", '')                      AS "SAP Status",
                    COALESCE(m."Order User Status", '')                AS "Order User Status",
                    ''                                                 AS "Open Dependencies",
                    COALESCE(et."DS11", '')                            AS "DS11",
                    COALESCE(et."PC21", '')                            AS "PC21",
                    COALESCE(et."Environment Anticipated Out Date", '') AS "Environment Anticipated Out Date",
                    COALESCE(et."Environment Notes", '')               AS "Environment Notes",
                    COALESCE(et."Action", '')                          AS "Action"
                FROM environment_tracker et
                LEFT JOIN mpp_data m
                    ON m."Order" = et."Order"
                ORDER BY et."Order" ASC
            """

        cur.execute(sql)
        rows = cur.fetchall()
        return ENV_COLUMNS, rows
