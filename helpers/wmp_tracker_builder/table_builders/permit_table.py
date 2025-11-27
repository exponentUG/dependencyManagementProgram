# helpers/wmp_tracker_builder/table_builders/permit_table.py
from __future__ import annotations
import sqlite3
from typing import List, Tuple

COLUMNS: List[str] = [
    "Order",
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
    "SP56",
    "RP56",
    "E Permit Status",
    "Submit Days",
    "Permit Expiration Date",
    "LEAPS Cycle Time",
    "Permit Notes",
    "Action",
]

def _table_exists(cur: sqlite3.Cursor, name: str) -> bool:
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,))
    return cur.fetchone() is not None

def get_permit_table(db_path: str) -> Tuple[list[str], list[tuple]]:
    """
    Returns (columns, rows) for the Permit view.
    Safe if tables are missing: returns ([], []) and caller shows a friendly message.
    """
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        # Require permit_tracker to show anything
        if not _table_exists(cur, "permit_tracker"):
            return [], []

        has_mpp = _table_exists(cur, "mpp_data")
        has_manual = _table_exists(cur, "manual_tracker")

        if has_mpp and has_manual:
            sql = """
                SELECT
                    pt."Order",
                    COALESCE(m."Project Reporting Year", '') AS "Project Reporting Year",
                    COALESCE(m."MAT", '')                    AS "MAT Code",
                    COALESCE(m."Program", '')                AS "Program",
                    COALESCE(m."Sub-Category", '')           AS "Sub-Category",
                    COALESCE(m."Div", '')                    AS "Div",
                    COALESCE(m."Region", '')                 AS "Region",
                    COALESCE(m."Work Plan Date", '')         AS "WPD",
                    COALESCE(m."CLICK Start Date", '')       AS "CLICK Start Date",
                    COALESCE(m."CLICK End Date", '')         AS "CLICK End Date",
                    COALESCE(pt."Notification Status", '')   AS "Notification Status",
                    COALESCE(pt."SAP Status", '')            AS "SAP Status",
                    COALESCE(pt."SP56 Status", '')           AS "SP56",
                    COALESCE(pt."RP56 Status", '')           AS "RP56",
                    COALESCE(pt."E Permit Status", '')       AS "E Permit Status",
                    COALESCE(CAST(pt."Submit Days" AS TEXT), '')      AS "Submit Days",
                    COALESCE(pt."Permit Expiration Date", '')         AS "Permit Expiration Date",
                    COALESCE(CAST(pt."LEAPS Cycle Time" AS TEXT), '') AS "LEAPS Cycle Time",
                    COALESCE(mt."Permit Notes", '')          AS "Permit Notes",
                    COALESCE(pt."Action", '')                AS "Action"
                FROM permit_tracker pt
                LEFT JOIN mpp_data m ON m."Order" = pt."Order"
                LEFT JOIN manual_tracker mt ON mt."Order" = pt."Order"
                ORDER BY pt."Order"
            """
        elif has_mpp and not has_manual:
            sql = """
                SELECT
                    pt."Order",
                    COALESCE(m."Project Reporting Year", '') AS "Project Reporting Year",
                    COALESCE(m."MAT", '')                    AS "MAT Code",
                    COALESCE(m."Program", '')                AS "Program",
                    COALESCE(m."Sub-Category", '')           AS "Sub-Category",
                    COALESCE(m."Div", '')                    AS "Div",
                    COALESCE(m."Region", '')                 AS "Region",
                    COALESCE(m."Work Plan Date", '')         AS "WPD",
                    COALESCE(m."CLICK Start Date", '')       AS "CLICK Start Date",
                    COALESCE(m."CLICK End Date", '')         AS "CLICK End Date",
                    COALESCE(pt."Notification Status", '')   AS "Notification Status",
                    COALESCE(pt."SAP Status", '')            AS "SAP Status",
                    COALESCE(pt."SP56 Status", '')           AS "SP56",
                    COALESCE(pt."RP56 Status", '')           AS "RP56",
                    COALESCE(pt."E Permit Status", '')       AS "E Permit Status",
                    COALESCE(CAST(pt."Submit Days" AS TEXT), '')      AS "Submit Days",
                    COALESCE(pt."Permit Expiration Date", '')         AS "Permit Expiration Date",
                    COALESCE(CAST(pt."LEAPS Cycle Time" AS TEXT), '') AS "LEAPS Cycle Time",
                    ''                                       AS "Permit Notes",
                    COALESCE(pt."Action", '')                AS "Action"
                FROM permit_tracker pt
                LEFT JOIN mpp_data m ON m."Order" = pt."Order"
                ORDER BY pt."Order"
            """
        elif not has_mpp and has_manual:
            sql = """
                SELECT
                    pt."Order",
                    '' AS "Project Reporting Year",
                    '' AS "MAT Code",
                    '' AS "Program",
                    '' AS "Sub-Category",
                    '' AS "Div",
                    '' AS "Region",
                    '' AS "WPD",
                    '' AS "CLICK Start Date",
                    '' AS "CLICK End Date",
                    COALESCE(pt."Notification Status", '')   AS "Notification Status",
                    COALESCE(pt."SAP Status", '')            AS "SAP Status",
                    COALESCE(pt."SP56 Status", '')           AS "SP56",
                    COALESCE(pt."RP56 Status", '')           AS "RP56",
                    COALESCE(pt."E Permit Status", '')       AS "E Permit Status",
                    COALESCE(CAST(pt."Submit Days" AS TEXT), '')      AS "Submit Days",
                    COALESCE(pt."Permit Expiration Date", '')         AS "Permit Expiration Date",
                    COALESCE(CAST(pt."LEAPS Cycle Time" AS TEXT), '') AS "LEAPS Cycle Time",
                    COALESCE(mt."Permit Notes", '')          AS "Permit Notes",
                    COALESCE(pt."Action", '')                AS "Action"
                FROM permit_tracker pt
                LEFT JOIN manual_tracker mt ON mt."Order" = pt."Order"
                ORDER BY pt."Order"
            """
        else:
            sql = """
                SELECT
                    pt."Order",
                    '' AS "Project Reporting Year",
                    '' AS "MAT Code",
                    '' AS "Program",
                    '' AS "Sub-Category",
                    '' AS "Div",
                    '' AS "Region",
                    '' AS "WPD",
                    '' AS "CLICK Start Date",
                    '' AS "CLICK End Date",
                    COALESCE(pt."Notification Status", '')   AS "Notification Status",
                    COALESCE(pt."SAP Status", '')            AS "SAP Status",
                    COALESCE(pt."SP56 Status", '')           AS "SP56",
                    COALESCE(pt."RP56 Status", '')           AS "RP56",
                    COALESCE(pt."E Permit Status", '')       AS "E Permit Status",
                    COALESCE(CAST(pt."Submit Days" AS TEXT), '')      AS "Submit Days",
                    COALESCE(pt."Permit Expiration Date", '')         AS "Permit Expiration Date",
                    COALESCE(CAST(pt."LEAPS Cycle Time" AS TEXT), '') AS "LEAPS Cycle Time",
                    ''                                       AS "Permit Notes",
                    COALESCE(pt."Action", '')                AS "Action"
                FROM permit_tracker pt
                ORDER BY pt."Order"
            """

        cur.execute(sql)
        rows = cur.fetchall()
        return COLUMNS, rows
