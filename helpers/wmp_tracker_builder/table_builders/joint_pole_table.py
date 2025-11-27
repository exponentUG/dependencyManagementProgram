# helpers/wmp_tracker_builder/table_builders/joint_pole_table.py
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
    "DS42",
    "PC20",
    "Sent to OU Date",
    "Anticipated Out Date",
    "Joint Pole Notes",
    "Action",
]

def _table_exists(cur: sqlite3.Cursor, name: str) -> bool:
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,))
    return cur.fetchone() is not None

def get_joint_pole_table(db_path: str) -> Tuple[list[str], list[tuple]]:
    """
    Returns (columns, rows) for the Joint Pole view.
    Safe when tables are missing: returns ([], []) so the caller can show a friendly message.
    """
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        # must have joint_pole_tracker at minimum
        if not _table_exists(cur, "joint_pole_tracker"):
            return [], []

        has_mpp = _table_exists(cur, "mpp_data")
        has_manual = _table_exists(cur, "manual_tracker")

        if has_mpp and has_manual:
            sql = """
                SELECT
                    jt."Order",
                    COALESCE(m."Project Reporting Year", '') AS "Project Reporting Year",
                    COALESCE(m."MAT", '')                    AS "MAT Code",
                    COALESCE(m."Program", '')                AS "Program",
                    COALESCE(m."Sub-Category", '')           AS "Sub-Category",
                    COALESCE(m."Div", '')                    AS "Div",
                    COALESCE(m."Region", '')                 AS "Region",
                    COALESCE(m."Work Plan Date", '')         AS "WPD",
                    COALESCE(m."CLICK Start Date", '')       AS "CLICK Start Date",
                    COALESCE(m."CLICK End Date", '')         AS "CLICK End Date",
                    COALESCE(jt."Notification Status", '')   AS "Notification Status",
                    COALESCE(jt."SAP Status", '')            AS "SAP Status",
                    COALESCE(jt."DS42", '')                  AS "DS42",
                    COALESCE(jt."PC20", '')                  AS "PC20",
                    jt."Sent to OU Date"                     AS "Sent to OU Date",
                    jt."Anticipated Out Date"                AS "Anticipated Out Date",
                    COALESCE(mt."Joint Pole Notes", '')      AS "Joint Pole Notes",
                    COALESCE(jt."Action", '')                AS "Action"
                FROM joint_pole_tracker jt
                LEFT JOIN mpp_data m ON m."Order" = jt."Order"
                LEFT JOIN manual_tracker mt ON mt."Order" = jt."Order"
                ORDER BY jt."Order"
            """
        elif has_mpp and not has_manual:
            sql = """
                SELECT
                    jt."Order",
                    COALESCE(m."Project Reporting Year", '') AS "Project Reporting Year",
                    COALESCE(m."MAT", '')                    AS "MAT Code",
                    COALESCE(m."Program", '')                AS "Program",
                    COALESCE(m."Sub-Category", '')           AS "Sub-Category",
                    COALESCE(m."Div", '')                    AS "Div",
                    COALESCE(m."Region", '')                 AS "Region",
                    COALESCE(m."Work Plan Date", '')         AS "WPD",
                    COALESCE(m."CLICK Start Date", '')       AS "CLICK Start Date",
                    COALESCE(m."CLICK End Date", '')         AS "CLICK End Date",
                    COALESCE(jt."Notification Status", '')   AS "Notification Status",
                    COALESCE(jt."SAP Status", '')            AS "SAP Status",
                    COALESCE(jt."DS42", '')                  AS "DS42",
                    COALESCE(jt."PC20", '')                  AS "PC20",
                    jt."Sent to OU Date"                     AS "Sent to OU Date",
                    jt."Anticipated Out Date"                AS "Anticipated Out Date",
                    ''                                       AS "Joint Pole Notes",
                    COALESCE(jt."Action", '')                AS "Action"
                FROM joint_pole_tracker jt
                LEFT JOIN mpp_data m ON m."Order" = jt."Order"
                ORDER BY jt."Order"
            """
        elif not has_mpp and has_manual:
            sql = """
                SELECT
                    jt."Order",
                    ''                                      AS "Project Reporting Year",
                    ''                                      AS "MAT Code",
                    ''                                      AS "Program",
                    ''                                      AS "Sub-Category",
                    ''                                      AS "Div",
                    ''                                      AS "Region",
                    ''                                      AS "WPD",
                    ''                                      AS "CLICK Start Date",
                    ''                                      AS "CLICK End Date",
                    COALESCE(jt."Notification Status", '')  AS "Notification Status",
                    COALESCE(jt."SAP Status", '')           AS "SAP Status",
                    COALESCE(jt."DS42", '')                 AS "DS42",
                    COALESCE(jt."PC20", '')                 AS "PC20",
                    jt."Sent to OU Date"                    AS "Sent to OU Date",
                    jt."Anticipated Out Date"               AS "Anticipated Out Date",
                    COALESCE(mt."Joint Pole Notes", '')     AS "Joint Pole Notes",
                    COALESCE(jt."Action", '')               AS "Action"
                FROM joint_pole_tracker jt
                LEFT JOIN manual_tracker mt ON mt."Order" = jt."Order"
                ORDER BY jt."Order"
            """
        else:
            sql = """
                SELECT
                    jt."Order",
                    ''                                      AS "Project Reporting Year",
                    ''                                      AS "MAT Code",
                    ''                                      AS "Program",
                    ''                                      AS "Sub-Category",
                    ''                                      AS "Div",
                    ''                                      AS "Region",
                    ''                                      AS "WPD",
                    ''                                      AS "CLICK Start Date",
                    ''                                      AS "CLICK End Date",
                    COALESCE(jt."Notification Status", '')  AS "Notification Status",
                    COALESCE(jt."SAP Status", '')           AS "SAP Status",
                    COALESCE(jt."DS42", '')                 AS "DS42",
                    COALESCE(jt."PC20", '')                 AS "PC20",
                    jt."Sent to OU Date"                    AS "Sent to OU Date",
                    jt."Anticipated Out Date"               AS "Anticipated Out Date",
                    ''                                      AS "Joint Pole Notes",
                    COALESCE(jt."Action", '')               AS "Action"
                FROM joint_pole_tracker jt
                ORDER BY jt."Order"
            """

        cur.execute(sql)
        rows = cur.fetchall()
        return COLUMNS, rows
