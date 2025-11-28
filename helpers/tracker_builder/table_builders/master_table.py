# helpers/wmp_tracker_builder/table_builders/master_table.py
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
    "Open Dependencies",
    "Stage of Job",
]

def _table_exists(cur: sqlite3.Cursor, name: str) -> bool:
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,))
    return cur.fetchone() is not None

def get_master_table(db_path: str) -> Tuple[list[str], list[tuple]]:
    """
    Returns (columns, rows) for the Master view.
    - Requires order_tracking_list.
    - mpp_data and open_dependencies are optional (their fields will be '').
    - If 'Stage of Job' column doesn't exist yet in open_dependencies, it is shown as ''.
    """
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        if not _table_exists(cur, "order_tracking_list"):
            return [], []

        has_mpp = _table_exists(cur, "mpp_data")
        has_open = _table_exists(cur, "open_dependencies")

        has_stage = False
        if has_open:
            cur.execute("PRAGMA table_info(open_dependencies)")
            od_cols = {row[1] for row in cur.fetchall()}
            has_stage = "Stage of Job" in od_cols

        if has_mpp and has_open:
            if has_stage:
                sql = """
                    SELECT
                        ot."Order",
                        COALESCE(CAST(m."Notification" AS TEXT), '') AS "Notification",
                        COALESCE(m."Project Reporting Year", '')     AS "Project Reporting Year",
                        COALESCE(m."MAT", '')                        AS "MAT Code",
                        COALESCE(m."Program", '')                    AS "Program",
                        COALESCE(m."Sub-Category", '')               AS "Sub-Category",
                        COALESCE(m."Div", '')                        AS "Div",
                        COALESCE(m."Region", '')                     AS "Region",
                        COALESCE(m."Work Plan Date", '')             AS "WPD",
                        COALESCE(m."CLICK Start Date", '')           AS "CLICK Start Date",
                        COALESCE(m."CLICK End Date", '')             AS "CLICK End Date",
                        COALESCE(m."Notif Status", '')               AS "Notification Status",
                        COALESCE(m."Primary Status", '')             AS "SAP Status",
                        COALESCE(od."Open Dependencies", '')         AS "Open Dependencies",
                        COALESCE(od."Stage of Job", '')              AS "Stage of Job"
                    FROM order_tracking_list ot
                    LEFT JOIN mpp_data m           ON m."Order" = ot."Order"
                    LEFT JOIN open_dependencies od ON od."Order" = ot."Order"
                    ORDER BY ot."Order"
                """
            else:
                sql = """
                    SELECT
                        ot."Order",
                        COALESCE(CAST(m."Notification" AS TEXT), '') AS "Notification",
                        COALESCE(m."Project Reporting Year", '')     AS "Project Reporting Year",
                        COALESCE(m."MAT", '')                        AS "MAT Code",
                        COALESCE(m."Program", '')                    AS "Program",
                        COALESCE(m."Sub-Category", '')               AS "Sub-Category",
                        COALESCE(m."Div", '')                        AS "Div",
                        COALESCE(m."Region", '')                     AS "Region",
                        COALESCE(m."Work Plan Date", '')             AS "WPD",
                        COALESCE(m."CLICK Start Date", '')           AS "CLICK Start Date",
                        COALESCE(m."CLICK End Date", '')             AS "CLICK End Date",
                        COALESCE(m."Notif Status", '')               AS "Notification Status",
                        COALESCE(m."Primary Status", '')             AS "SAP Status",
                        COALESCE(od."Open Dependencies", '')         AS "Open Dependencies",
                        ''                                           AS "Stage of Job"
                    FROM order_tracking_list ot
                    LEFT JOIN mpp_data m           ON m."Order" = ot."Order"
                    LEFT JOIN open_dependencies od ON od."Order" = ot."Order"
                    ORDER BY ot."Order"
                """
        elif has_mpp and not has_open:
            sql = """
                SELECT
                    ot."Order",
                    COALESCE(CAST(m."Notification" AS TEXT), '') AS "Notification",
                    COALESCE(m."Project Reporting Year", '')     AS "Project Reporting Year",
                    COALESCE(m."MAT", '')                        AS "MAT Code",
                    COALESCE(m."Program", '')                    AS "Program",
                    COALESCE(m."Sub-Category", '')               AS "Sub-Category",
                    COALESCE(m."Div", '')                        AS "Div",
                    COALESCE(m."Region", '')                     AS "Region",
                    COALESCE(m."Work Plan Date", '')             AS "WPD",
                    COALESCE(m."CLICK Start Date", '')           AS "CLICK Start Date",
                    COALESCE(m."CLICK End Date", '')             AS "CLICK End Date",
                    COALESCE(m."Notif Status", '')               AS "Notification Status",
                    COALESCE(m."Primary Status", '')             AS "SAP Status",
                    ''                                           AS "Open Dependencies",
                    ''                                           AS "Stage of Job"
                FROM order_tracking_list ot
                LEFT JOIN mpp_data m ON m."Order" = ot."Order"
                ORDER BY ot."Order"
            """
        elif not has_mpp and has_open:
            if has_stage:
                sql = """
                    SELECT
                        ot."Order",
                        ''                                   AS "Notification",
                        ''                                   AS "Project Reporting Year",
                        ''                                   AS "MAT Code",
                        ''                                   AS "Program",
                        ''                                   AS "Sub-Category",
                        ''                                   AS "Div",
                        ''                                   AS "Region",
                        ''                                   AS "WPD",
                        ''                                   AS "CLICK Start Date",
                        ''                                   AS "CLICK End Date",
                        ''                                   AS "Notification Status",
                        ''                                   AS "SAP Status",
                        COALESCE(od."Open Dependencies", '') AS "Open Dependencies",
                        COALESCE(od."Stage of Job", '')      AS "Stage of Job"
                    FROM order_tracking_list ot
                    LEFT JOIN open_dependencies od ON od."Order" = ot."Order"
                    ORDER BY ot."Order"
                """
            else:
                sql = """
                    SELECT
                        ot."Order",
                        ''                                   AS "Notification",
                        ''                                   AS "Project Reporting Year",
                        ''                                   AS "MAT Code",
                        ''                                   AS "Program",
                        ''                                   AS "Sub-Category",
                        ''                                   AS "Div",
                        ''                                   AS "Region",
                        ''                                   AS "WPD",
                        ''                                   AS "CLICK Start Date",
                        ''                                   AS "CLICK End Date",
                        ''                                   AS "Notification Status",
                        ''                                   AS "SAP Status",
                        COALESCE(od."Open Dependencies", '') AS "Open Dependencies",
                        ''                                   AS "Stage of Job"
                    FROM order_tracking_list ot
                    LEFT JOIN open_dependencies od ON od."Order" = ot."Order"
                    ORDER BY ot."Order"
                """
        else:
            # Neither mpp_data nor open_dependencies present
            sql = """
                SELECT
                    ot."Order",
                    '' AS "Notification",
                    '' AS "Project Reporting Year",
                    '' AS "MAT Code",
                    '' AS "Program",
                    '' AS "Sub-Category",
                    '' AS "Div",
                    '' AS "Region",
                    '' AS "WPD",
                    '' AS "CLICK Start Date",
                    '' AS "CLICK End Date",
                    '' AS "Notification Status",
                    '' AS "SAP Status",
                    '' AS "Open Dependencies",
                    '' AS "Stage of Job"
                FROM order_tracking_list ot
                ORDER BY ot."Order"
            """

        cur.execute(sql)
        rows = cur.fetchall()
        return COLUMNS, rows
