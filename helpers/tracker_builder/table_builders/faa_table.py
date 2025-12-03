# helpers/wmp_tracker_builder/table_builders/faa_table.py
from __future__ import annotations
import sqlite3
from typing import List, Tuple

COLUMNS: List[str] = [
    "Order",
    "Notification",           # from mpp_data
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
    "Order User Status",      # <-- NEW COLUMN (from mpp_data)
    "Open Dependencies",      # <-- NEW COLUMN (from open_dependencies)
    "DS76",
    "PC24",
    "FAA Notes",
]


def _table_exists(cur: sqlite3.Cursor, name: str) -> bool:
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (name,),
    )
    return cur.fetchone() is not None


def get_faa_table(db_path: str) -> Tuple[list[str], list[tuple]]:
    """
    Returns (columns, rows) for the FAA view.

    Requirements:
      - Must have faa_tracker; otherwise returns ([], []).
      - mpp_data and manual_tracker are optional; their columns will be blank ('') if unavailable.
      - open_dependencies is optional; if missing, 'Open Dependencies' is ''.
    """
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        if not _table_exists(cur, "faa_tracker"):
            return [], []

        has_mpp = _table_exists(cur, "mpp_data")
        has_manual = _table_exists(cur, "manual_tracker")
        has_open = _table_exists(cur, "open_dependencies")

        if has_mpp and has_manual and has_open:
            sql = """
                SELECT
                    ft."Order",
                    COALESCE(CAST(m."Notification" AS TEXT), '') AS "Notification",
                    COALESCE(m."Project Reporting Year", '')     AS "Project Reporting Year",
                    COALESCE(m."MAT", '')                        AS "MAT Code",
                    COALESCE(m."Program", '')                    AS "Program",
                    COALESCE(m."Sub-Category", '')               AS "Sub-Category",
                    COALESCE(m."Div", '')                        AS "Div",
                    COALESCE(m."Region", '')                     AS "Region",
                    COALESCE(m."Work Plan Date", '')             AS "WPD",
                    COALESCE(m."CLICK Start Date", '')           AS "CLICK Start Date",
                    COALESCE(m."CLICK End Date", '')            AS "CLICK End Date",
                    COALESCE(ft."Notification Status", '')       AS "Notification Status",
                    COALESCE(ft."SAP Status", '')                AS "SAP Status",
                    COALESCE(m."Order User Status", '')          AS "Order User Status",
                    COALESCE(od."Open Dependencies", '')         AS "Open Dependencies",
                    COALESCE(ft."DS76", '')                      AS "DS76",
                    COALESCE(ft."PC24", '')                      AS "PC24",
                    COALESCE(mt."FAA Notes", '')                 AS "FAA Notes"
                FROM faa_tracker ft
                LEFT JOIN mpp_data m
                    ON m."Order" = ft."Order"
                LEFT JOIN open_dependencies od
                    ON od."Order" = ft."Order"
                LEFT JOIN manual_tracker mt
                    ON mt."Order" = ft."Order"
                ORDER BY ft."Order"
            """
        elif has_mpp and has_manual and not has_open:
            sql = """
                SELECT
                    ft."Order",
                    COALESCE(CAST(m."Notification" AS TEXT), '') AS "Notification",
                    COALESCE(m."Project Reporting Year", '')     AS "Project Reporting Year",
                    COALESCE(m."MAT", '')                        AS "MAT Code",
                    COALESCE(m."Program", '')                    AS "Program",
                    COALESCE(m."Sub-Category", '')               AS "Sub-Category",
                    COALESCE(m."Div", '')                        AS "Div",
                    COALESCE(m."Region", '')                     AS "Region",
                    COALESCE(m."Work Plan Date", '')             AS "WPD",
                    COALESCE(m."CLICK Start Date", '')           AS "CLICK Start Date",
                    COALESCE(m."CLICK End Date", '')            AS "CLICK End Date",
                    COALESCE(ft."Notification Status", '')       AS "Notification Status",
                    COALESCE(ft."SAP Status", '')                AS "SAP Status",
                    COALESCE(m."Order User Status", '')          AS "Order User Status",
                    ''                                           AS "Open Dependencies",
                    COALESCE(ft."DS76", '')                      AS "DS76",
                    COALESCE(ft."PC24", '')                      AS "PC24",
                    COALESCE(mt."FAA Notes", '')                 AS "FAA Notes"
                FROM faa_tracker ft
                LEFT JOIN mpp_data m
                    ON m."Order" = ft."Order"
                LEFT JOIN manual_tracker mt
                    ON mt."Order" = ft."Order"
                ORDER BY ft."Order"
            """
        elif has_mpp and not has_manual and has_open:
            sql = """
                SELECT
                    ft."Order",
                    COALESCE(CAST(m."Notification" AS TEXT), '') AS "Notification",
                    COALESCE(m."Project Reporting Year", '')     AS "Project Reporting Year",
                    COALESCE(m."MAT", '')                        AS "MAT Code",
                    COALESCE(m."Program", '')                    AS "Program",
                    COALESCE(m."Sub-Category", '')               AS "Sub-Category",
                    COALESCE(m."Div", '')                        AS "Div",
                    COALESCE(m."Region", '')                     AS "Region",
                    COALESCE(m."Work Plan Date", '')             AS "WPD",
                    COALESCE(m."CLICK Start Date", '')           AS "CLICK Start Date",
                    COALESCE(m."CLICK End Date", '')            AS "CLICK End Date",
                    COALESCE(ft."Notification Status", '')       AS "Notification Status",
                    COALESCE(ft."SAP Status", '')                AS "SAP Status",
                    COALESCE(m."Order User Status", '')          AS "Order User Status",
                    COALESCE(od."Open Dependencies", '')         AS "Open Dependencies",
                    COALESCE(ft."DS76", '')                      AS "DS76",
                    COALESCE(ft."PC24", '')                      AS "PC24",
                    ''                                           AS "FAA Notes"
                FROM faa_tracker ft
                LEFT JOIN mpp_data m
                    ON m."Order" = ft."Order"
                LEFT JOIN open_dependencies od
                    ON od."Order" = ft."Order"
                ORDER BY ft."Order"
            """
        elif has_mpp and not has_manual and not has_open:
            sql = """
                SELECT
                    ft."Order",
                    COALESCE(CAST(m."Notification" AS TEXT), '') AS "Notification",
                    COALESCE(m."Project Reporting Year", '')     AS "Project Reporting Year",
                    COALESCE(m."MAT", '')                        AS "MAT Code",
                    COALESCE(m."Program", '')                    AS "Program",
                    COALESCE(m."Sub-Category", '')               AS "Sub-Category",
                    COALESCE(m."Div", '')                        AS "Div",
                    COALESCE(m."Region", '')                     AS "Region",
                    COALESCE(m."Work Plan Date", '')             AS "WPD",
                    COALESCE(m."CLICK Start Date", '')           AS "CLICK Start Date",
                    COALESCE(m."CLICK End Date", '')            AS "CLICK End Date",
                    COALESCE(ft."Notification Status", '')       AS "Notification Status",
                    COALESCE(ft."SAP Status", '')                AS "SAP Status",
                    COALESCE(m."Order User Status", '')          AS "Order User Status",
                    ''                                           AS "Open Dependencies",
                    COALESCE(ft."DS76", '')                      AS "DS76",
                    COALESCE(ft."PC24", '')                      AS "PC24",
                    ''                                           AS "FAA Notes"
                FROM faa_tracker ft
                LEFT JOIN mpp_data m
                    ON m."Order" = ft."Order"
                ORDER BY ft."Order"
            """
        elif not has_mpp and has_manual and has_open:
            sql = """
                SELECT
                    ft."Order",
                    ''                                           AS "Notification",
                    ''                                           AS "Project Reporting Year",
                    ''                                           AS "MAT Code",
                    ''                                           AS "Program",
                    ''                                           AS "Sub-Category",
                    ''                                           AS "Div",
                    ''                                           AS "Region",
                    ''                                           AS "WPD",
                    ''                                           AS "CLICK Start Date",
                    ''                                           AS "CLICK End Date",
                    COALESCE(ft."Notification Status", '')       AS "Notification Status",
                    COALESCE(ft."SAP Status", '')                AS "SAP Status",
                    ''                                           AS "Order User Status",
                    COALESCE(od."Open Dependencies", '')         AS "Open Dependencies",
                    COALESCE(ft."DS76", '')                      AS "DS76",
                    COALESCE(ft."PC24", '')                      AS "PC24",
                    COALESCE(mt."FAA Notes", '')                 AS "FAA Notes"
                FROM faa_tracker ft
                LEFT JOIN open_dependencies od
                    ON od."Order" = ft."Order"
                LEFT JOIN manual_tracker mt
                    ON mt."Order" = ft."Order"
                ORDER BY ft."Order"
            """
        elif not has_mpp and has_manual and not has_open:
            sql = """
                SELECT
                    ft."Order",
                    ''                                           AS "Notification",
                    ''                                           AS "Project Reporting Year",
                    ''                                           AS "MAT Code",
                    ''                                           AS "Program",
                    ''                                           AS "Sub-Category",
                    ''                                           AS "Div",
                    ''                                           AS "Region",
                    ''                                           AS "WPD",
                    ''                                           AS "CLICK Start Date",
                    ''                                           AS "CLICK End Date",
                    COALESCE(ft."Notification Status", '')       AS "Notification Status",
                    COALESCE(ft."SAP Status", '')                AS "SAP Status",
                    ''                                           AS "Order User Status",
                    ''                                           AS "Open Dependencies",
                    COALESCE(ft."DS76", '')                      AS "DS76",
                    COALESCE(ft."PC24", '')                      AS "PC24",
                    COALESCE(mt."FAA Notes", '')                 AS "FAA Notes"
                FROM faa_tracker ft
                LEFT JOIN manual_tracker mt
                    ON mt."Order" = ft."Order"
                ORDER BY ft."Order"
            """
        elif not has_mpp and not has_manual and has_open:
            sql = """
                SELECT
                    ft."Order",
                    ''                                           AS "Notification",
                    ''                                           AS "Project Reporting Year",
                    ''                                           AS "MAT Code",
                    ''                                           AS "Program",
                    ''                                           AS "Sub-Category",
                    ''                                           AS "Div",
                    ''                                           AS "Region",
                    ''                                           AS "WPD",
                    ''                                           AS "CLICK Start Date",
                    ''                                           AS "CLICK End Date",
                    COALESCE(ft."Notification Status", '')       AS "Notification Status",
                    COALESCE(ft."SAP Status", '')                AS "SAP Status",
                    ''                                           AS "Order User Status",
                    COALESCE(od."Open Dependencies", '')         AS "Open Dependencies",
                    COALESCE(ft."DS76", '')                      AS "DS76",
                    COALESCE(ft."PC24", '')                      AS "PC24",
                    ''                                           AS "FAA Notes"
                FROM faa_tracker ft
                LEFT JOIN open_dependencies od
                    ON od."Order" = ft."Order"
                ORDER BY ft."Order"
            """
        else:
            # No mpp_data, no manual_tracker, no open_dependencies
            sql = """
                SELECT
                    ft."Order",
                    ''                                           AS "Notification",
                    ''                                           AS "Project Reporting Year",
                    ''                                           AS "MAT Code",
                    ''                                           AS "Program",
                    ''                                           AS "Sub-Category",
                    ''                                           AS "Div",
                    ''                                           AS "Region",
                    ''                                           AS "WPD",
                    ''                                           AS "CLICK Start Date",
                    ''                                           AS "CLICK End Date",
                    COALESCE(ft."Notification Status", '')       AS "Notification Status",
                    COALESCE(ft."SAP Status", '')                AS "SAP Status",
                    ''                                           AS "Order User Status",
                    ''                                           AS "Open Dependencies",
                    COALESCE(ft."DS76", '')                      AS "DS76",
                    COALESCE(ft."PC24", '')                      AS "PC24",
                    ''                                           AS "FAA Notes"
                FROM faa_tracker ft
                ORDER BY ft."Order"
            """

        cur.execute(sql)
        rows = cur.fetchall()
        return COLUMNS, rows
