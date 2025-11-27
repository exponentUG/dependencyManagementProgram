# helpers/wmp_tracker_builder/table_builders/land_table.py
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
    "SP57",
    "RP57",
    "Permit Type",
    "Anticipated Application Date",
    "Anticipated Issue Date",
    "Permit Expiration Date",
    "Land Management Comments",
    "Permit Comment",
    "Land Notes",
    "Action",
]

def _table_exists(cur: sqlite3.Cursor, name: str) -> bool:
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,))
    return cur.fetchone() is not None

def get_land_table(db_path: str) -> Tuple[list[str], list[tuple]]:
    """
    Returns (columns, rows) for the Land view.
    Safe when tables are missing: returns ([], []) so the caller can show a friendly message.
    """
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        # Must have land_tracker to show anything
        if not _table_exists(cur, "land_tracker"):
            return [], []

        has_mpp = _table_exists(cur, "mpp_data")
        has_manual = _table_exists(cur, "manual_tracker")

        if has_mpp and has_manual:
            sql = """
                SELECT
                    lt."Order",
                    COALESCE(m."Project Reporting Year", '')      AS "Project Reporting Year",
                    COALESCE(m."MAT", '')                          AS "MAT Code",
                    COALESCE(m."Program", '')                      AS "Program",
                    COALESCE(m."Sub-Category", '')                 AS "Sub-Category",
                    COALESCE(m."Div", '')                          AS "Div",
                    COALESCE(m."Region", '')                       AS "Region",
                    COALESCE(m."Work Plan Date", '')               AS "WPD",
                    COALESCE(m."CLICK Start Date", '')             AS "CLICK Start Date",
                    COALESCE(m."CLICK End Date", '')               AS "CLICK End Date",
                    COALESCE(lt."Notification Status", '')         AS "Notification Status",
                    COALESCE(lt."SAP Status", '')                  AS "SAP Status",
                    COALESCE(lt."SP57", '')                        AS "SP57",
                    COALESCE(lt."RP57", '')                        AS "RP57",
                    COALESCE(lt."Permit Type", '')                 AS "Permit Type",
                    COALESCE(lt."Anticipated Application Date", '') AS "Anticipated Application Date",
                    COALESCE(lt."Anticipated Issue Date", '')      AS "Anticipated Issue Date",
                    COALESCE(lt."Permit Expiration Date", '')      AS "Permit Expiration Date",
                    COALESCE(lt."Land Management Comments", '')    AS "Land Management Comments",
                    COALESCE(lt."Permit Comment", '')              AS "Permit Comment",
                    COALESCE(mt."Land Notes", '')                  AS "Land Notes",
                    COALESCE(lt."Action", '')                      AS "Action"
                FROM land_tracker lt
                LEFT JOIN mpp_data m ON m."Order" = lt."Order"
                LEFT JOIN manual_tracker mt ON mt."Order" = lt."Order"
                ORDER BY lt."Order"
            """
        elif has_mpp and not has_manual:
            sql = """
                SELECT
                    lt."Order",
                    COALESCE(m."Project Reporting Year", '')      AS "Project Reporting Year",
                    COALESCE(m."MAT", '')                          AS "MAT Code",
                    COALESCE(m."Program", '')                      AS "Program",
                    COALESCE(m."Sub-Category", '')                 AS "Sub-Category",
                    COALESCE(m."Div", '')                          AS "Div",
                    COALESCE(m."Region", '')                       AS "Region",
                    COALESCE(m."Work Plan Date", '')               AS "WPD",
                    COALESCE(m."CLICK Start Date", '')             AS "CLICK Start Date",
                    COALESCE(m."CLICK End Date", '')               AS "CLICK End Date",
                    COALESCE(lt."Notification Status", '')         AS "Notification Status",
                    COALESCE(lt."SAP Status", '')                  AS "SAP Status",
                    COALESCE(lt."SP57", '')                        AS "SP57",
                    COALESCE(lt."RP57", '')                        AS "RP57",
                    COALESCE(lt."Permit Type", '')                 AS "Permit Type",
                    COALESCE(lt."Anticipated Application Date", '') AS "Anticipated Application Date",
                    COALESCE(lt."Anticipated Issue Date", '')      AS "Anticipated Issue Date",
                    COALESCE(lt."Permit Expiration Date", '')      AS "Permit Expiration Date",
                    COALESCE(lt."Land Management Comments", '')    AS "Land Management Comments",
                    COALESCE(lt."Permit Comment", '')              AS "Permit Comment",
                    ''                                             AS "Land Notes",
                    COALESCE(lt."Action", '')                      AS "Action"
                FROM land_tracker lt
                LEFT JOIN mpp_data m ON m."Order" = lt."Order"
                ORDER BY lt."Order"
            """
        elif not has_mpp and has_manual:
            sql = """
                SELECT
                    lt."Order",
                    ''                                             AS "Project Reporting Year",
                    ''                                             AS "MAT Code",
                    ''                                             AS "Program",
                    ''                                             AS "Sub-Category",
                    ''                                             AS "Div",
                    ''                                             AS "Region",
                    ''                                             AS "WPD",
                    ''                                             AS "CLICK Start Date",
                    ''                                             AS "CLICK End Date",
                    COALESCE(lt."Notification Status", '')         AS "Notification Status",
                    COALESCE(lt."SAP Status", '')                  AS "SAP Status",
                    COALESCE(lt."SP57", '')                        AS "SP57",
                    COALESCE(lt."RP57", '')                        AS "RP57",
                    COALESCE(lt."Permit Type", '')                 AS "Permit Type",
                    COALESCE(lt."Anticipated Application Date", '') AS "Anticipated Application Date",
                    COALESCE(lt."Anticipated Issue Date", '')      AS "Anticipated Issue Date",
                    COALESCE(lt."Permit Expiration Date", '')      AS "Permit Expiration Date",
                    COALESCE(lt."Land Management Comments", '')    AS "Land Management Comments",
                    COALESCE(lt."Permit Comment", '')              AS "Permit Comment",
                    COALESCE(mt."Land Notes", '')                  AS "Land Notes",
                    COALESCE(lt."Action", '')                      AS "Action"
                FROM land_tracker lt
                LEFT JOIN manual_tracker mt ON mt."Order" = lt."Order"
                ORDER BY lt."Order"
            """
        else:
            sql = """
                SELECT
                    lt."Order",
                    ''                                             AS "Project Reporting Year",
                    ''                                             AS "MAT Code",
                    ''                                             AS "Program",
                    ''                                             AS "Sub-Category",
                    ''                                             AS "Div",
                    ''                                             AS "Region",
                    ''                                             AS "WPD",
                    ''                                             AS "CLICK Start Date",
                    ''                                             AS "CLICK End Date",
                    COALESCE(lt."Notification Status", '')         AS "Notification Status",
                    COALESCE(lt."SAP Status", '')                  AS "SAP Status",
                    COALESCE(lt."SP57", '')                        AS "SP57",
                    COALESCE(lt."RP57", '')                        AS "RP57",
                    COALESCE(lt."Permit Type", '')                 AS "Permit Type",
                    COALESCE(lt."Anticipated Application Date", '') AS "Anticipated Application Date",
                    COALESCE(lt."Anticipated Issue Date", '')      AS "Anticipated Issue Date",
                    COALESCE(lt."Permit Expiration Date", '')      AS "Permit Expiration Date",
                    COALESCE(lt."Land Management Comments", '')    AS "Land Management Comments",
                    COALESCE(lt."Permit Comment", '')              AS "Permit Comment",
                    ''                                             AS "Land Notes",
                    COALESCE(lt."Action", '')                      AS "Action"
                FROM land_tracker lt
                ORDER BY lt."Order"
            """

        cur.execute(sql)
        rows = cur.fetchall()
        return COLUMNS, rows
