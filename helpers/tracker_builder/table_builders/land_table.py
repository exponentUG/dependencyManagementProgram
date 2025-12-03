# helpers/tracker_builder/table_builders/land_table.py
from __future__ import annotations
import sqlite3
from typing import List, Tuple

COLUMNS: List[str] = [
    "Order",
    "Notification",
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
    "Permit Status",            # <-- NEW
    "Anticipated Application Date",
    "Application Date",         # <-- NEW
    "Anticipated Issue Date",
    "Permit Expiration Date",
    "Exception to Policy",      # <-- NEW
    "Annual Permit",            # <-- NEW
    "Long Lead Permit",         # <-- NEW
    "DSDD Required",            # <-- NEW
    "Land Management Comments",
    "Permit Comment",
    "Land Notes",
    "Action",
]


def _table_exists(cur: sqlite3.Cursor, name: str) -> bool:
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (name,),
    )
    return cur.fetchone() is not None


def get_land_table(db_path: str) -> Tuple[list[str], list[tuple]]:
    """
    Returns (columns, rows) for the Land view.

    Sources:
      - land_tracker (required)
      - mpp_data (optional; for MPP context columns)
      - manual_tracker (optional; for Land Notes)
      - land_data (optional; for live permit status info)

    land_data logic:
      - Match on "Order"
      - If multiple rows per Order, pick the one with the latest
        "Permit Created Date" (string max with COALESCE).
    """
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        # Must have land_tracker to show anything
        if not _table_exists(cur, "land_tracker"):
            return [], []

        has_mpp = _table_exists(cur, "mpp_data")
        has_manual = _table_exists(cur, "manual_tracker")
        has_land = _table_exists(cur, "land_data")

        base_with_land = ""
        if has_land:
            # land_latest = latest row per Order based on Permit Created Date
            base_with_land = """
                WITH land_latest AS (
                    SELECT ld.*
                    FROM land_data ld
                    JOIN (
                        SELECT
                            "Order" AS ord,
                            MAX(COALESCE("Permit Created Date", '')) AS max_pcd
                        FROM land_data
                        GROUP BY "Order"
                    ) mx
                      ON mx.ord = ld."Order"
                     AND COALESCE(ld."Permit Created Date", '') = mx.max_pcd
                )
            """

        # ------------------------------------------------------------------
        # 1) land_tracker + mpp_data + manual_tracker
        # ------------------------------------------------------------------
        if has_mpp and has_manual:
            if has_land:
                sql = base_with_land + """
                    SELECT
                        lt."Order",
                        COALESCE(CAST(m."Notification" AS TEXT), '')   AS "Notification",
                        COALESCE(m."Project Reporting Year", '')       AS "Project Reporting Year",
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
                        COALESCE(ld."Permit Status", '')               AS "Permit Status",
                        COALESCE(lt."Anticipated Application Date", '') AS "Anticipated Application Date",
                        COALESCE(ld."Application Date", '')            AS "Application Date",
                        COALESCE(lt."Anticipated Issue Date", '')      AS "Anticipated Issue Date",
                        COALESCE(lt."Permit Expiration Date", '')      AS "Permit Expiration Date",
                        COALESCE(ld."Exception to Policy", '')         AS "Exception to Policy",
                        COALESCE(ld."Annual Permit", '')               AS "Annual Permit",
                        COALESCE(ld."Long Lead Permit", '')            AS "Long Lead Permit",
                        COALESCE(ld."DSDD Required", '')               AS "DSDD Required",
                        COALESCE(lt."Land Management Comments", '')    AS "Land Management Comments",
                        COALESCE(lt."Permit Comment", '')              AS "Permit Comment",
                        COALESCE(mt."Land Notes", '')                  AS "Land Notes",
                        COALESCE(lt."Action", '')                      AS "Action"
                    FROM land_tracker lt
                    LEFT JOIN mpp_data m
                           ON m."Order" = lt."Order"
                    LEFT JOIN manual_tracker mt
                           ON mt."Order" = lt."Order"
                    LEFT JOIN land_latest ld
                           ON ld."Order" = lt."Order"
                    ORDER BY lt."Order"
                """
            else:
                # land_data missing: keep shape, new columns are blank
                sql = """
                    SELECT
                        lt."Order",
                        COALESCE(CAST(m."Notification" AS TEXT), '')   AS "Notification",
                        COALESCE(m."Project Reporting Year", '')       AS "Project Reporting Year",
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
                        ''                                             AS "Permit Status",
                        COALESCE(lt."Anticipated Application Date", '') AS "Anticipated Application Date",
                        ''                                             AS "Application Date",
                        COALESCE(lt."Anticipated Issue Date", '')      AS "Anticipated Issue Date",
                        COALESCE(lt."Permit Expiration Date", '')      AS "Permit Expiration Date",
                        ''                                             AS "Exception to Policy",
                        ''                                             AS "Annual Permit",
                        ''                                             AS "Long Lead Permit",
                        ''                                             AS "DSDD Required",
                        COALESCE(lt."Land Management Comments", '')    AS "Land Management Comments",
                        COALESCE(lt."Permit Comment", '')              AS "Permit Comment",
                        COALESCE(mt."Land Notes", '')                  AS "Land Notes",
                        COALESCE(lt."Action", '')                      AS "Action"
                    FROM land_tracker lt
                    LEFT JOIN mpp_data m
                           ON m."Order" = lt."Order"
                    LEFT JOIN manual_tracker mt
                           ON mt."Order" = lt."Order"
                    ORDER BY lt."Order"
                """
        # ------------------------------------------------------------------
        # 2) land_tracker + mpp_data only
        # ------------------------------------------------------------------
        elif has_mpp and not has_manual:
            if has_land:
                sql = base_with_land + """
                    SELECT
                        lt."Order",
                        COALESCE(CAST(m."Notification" AS TEXT), '')   AS "Notification",
                        COALESCE(m."Project Reporting Year", '')       AS "Project Reporting Year",
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
                        COALESCE(ld."Permit Status", '')               AS "Permit Status",
                        COALESCE(lt."Anticipated Application Date", '') AS "Anticipated Application Date",
                        COALESCE(ld."Application Date", '')            AS "Application Date",
                        COALESCE(lt."Anticipated Issue Date", '')      AS "Anticipated Issue Date",
                        COALESCE(lt."Permit Expiration Date", '')      AS "Permit Expiration Date",
                        COALESCE(ld."Exception to Policy", '')         AS "Exception to Policy",
                        COALESCE(ld."Annual Permit", '')               AS "Annual Permit",
                        COALESCE(ld."Long Lead Permit", '')            AS "Long Lead Permit",
                        COALESCE(ld."DSDD Required", '')               AS "DSDD Required",
                        COALESCE(lt."Land Management Comments", '')    AS "Land Management Comments",
                        COALESCE(lt."Permit Comment", '')              AS "Permit Comment",
                        ''                                             AS "Land Notes",
                        COALESCE(lt."Action", '')                      AS "Action"
                    FROM land_tracker lt
                    LEFT JOIN mpp_data m
                           ON m."Order" = lt."Order"
                    LEFT JOIN land_latest ld
                           ON ld."Order" = lt."Order"
                    ORDER BY lt."Order"
                """
            else:
                sql = """
                    SELECT
                        lt."Order",
                        COALESCE(CAST(m."Notification" AS TEXT), '')   AS "Notification",
                        COALESCE(m."Project Reporting Year", '')       AS "Project Reporting Year",
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
                        ''                                             AS "Permit Status",
                        COALESCE(lt."Anticipated Application Date", '') AS "Anticipated Application Date",
                        ''                                             AS "Application Date",
                        COALESCE(lt."Anticipated Issue Date", '')      AS "Anticipated Issue Date",
                        COALESCE(lt."Permit Expiration Date", '')      AS "Permit Expiration Date",
                        ''                                             AS "Exception to Policy",
                        ''                                             AS "Annual Permit",
                        ''                                             AS "Long Lead Permit",
                        ''                                             AS "DSDD Required",
                        COALESCE(lt."Land Management Comments", '')    AS "Land Management Comments",
                        COALESCE(lt."Permit Comment", '')              AS "Permit Comment",
                        ''                                             AS "Land Notes",
                        COALESCE(lt."Action", '')                      AS "Action"
                    FROM land_tracker lt
                    LEFT JOIN mpp_data m
                           ON m."Order" = lt."Order"
                    ORDER BY lt."Order"
                """
        # ------------------------------------------------------------------
        # 3) land_tracker + manual_tracker only
        # ------------------------------------------------------------------
        elif not has_mpp and has_manual:
            if has_land:
                sql = base_with_land + """
                    SELECT
                        lt."Order",
                        ''                                             AS "Notification",
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
                        COALESCE(ld."Permit Status", '')               AS "Permit Status",
                        COALESCE(lt."Anticipated Application Date", '') AS "Anticipated Application Date",
                        COALESCE(ld."Application Date", '')            AS "Application Date",
                        COALESCE(lt."Anticipated Issue Date", '')      AS "Anticipated Issue Date",
                        COALESCE(lt."Permit Expiration Date", '')      AS "Permit Expiration Date",
                        COALESCE(ld."Exception to Policy", '')         AS "Exception to Policy",
                        COALESCE(ld."Annual Permit", '')               AS "Annual Permit",
                        COALESCE(ld."Long Lead Permit", '')            AS "Long Lead Permit",
                        COALESCE(ld."DSDD Required", '')               AS "DSDD Required",
                        COALESCE(lt."Land Management Comments", '')    AS "Land Management Comments",
                        COALESCE(lt."Permit Comment", '')              AS "Permit Comment",
                        COALESCE(mt."Land Notes", '')                  AS "Land Notes",
                        COALESCE(lt."Action", '')                      AS "Action"
                    FROM land_tracker lt
                    LEFT JOIN manual_tracker mt
                           ON mt."Order" = lt."Order"
                    LEFT JOIN land_latest ld
                           ON ld."Order" = lt."Order"
                    ORDER BY lt."Order"
                """
            else:
                sql = """
                    SELECT
                        lt."Order",
                        ''                                             AS "Notification",
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
                        ''                                             AS "Permit Status",
                        COALESCE(lt."Anticipated Application Date", '') AS "Anticipated Application Date",
                        ''                                             AS "Application Date",
                        COALESCE(lt."Anticipated Issue Date", '')      AS "Anticipated Issue Date",
                        COALESCE(lt."Permit Expiration Date", '')      AS "Permit Expiration Date",
                        ''                                             AS "Exception to Policy",
                        ''                                             AS "Annual Permit",
                        ''                                             AS "Long Lead Permit",
                        ''                                             AS "DSDD Required",
                        COALESCE(lt."Land Management Comments", '')    AS "Land Management Comments",
                        COALESCE(lt."Permit Comment", '')              AS "Permit Comment",
                        COALESCE(mt."Land Notes", '')                  AS "Land Notes",
                        COALESCE(lt."Action", '')                      AS "Action"
                    FROM land_tracker lt
                    LEFT JOIN manual_tracker mt
                           ON mt."Order" = lt."Order"
                    ORDER BY lt."Order"
                """
        # ------------------------------------------------------------------
        # 4) land_tracker only
        # ------------------------------------------------------------------
        else:
            if has_land:
                sql = base_with_land + """
                    SELECT
                        lt."Order",
                        ''                                             AS "Notification",
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
                        COALESCE(ld."Permit Status", '')               AS "Permit Status",
                        COALESCE(lt."Anticipated Application Date", '') AS "Anticipated Application Date",
                        COALESCE(ld."Application Date", '')            AS "Application Date",
                        COALESCE(lt."Anticipated Issue Date", '')      AS "Anticipated Issue Date",
                        COALESCE(lt."Permit Expiration Date", '')      AS "Permit Expiration Date",
                        COALESCE(ld."Exception to Policy", '')         AS "Exception to Policy",
                        COALESCE(ld."Annual Permit", '')               AS "Annual Permit",
                        COALESCE(ld."Long Lead Permit", '')            AS "Long Lead Permit",
                        COALESCE(ld."DSDD Required", '')               AS "DSDD Required",
                        COALESCE(lt."Land Management Comments", '')    AS "Land Management Comments",
                        COALESCE(lt."Permit Comment", '')              AS "Permit Comment",
                        ''                                             AS "Land Notes",
                        COALESCE(lt."Action", '')                      AS "Action"
                    FROM land_tracker lt
                    LEFT JOIN land_latest ld
                           ON ld."Order" = lt."Order"
                    ORDER BY lt."Order"
                """
            else:
                sql = """
                    SELECT
                        lt."Order",
                        ''                                             AS "Notification",
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
                        ''                                             AS "Permit Status",
                        COALESCE(lt."Anticipated Application Date", '') AS "Anticipated Application Date",
                        ''                                             AS "Application Date",
                        COALESCE(lt."Anticipated Issue Date", '')      AS "Anticipated Issue Date",
                        COALESCE(lt."Permit Expiration Date", '')      AS "Permit Expiration Date",
                        ''                                             AS "Exception to Policy",
                        ''                                             AS "Annual Permit",
                        ''                                             AS "Long Lead Permit",
                        ''                                             AS "DSDD Required",
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
