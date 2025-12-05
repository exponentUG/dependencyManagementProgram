# helpers/wmp_tracker_builder/table_builders/master_table.py
from __future__ import annotations
import os
import sqlite3
from typing import List, Tuple

COLUMNS: List[str] = [
    "Order",
    "Notification",          # <-- from mpp_data
    "Project Reporting Year",
    "MAT Code",
    "Program",
    "Sub-Category",
    "WMP Commitments",
    "Priority",
    "Div",
    "Region",
    "Program Manager",       # <-- NEW COLUMN (from static_lists.pm_list)
    "WPD",
    "Est Req",
    "Shovel Ready Date",
    "Resource",
    "CLICK Start Date",
    "CLICK End Date",
    "Notification Status",
    "SAP Status",
    "Order User Status",
    "Open Dependencies",
    "Stage of Job",
    "SP56",
    "RP56",
    "SP57",
    "RP57",
    "DS42",
    "PC20",
    "DS76",
    "PC24",
    "DS11",
    "PC21",
    "AP10",
    "AP25",
    "DS28",
    "DS73",
    "Est Out Date",
    "PEND In",
    "LEAPS Combined Expected Out Date",
    "Completion Deadline Date",
]


def _table_exists(cur: sqlite3.Cursor, name: str) -> bool:
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (name,),
    )
    return cur.fetchone() is not None


def get_master_table(db_path: str) -> Tuple[list[str], list[tuple]]:
    """
    Returns (columns, rows) for the Master view.

    - Requires order_tracking_list.
    - mpp_data, open_dependencies, and sap_tracker are all optional:
        * If mpp_data is missing, all MPP fields are ''.
        * If open_dependencies is missing, Open Dependencies / Stage of Job are ''.
        * If sap_tracker is missing, all SP/RP/DS/PC/AP fields are ''.
    - If 'Stage of Job' column doesn't exist yet in open_dependencies, it is shown as ''.
    - Program Manager is looked up from data/static_lists.sqlite3, table pm_list,
      by matching mpp_data."MAT" to pm_list."MAT". If no match or no table, it is ''.
    """
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        # Base table is always order_tracking_list; if missing, nothing to show.
        if not _table_exists(cur, "order_tracking_list"):
            return [], []

        has_mpp = _table_exists(cur, "mpp_data")
        has_open = _table_exists(cur, "open_dependencies")
        has_sap = _table_exists(cur, "sap_tracker")

        # Check for static_lists.pm_list
        static_db_path = os.path.join("data", "static_lists.sqlite3")
        has_pm_list = False
        if os.path.exists(static_db_path):
            try:
                cur.execute('ATTACH DATABASE ? AS static_db', (static_db_path,))
                cur.execute(
                    "SELECT name FROM static_db.sqlite_master "
                    "WHERE type='table' AND name='pm_list' LIMIT 1"
                )
                has_pm_list = cur.fetchone() is not None
            except Exception:
                has_pm_list = False

        # Does open_dependencies have 'Stage of Job'?
        has_stage = False
        if has_open:
            cur.execute("PRAGMA table_info(open_dependencies)")
            od_cols = {row[1] for row in cur.fetchall()}
            has_stage = "Stage of Job" in od_cols

        # --- Expression pieces depending on table availability -----------------
        # MPP fields
        if has_mpp:
            notif_expr = 'COALESCE(CAST(m."Notification" AS TEXT), \'\')'
            pry_expr = 'COALESCE(m."Project Reporting Year", \'\')'
            mat_expr = 'COALESCE(m."MAT", \'\')'
            prog_expr = 'COALESCE(m."Program", \'\')'
            subcat_expr = 'COALESCE(m."Sub-Category", \'\')'
            wmp_commit_expr = 'COALESCE(m."WMP Commitments", \'\')'
            priority_expr = 'COALESCE(m."Priority", \'\')'
            div_expr = 'COALESCE(m."Div", \'\')'
            region_expr = 'COALESCE(m."Region", \'\')'
            wpd_expr = 'COALESCE(m."Work Plan Date", \'\')'
            est_req_expr = 'COALESCE(m."Est Req", \'\')'
            shovel_expr = 'COALESCE(m."Shovel Ready Date", \'\')'
            resource_expr = 'COALESCE(m."Resource", \'\')'
            click_start_expr = 'COALESCE(m."CLICK Start Date", \'\')'
            click_end_expr = 'COALESCE(m."CLICK End Date", \'\')'
            notif_status_expr = 'COALESCE(m."Notif Status", \'\')'
            sap_status_expr = 'COALESCE(m."Primary Status", \'\')'
            order_user_expr = 'COALESCE(m."Order User Status", \'\')'
            est_out_expr = 'COALESCE(m."Est Out Date", \'\')'
            pend_in_expr = 'COALESCE(m."PEND In", \'\')'
            leaps_expr = 'COALESCE(m."LEAPs Combined Exp Out Date", \'\')'
            compl_deadline_expr = 'COALESCE(m."Completion Deadline Date", \'\')'

            mpp_join = 'LEFT JOIN mpp_data m           ON m."Order" = ot."Order"'
        else:
            notif_expr = "''"
            pry_expr = "''"
            mat_expr = "''"
            prog_expr = "''"
            subcat_expr = "''"
            wmp_commit_expr = "''"
            priority_expr = "''"
            div_expr = "''"
            region_expr = "''"
            wpd_expr = "''"
            est_req_expr = "''"
            shovel_expr = "''"
            resource_expr = "''"
            click_start_expr = "''"
            click_end_expr = "''"
            notif_status_expr = "''"
            sap_status_expr = "''"
            order_user_expr = "''"
            est_out_expr = "''"
            pend_in_expr = "''"
            leaps_expr = "''"
            compl_deadline_expr = "''"

            mpp_join = ""

        # Program Manager from static_db.pm_list (by MAT)
        if has_mpp and has_pm_list:
            pm_expr = 'COALESCE(pm."Program Manager", \'\')'
            pm_join = 'LEFT JOIN static_db.pm_list pm ON pm."MAT" = m."MAT"'
        else:
            pm_expr = "''"
            pm_join = ""

        # open_dependencies fields
        if has_open:
            open_deps_expr = 'COALESCE(od."Open Dependencies", \'\')'
            if has_stage:
                stage_expr = 'COALESCE(od."Stage of Job", \'\')'
            else:
                stage_expr = "''"
            open_join = 'LEFT JOIN open_dependencies od ON od."Order" = ot."Order"'
        else:
            open_deps_expr = "''"
            stage_expr = "''"
            open_join = ""

        # sap_tracker fields
        if has_sap:
            sp56_expr = 'COALESCE(st."SP56", \'\')'
            rp56_expr = 'COALESCE(st."RP56", \'\')'
            sp57_expr = 'COALESCE(st."SP57", \'\')'
            rp57_expr = 'COALESCE(st."RP57", \'\')'
            ds42_expr = 'COALESCE(st."DS42", \'\')'
            pc20_expr = 'COALESCE(st."PC20", \'\')'
            ds76_expr = 'COALESCE(st."DS76", \'\')'
            pc24_expr = 'COALESCE(st."PC24", \'\')'
            ds11_expr = 'COALESCE(st."DS11", \'\')'
            pc21_expr = 'COALESCE(st."PC21", \'\')'
            ap10_expr = 'COALESCE(st."AP10", \'\')'
            ap25_expr = 'COALESCE(st."AP25", \'\')'
            ds28_expr = 'COALESCE(st."DS28", \'\')'
            ds73_expr = 'COALESCE(st."DS73", \'\')'
            sap_join = 'LEFT JOIN sap_tracker st       ON st."Order" = ot."Order"'
        else:
            sp56_expr = "''"
            rp56_expr = "''"
            sp57_expr = "''"
            rp57_expr = "''"
            ds42_expr = "''"
            pc20_expr = "''"
            ds76_expr = "''"
            pc24_expr = "''"
            ds11_expr = "''"
            pc21_expr = "''"
            ap10_expr = "''"
            ap25_expr = "''"
            ds28_expr = "''"
            ds73_expr = "''"
            sap_join = ""

        # --- Final SQL (single query, joins conditional on availability) -------
        sql = f"""
            SELECT
                ot."Order",
                {notif_expr}          AS "Notification",
                {pry_expr}            AS "Project Reporting Year",
                {mat_expr}            AS "MAT Code",
                {prog_expr}           AS "Program",
                {subcat_expr}         AS "Sub-Category",
                {wmp_commit_expr}     AS "WMP Commitments",
                {priority_expr}       AS "Priority",
                {div_expr}            AS "Div",
                {region_expr}         AS "Region",
                {pm_expr}             AS "Program Manager",
                {wpd_expr}            AS "WPD",
                {est_req_expr}        AS "Est Req",
                {shovel_expr}         AS "Shovel Ready Date",
                {resource_expr}       AS "Resource",
                {click_start_expr}    AS "CLICK Start Date",
                {click_end_expr}      AS "CLICK End Date",
                {notif_status_expr}   AS "Notification Status",
                {sap_status_expr}     AS "SAP Status",
                {order_user_expr}     AS "Order User Status",
                {open_deps_expr}      AS "Open Dependencies",
                {stage_expr}          AS "Stage of Job",
                {sp56_expr}           AS "SP56",
                {rp56_expr}           AS "RP56",
                {sp57_expr}           AS "SP57",
                {rp57_expr}           AS "RP57",
                {ds42_expr}           AS "DS42",
                {pc20_expr}           AS "PC20",
                {ds76_expr}           AS "DS76",
                {pc24_expr}           AS "PC24",
                {ds11_expr}           AS "DS11",
                {pc21_expr}           AS "PC21",
                {ap10_expr}           AS "AP10",
                {ap25_expr}           AS "AP25",
                {ds28_expr}           AS "DS28",
                {ds73_expr}           AS "DS73",
                {est_out_expr}        AS "Est Out Date",
                {pend_in_expr}        AS "PEND In",
                {leaps_expr}          AS "LEAPS Combined Expected Out Date",
                {compl_deadline_expr} AS "Completion Deadline Date"
            FROM order_tracking_list ot
            {mpp_join}
            {pm_join}
            {open_join}
            {sap_join}
            ORDER BY ot."Order"
        """

        cur.execute(sql)
        rows = cur.fetchall()
        return COLUMNS, rows
