# helpers/wmp_tracker_builder/dependency_trackers/permit.py
from __future__ import annotations
import sqlite3
from datetime import datetime

# Desired final column order for permit_tracker
PERMIT_TRACKER_COLS = [
    "Order",
    "Notification Status",
    "SAP Status",
    "SP56 Status",
    "RP56 Status",
    "E Permit Status",
    "Submit Days",
    "Permit Expiration Date",
    "Work Plan Date",
    "CLICK Start Date",
    "CLICK End Date",
    "LEAPS Cycle Time",
    "Action",
]

def _ensure_table(conn: sqlite3.Connection) -> None:
    """
    Ensure permit_tracker exists with the desired schema.
    If it exists but is missing columns (e.g., 'SAP Status' or 'Notification Status'),
    migrate it in-place and preserve data.
    """
    cur = conn.cursor()

    # Create if missing with the full, current schema
    cur.execute("""
        CREATE TABLE IF NOT EXISTS permit_tracker (
            "Order" INTEGER PRIMARY KEY,
            "Notification Status" TEXT,
            "SAP Status" TEXT,
            "SP56 Status" TEXT,
            "RP56 Status" TEXT,
            "E Permit Status" TEXT,
            "Submit Days" TEXT,
            "Permit Expiration Date" TEXT,
            "Work Plan Date" TEXT,
            "CLICK Start Date" TEXT,
            "CLICK End Date" TEXT,
            "LEAPS Cycle Time" TEXT,
            "Action" TEXT
        )
    """)
    conn.commit()

    # Check actual columns
    cur.execute("PRAGMA table_info(permit_tracker)")
    existing_cols = [row[1] for row in cur.fetchall()]

    # If already correct, done
    if existing_cols == PERMIT_TRACKER_COLS:
        return

    # If differs, migrate to new schema order
    cur.execute("""
        CREATE TABLE IF NOT EXISTS permit_tracker__new (
            "Order" INTEGER PRIMARY KEY,
            "Notification Status" TEXT,
            "SAP Status" TEXT,
            "SP56 Status" TEXT,
            "RP56 Status" TEXT,
            "E Permit Status" TEXT,
            "Submit Days" TEXT,
            "Permit Expiration Date" TEXT,
            "Work Plan Date" TEXT,
            "CLICK Start Date" TEXT,
            "CLICK End Date" TEXT,
            "LEAPS Cycle Time" TEXT,
            "Action" TEXT
        )
    """)

    # Map existing columns where present, else NULL
    select_parts = []
    for c in PERMIT_TRACKER_COLS:
        if c in existing_cols:
            select_parts.append(f'"{c}"')
        else:
            select_parts.append(f'NULL AS "{c}"')
    select_sql = ", ".join(select_parts)
    cols_csv = ", ".join(f'"{c}"' for c in PERMIT_TRACKER_COLS)

    cur.execute((
        f'INSERT OR REPLACE INTO permit_tracker__new ({cols_csv}) '
        f'SELECT {select_sql} FROM permit_tracker'
    ))

    cur.execute("DROP TABLE permit_tracker")
    cur.execute("ALTER TABLE permit_tracker__new RENAME TO permit_tracker")
    conn.commit()


def _to_iso_case(col_expr: str) -> str:
    """
    Convert MM/DD/YYYY text to ISO YYYY-MM-DD using pure SQL substrings.
    Anything else -> NULL (your MPP import already coerces to MM/DD/YYYY).
    """
    return f"""
    CASE
      WHEN {col_expr} IS NOT NULL AND LENGTH({col_expr})=10
           AND SUBSTR({col_expr},3,1)='/' AND SUBSTR({col_expr},6,1)='/'
        THEN SUBSTR({col_expr},7,4) || '-' || SUBSTR({col_expr},1,2) || '-' || SUBSTR({col_expr},4,2)
      ELSE NULL
    END
    """

def _iso_to_mdy(expr_iso: str) -> str:
    """Render an ISO date expression back to MM/DD/YYYY (text) inside SQL."""
    return f"""
    CASE
      WHEN {expr_iso} IS NOT NULL
        THEN SUBSTR({expr_iso},6,2) || '/' || SUBSTR({expr_iso},9,2) || '/' || SUBSTR({expr_iso},1,4)
      ELSE ''
    END
    """

def build_permit_tracker(conn: sqlite3.Connection) -> int:
    """
    Rebuilds the permit_tracker table per spec, including Notification Status & SAP Status.
    Returns affected rows (inserted/updated).
    """
    _ensure_table(conn)
    cur = conn.cursor()

    today_iso = datetime.now().date().isoformat()

    # 1) Orders with Permit='Pending'
    cur.executescript("""
        DROP TABLE IF EXISTS __pt_orders;
        CREATE TEMP TABLE __pt_orders AS
        SELECT od."Order"
        FROM open_dependencies od
        WHERE od."Permit" = 'Pending';
    """)

    # 2) SP56/RP56 from sap_tracker
    cur.executescript("""
        DROP TABLE IF EXISTS __pt_sap;
        CREATE TEMP TABLE __pt_sap AS
        SELECT st."Order",
               UPPER(TRIM(COALESCE(st."SP56", ''))) AS sp56,
               UPPER(TRIM(COALESCE(st."RP56", ''))) AS rp56
        FROM sap_tracker st
        INNER JOIN __pt_orders o ON o."Order" = st."Order";
    """)

    # 3) EPW one-per-order and normalize expiration
    cur.executescript("""
        DROP TABLE IF EXISTS __pt_epw_one;
        CREATE TEMP TABLE __pt_epw_one AS
        WITH c AS (
          SELECT
              "Order Number" AS order_num,
              "EPW Status" AS epw_status,
              "Epermit Update" AS epermit_update,
              "EPW Submit Days in Age" AS submit_days,
              "EPW Expiration Date" AS epw_exp_raw,
              "Cycle Time" AS cycle_time,
              rowid AS rid
          FROM epw_data
        )
        SELECT c1.order_num, c1.epw_status, c1.epermit_update, c1.submit_days, c1.epw_exp_raw, c1.cycle_time
        FROM c c1
        WHERE c1.rid = (SELECT MIN(c2.rid) FROM c c2 WHERE c2.order_num = c1.order_num);

        DROP TABLE IF EXISTS __pt_epw_norm;
        CREATE TEMP TABLE __pt_epw_norm AS
        SELECT
            eo.order_num AS "Order",
            eo.epw_status,
            eo.epermit_update,
            eo.submit_days,
            eo.cycle_time,
            CASE
              WHEN eo.epw_exp_raw IS NOT NULL AND LENGTH(eo.epw_exp_raw)=10
                   AND SUBSTR(eo.epw_exp_raw,3,1)='/' AND SUBSTR(eo.epw_exp_raw,6,1)='/'
                THEN SUBSTR(eo.epw_exp_raw,7,4) || '-' || SUBSTR(eo.epw_exp_raw,1,2) || '-' || SUBSTR(eo.epw_exp_raw,4,2)
              ELSE NULL
            END AS epw_exp_iso
        FROM __pt_epw_one eo;
    """)

    # 4) MPP (already coerced to MM/DD/YYYY) -> ISO; plus Primary/Notif Status
    cur.executescript(f"""
        DROP TABLE IF EXISTS __pt_mpp;
        CREATE TEMP TABLE __pt_mpp AS
        SELECT
            m."Order",
            m."Primary Status"   AS primary_status,
            m."Notif Status"     AS notif_status,
            m."Work Plan Date"   AS wpd_raw,
            m."CLICK Start Date" AS click_start_raw,
            m."CLICK End Date"   AS click_end_raw,
            m."Permit Exp Date"  AS mpp_exp_raw
        FROM mpp_data m
        INNER JOIN __pt_orders o ON o."Order" = m."Order";

        DROP TABLE IF EXISTS __pt_mpp_norm;
        CREATE TEMP TABLE __pt_mpp_norm AS
        SELECT
            "Order",
            primary_status,
            notif_status,
            {_to_iso_case('wpd_raw')}          AS wpd_iso,
            {_to_iso_case('click_start_raw')}  AS click_start_iso,
            {_to_iso_case('click_end_raw')}    AS click_end_iso,
            {_to_iso_case('mpp_exp_raw')}      AS mpp_exp_iso
        FROM __pt_mpp;
    """)

    # 5) Combine
    cur.executescript("""
        DROP TABLE IF EXISTS __pt_combined;
        CREATE TEMP TABLE __pt_combined AS
        SELECT
            o."Order",
            m.notif_status,
            m.primary_status,
            s.sp56, s.rp56,
            e.epw_status,
            e.epermit_update,
            e.submit_days,
            e.cycle_time,
            e.epw_exp_iso,
            m.wpd_iso, m.click_start_iso, m.click_end_iso, m.mpp_exp_iso
        FROM __pt_orders o
        LEFT JOIN __pt_sap      s ON s."Order" = o."Order"
        LEFT JOIN __pt_epw_norm e ON e."Order" = o."Order"
        LEFT JOIN __pt_mpp_norm m ON m."Order" = o."Order";
    """)

    # 6) E Permit Status derivation
    cur.executescript("""
        DROP TABLE IF EXISTS __pt_with_epermit;
        CREATE TEMP TABLE __pt_with_epermit AS
        SELECT
            "Order",
            notif_status,
            primary_status,
            sp56, rp56,
            submit_days,
            cycle_time,
            epw_exp_iso,
            wpd_iso, click_start_iso, click_end_iso, mpp_exp_iso,
            CASE
              WHEN TRIM(UPPER(COALESCE(epw_status,''))) = 'NOT ACTIVATED'
                THEN 'Not Needed'
              WHEN COALESCE(TRIM(epermit_update),'') = ''
                THEN 'Not Created'
              WHEN INSTR(epermit_update, ';') > 0
                THEN TRIM(SUBSTR(epermit_update, INSTR(epermit_update, ';') + 1))
              ELSE TRIM(epermit_update)
            END AS epermit_status
        FROM __pt_combined;
    """)

    # 7) Pick later expiration (ISO)
    cur.executescript("""
        DROP TABLE IF EXISTS __pt_with_exp;
        CREATE TEMP TABLE __pt_with_exp AS
        SELECT
            "Order",
            notif_status,
            primary_status,
            sp56, rp56,
            submit_days,
            cycle_time,
            wpd_iso, click_start_iso, click_end_iso,
            CASE
              WHEN epw_exp_iso IS NULL AND mpp_exp_iso IS NULL THEN NULL
              WHEN epw_exp_iso IS NULL THEN mpp_exp_iso
              WHEN mpp_exp_iso IS NULL THEN epw_exp_iso
              WHEN date(epw_exp_iso) >= date(mpp_exp_iso) THEN epw_exp_iso
              ELSE mpp_exp_iso
            END AS final_exp_iso,
            epermit_status
        FROM __pt_with_epermit;
    """)

    # 8) Action logic (with bound params)
    cur.executescript("DROP TABLE IF EXISTS __pt_action;")
    cur.execute("""
        CREATE TEMP TABLE __pt_action AS
        SELECT
          "Order",
          notif_status,
          primary_status,
          sp56, rp56,
          submit_days,
          cycle_time,
          wpd_iso, click_start_iso, click_end_iso,
          final_exp_iso,
          epermit_status,
          CASE
            WHEN epermit_status = 'Not Needed'
              THEN 'Permit not needed. Please close SP/RP56.'

            WHEN final_exp_iso IS NOT NULL AND date(final_exp_iso) >= date(?)
              THEN 'Please confirm permit is approved and complete SAP task.'

            WHEN final_exp_iso IS NOT NULL AND date(final_exp_iso) < date(?) AND (click_start_iso IS NULL)
              THEN 'Please provide CLICK Date for extension.'

            WHEN final_exp_iso IS NOT NULL AND date(final_exp_iso) < date(?) AND date(click_start_iso) >= date(?)
              THEN 'Please request for extension.'

            WHEN final_exp_iso IS NOT NULL AND date(final_exp_iso) < date(?) AND (click_start_iso IS NOT NULL) AND date(click_start_iso) < date(?)
              THEN 'Please request for extension.'

            WHEN epermit_status = 'In Progress' AND wpd_iso IS NOT NULL AND date(wpd_iso) < date(?)
              THEN 'In progress but past WPD. Please escalate.'

            WHEN epermit_status = 'In Progress' AND (wpd_iso IS NULL OR date(wpd_iso) >= date(?))
              THEN 'In progress.'

            WHEN epermit_status = 'Not Created' AND (wpd_iso IS NOT NULL) AND date(wpd_iso) >= date(?)
              THEN 'Permit not created. Need current WPD.'

            WHEN epermit_status = 'Not Created' AND (wpd_iso IS NOT NULL) AND date(wpd_iso) < date(?)
              THEN 'Not created and past WPD. Please escalate.'

            WHEN epermit_status = 'Submitted' AND CAST(COALESCE(NULLIF(TRIM(submit_days), ''), '0') AS INTEGER) > 45
              THEN 'Submitted over 45 days. Please provide update.'

            WHEN epermit_status = 'Submitted' AND CAST(COALESCE(NULLIF(TRIM(submit_days), ''), '0') AS INTEGER) <= 45
              THEN 'In progress.'

            ELSE 'check'
          END AS action_text
        FROM __pt_with_exp
    """, (
        today_iso,   # future check
        today_iso,   # past (no click)
        today_iso,   # past exp
        today_iso,   # click >= today
        today_iso,   # past exp
        today_iso,   # click < today
        today_iso,   # WPD past
        today_iso,   # WPD future or null
        "2026-01-01",# WPD >= 2026-01-01 (per current logic)
        today_iso,   # WPD past
    ))

    # 9) Final rows with formatted MM/DD/YYYY + Notification/SAP Status columns
    cur.executescript(f"""
        DROP TABLE IF EXISTS __permit_tracker_final;
        CREATE TEMP TABLE __permit_tracker_final AS
        SELECT
            "Order",
            notif_status        AS "Notification Status",
            primary_status      AS "SAP Status",
            sp56                AS "SP56 Status",
            rp56                AS "RP56 Status",
            epermit_status      AS "E Permit Status",
            submit_days         AS "Submit Days",
            {_iso_to_mdy('final_exp_iso')}    AS "Permit Expiration Date",
            {_iso_to_mdy('wpd_iso')}          AS "Work Plan Date",
            {_iso_to_mdy('click_start_iso')}  AS "CLICK Start Date",
            {_iso_to_mdy('click_end_iso')}    AS "CLICK End Date",
            cycle_time          AS "LEAPS Cycle Time",
            action_text         AS "Action"
        FROM __pt_action;
    """)

    # 10) Upsert into final table
    before = conn.total_changes
    cols_csv = ", ".join(f'"{c}"' for c in PERMIT_TRACKER_COLS)

    # NEW: remove rows that are no longer in the current pending set
    cur.execute('DELETE FROM permit_tracker WHERE "Order" NOT IN (SELECT "Order" FROM __permit_tracker_final)')

    cur.executescript(f"""
        INSERT OR REPLACE INTO permit_tracker ({cols_csv})
        SELECT {cols_csv}
        FROM __permit_tracker_final;
    """)
    conn.commit()
    return conn.total_changes - before
