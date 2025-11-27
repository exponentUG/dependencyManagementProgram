# helpers/wmp_tracker_builder/dependency_trackers/land.py
from __future__ import annotations
import sqlite3
from datetime import datetime, date
import pandas as pd

from helpers.misc.comments import extract_latest_comment_block
from helpers.misc.comments_parser import parse_comment_semantics  # uses updated parser

# Final desired column order
LAND_COLS = [
    "Order",
    "Notification Status",
    "SAP Status",
    "SP57",
    "RP57",
    "Permit Type",
    "Permit Status",
    "Anticipated Application Date",
    "Anticipated Issue Date",
    "Permit Expiration Date",
    "Land Management Comments",
    "Permit Comment",
    "Latest Comment Date",
    "LAN ID",
    "Latest Comment",
    "Parsed Action",
    "Parsed Anticipated Issue Date",
    "Parsed Permit Expiration Date",
    "Action",
]

def _ensure_table(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    # DDL / multi statements must use executescript
    cur.executescript("""
        DROP TABLE IF EXISTS land_tracker__new;

        CREATE TABLE IF NOT EXISTS land_tracker (
            "Order" INTEGER PRIMARY KEY,
            "Notification Status" TEXT,
            "SAP Status" TEXT,
            "SP57" TEXT,
            "RP57" TEXT,
            "Permit Type" TEXT,
            "Permit Status" TEXT,
            "Anticipated Application Date" TEXT,
            "Anticipated Issue Date" TEXT,
            "Permit Expiration Date" TEXT,
            "Land Management Comments" TEXT,
            "Permit Comment" TEXT,
            "Latest Comment Date" TEXT,
            "LAN ID" TEXT,
            "Latest Comment" TEXT,
            "Parsed Action" TEXT,
            "Parsed Anticipated Issue Date" TEXT,
            "Parsed Permit Expiration Date" TEXT,
            "Action" TEXT
        );
    """)
    conn.commit()

    # Current physical order?
    cur.execute("PRAGMA table_info(land_tracker)")
    existing_cols = [row[1] for row in cur.fetchall()]
    if existing_cols == LAND_COLS:
        return

    # Add any missing columns
    for col in LAND_COLS:
        if col not in existing_cols and col != "Order":
            cur.execute(f'ALTER TABLE land_tracker ADD COLUMN "{col}" TEXT')
    conn.commit()

    # Re-order into canonical layout
    cur.execute("PRAGMA table_info(land_tracker)")
    existing_cols = [row[1] for row in cur.fetchall()]

    cols_csv = ", ".join(f'"{c}"' for c in LAND_COLS)
    select_parts = [(f'"{c}"' if c in existing_cols else f'NULL AS "{c}"') for c in LAND_COLS]
    select_sql = ", ".join(select_parts)

    cur.executescript(f"""
        CREATE TABLE land_tracker__new (
            "Order" INTEGER PRIMARY KEY,
            "Notification Status" TEXT,
            "SAP Status" TEXT,
            "SP57" TEXT,
            "RP57" TEXT,
            "Permit Type" TEXT,
            "Permit Status" TEXT,
            "Anticipated Application Date" TEXT,
            "Anticipated Issue Date" TEXT,
            "Permit Expiration Date" TEXT,
            "Land Management Comments" TEXT,
            "Permit Comment" TEXT,
            "Latest Comment Date" TEXT,
            "LAN ID" TEXT,
            "Latest Comment" TEXT,
            "Parsed Action" TEXT,
            "Parsed Anticipated Issue Date" TEXT,
            "Parsed Permit Expiration Date" TEXT,
            "Action" TEXT
        );

        INSERT OR REPLACE INTO land_tracker__new ({cols_csv})
        SELECT {select_sql} FROM land_tracker;

        DROP TABLE land_tracker;
        ALTER TABLE land_tracker__new RENAME TO land_tracker;
    """)
    conn.commit()

def _to_iso_case(col_expr: str) -> str:
    # Returns a CASE expression to convert MM/DD/YYYY -> YYYY-MM-DD; else NULL
    return f"""
    CASE
      WHEN {col_expr} IS NOT NULL AND LENGTH({col_expr})=10
           AND SUBSTR({col_expr},3,1)='/' AND SUBSTR({col_expr},6,1)='/'
        THEN SUBSTR({col_expr},7,4) || '-' || SUBSTR({col_expr},1,2) || '-' || SUBSTR({col_expr},4,2)
      ELSE NULL
    END
    """

def _to_iso_case_flex(col_expr: str) -> str:
    """
    Flexible M/D/YYYY or MM/DD/YYYY -> YYYY-MM-DD
    Returns NULL if not parseable.
    """
    expr = f"TRIM({col_expr})"
    return f"""
    CASE
      WHEN {expr} IS NULL OR {expr} = '' OR {expr} = '-'
        THEN NULL
      WHEN instr({expr}, '/') = 0
        THEN NULL
      ELSE
        -- month = text before first '/'
        -- day   = text between first and second '/'
        -- year  = last 4 chars
        printf(
          '%04d-%02d-%02d',
          CAST(substr({expr}, -4) AS INTEGER),
          CAST(substr({expr}, 1, instr({expr}, '/')-1) AS INTEGER),
          CAST(
            substr(
              {expr},
              instr({expr}, '/')+1,
              instr(substr({expr}, instr({expr}, '/')+1), '/')-1
            ) AS INTEGER
          )
        )
    END
    """


def build_land_tracker(conn: sqlite3.Connection) -> int:
    _ensure_table(conn)
    cur = conn.cursor()

    # 1) Orders with Land pending
    cur.executescript("""
        DROP TABLE IF EXISTS __land_orders;
        CREATE TEMP TABLE __land_orders AS
        SELECT od."Order"
        FROM open_dependencies od
        WHERE od."Land" = 'Pending';
    """)

    # 2) mpp_data  (now also pulling WPD + PRY)
    cur.executescript(f"""
        DROP TABLE IF EXISTS __land_mpp;
        CREATE TEMP TABLE __land_mpp AS
        SELECT
            m."Order",
            m."Notif Status"   AS notif_status,
            m."Primary Status" AS sap_status,

            m."Work Plan Date" AS work_plan_date_raw,
            {_to_iso_case('m."Work Plan Date"')} AS work_plan_date_iso,

            CAST(NULLIF(TRIM(COALESCE(m."Project Reporting Year", '')), '') AS INTEGER) AS pry_int
        FROM mpp_data m
        INNER JOIN __land_orders o ON o."Order" = m."Order";
    """)

    # 3) land_data snapshot + ISO helpers (pick latest Permit Created Date per Order)
    cur.executescript(f"""
        DROP TABLE IF EXISTS __land_ld;
        CREATE TEMP TABLE __land_ld AS
        WITH src AS (
            SELECT
                ld."Order" AS order_num,
                ld."Permit Status" AS permit_status_raw,
                ld."Permit Type"   AS permit_type_raw,
                ld."Anticipated Application" AS anticipated_app_raw,
                ld."Anticipated Issued Date" AS anticipated_issue_raw,
                ld."Permit Expiration"       AS permit_expiration_raw,
                ld."Land Mgmt Project Status Comments" AS land_mgmt_comments,
                ld."Permit Comment" AS permit_comment,
                ld."Permit Created Date" AS created_raw,
                {_to_iso_case('ld."Permit Created Date"')}        AS created_iso,
                {_to_iso_case('ld."Anticipated Application"')}    AS anticipated_app_iso,
                rowid AS rid
            FROM land_data ld
        ),
        chosen AS (
            SELECT s.*
            FROM src s
            WHERE
              (
                s.created_iso IS NOT NULL
                AND s.created_iso = (
                    SELECT MAX(s2.created_iso)
                    FROM src s2
                    WHERE s2.order_num = s.order_num
                      AND s2.created_iso IS NOT NULL
                )
              )
              OR
              (
                s.created_iso IS NULL
                AND NOT EXISTS (
                    SELECT 1 FROM src s3
                    WHERE s3.order_num = s.order_num AND s3.created_iso IS NOT NULL
                )
                AND s.rid = (
                    SELECT MIN(s4.rid) FROM src s4 WHERE s4.order_num = s.order_num
                )
              )
        )
        SELECT
            c.order_num AS "Order",
            CASE WHEN TRIM(COALESCE(c.permit_status_raw,''))='' THEN 'Unknown'
                 ELSE c.permit_status_raw END AS permit_status,
            NULLIF(TRIM(COALESCE(c.permit_type_raw,'')), '') AS permit_type,
            COALESCE(c.anticipated_app_raw, '')        AS anticipated_app_date,
            c.anticipated_app_iso                      AS anticipated_app_iso,
            COALESCE(c.anticipated_issue_raw, '')      AS anticipated_issue_date,
            {_to_iso_case('c.anticipated_issue_raw')}  AS anticipated_issue_iso,
            COALESCE(c.permit_expiration_raw, '')      AS permit_expiration_date,
            {_to_iso_case('c.permit_expiration_raw')}  AS permit_expiration_iso,
            c.land_mgmt_comments,
            c.permit_comment
        FROM chosen c;
    """)

    # 4) sap_tracker SP57/RP57
    cur.executescript("""
        DROP TABLE IF EXISTS __land_sap;
        CREATE TEMP TABLE __land_sap AS
        SELECT st."Order", st."SP57" AS sp57, st."RP57" AS rp57
        FROM sap_tracker st
        INNER JOIN __land_orders o ON o."Order" = st."Order";
    """)

    # 5) Base join (carry anticipated_app_iso + WPD/PRY through for action rules)
    cur.executescript("""
        DROP TABLE IF EXISTS __land_base;
        CREATE TEMP TABLE __land_base AS
        SELECT
            o."Order",
            COALESCE(m.notif_status, '') AS notif_status,
            COALESCE(m.sap_status,   '') AS sap_status,

            -- NEW: WPD + PRY carried through
            COALESCE(m.work_plan_date_raw, '') AS work_plan_date_raw,
            m.work_plan_date_iso               AS work_plan_date_iso,
            m.pry_int                          AS pry_int,

            COALESCE(s.sp57, '') AS sp57,
            COALESCE(s.rp57, '') AS rp57,
            ld.permit_type                               AS permit_type,
            COALESCE(ld.permit_status, 'Unknown')        AS permit_status,
            COALESCE(ld.anticipated_app_date, '')        AS anticipated_app_date,
            ld.anticipated_app_iso                       AS anticipated_app_iso,
            COALESCE(ld.anticipated_issue_date, '')      AS anticipated_issue_date,
            ld.anticipated_issue_iso                     AS anticipated_issue_iso,
            COALESCE(ld.permit_expiration_date, '')      AS permit_expiration_date,
            ld.permit_expiration_iso                     AS permit_expiration_iso,
            COALESCE(ld.land_mgmt_comments, '')          AS land_mgmt_comments,
            COALESCE(ld.permit_comment, '')              AS permit_comment
        FROM __land_orders o
        LEFT JOIN __land_mpp m ON m."Order" = o."Order"
        LEFT JOIN __land_sap s ON s."Order" = o."Order"
        LEFT JOIN __land_ld  ld ON ld."Order" = o."Order";
    """)

    # 6) Extract latest comment triplet
    cur.execute('SELECT "Order", land_mgmt_comments, permit_comment FROM __land_base')
    rows = cur.fetchall()

    cur.executescript("""
        DROP TABLE IF EXISTS __land_latest;
        CREATE TEMP TABLE __land_latest (
            "Order" INTEGER PRIMARY KEY,
            latest_date_iso   TEXT,
            latest_date_mdy   TEXT,
            latest_lan_id     TEXT,
            latest_comment    TEXT
        );
    """)

    today = datetime.now().date()
    to_insert = []
    for order, lm, pc in rows:
        lm_iso, lm_mdy, lm_lan, lm_txt = extract_latest_comment_block(lm, today=today)
        pc_iso, pc_mdy, pc_lan, pc_txt = extract_latest_comment_block(pc, today=today)

        choose_pc = False
        if lm_iso and pc_iso:
            choose_pc = (pc_iso > lm_iso)
        elif pc_iso and not lm_iso:
            choose_pc = True
        elif not lm_iso and not pc_iso:
            if (pc_txt and not lm_txt):
                choose_pc = True

        if choose_pc:
            iso, mdy, lan_id, txt = pc_iso, pc_mdy, pc_lan, pc_txt
        else:
            iso, mdy, lan_id, txt = lm_iso, lm_mdy, lm_lan, lm_txt

        if not mdy:
            mdy = "Not enough data"
        if not lan_id:
            lan_id = "Not enough data"
        if not txt:
            txt = "Not enough data"

        to_insert.append((order, iso, mdy, lan_id, txt))

    if to_insert:
        cur.executemany(
            'INSERT OR REPLACE INTO __land_latest ("Order", latest_date_iso, latest_date_mdy, latest_lan_id, latest_comment) VALUES (?, ?, ?, ?, ?)',
            to_insert
        )

    # 7) Action logic (includes anticipated_app_iso rule; three bindings for date(?))
    today_iso = today.isoformat()
    cur.execute("""
        CREATE TEMP TABLE __land_with_action AS
        SELECT
            b."Order",
            b.notif_status,
            b.sap_status,
            b.sp57,
            b.rp57,
            b.permit_type,
            b.permit_status,
            b.anticipated_app_date,
            b.anticipated_app_iso,
            b.anticipated_issue_date,
            b.anticipated_issue_iso,
            b.permit_expiration_date,
            b.permit_expiration_iso,
            b.land_mgmt_comments,
            b.permit_comment,
            l.latest_date_iso,
            l.latest_date_mdy,
            l.latest_lan_id,
            l.latest_comment,
            CASE
                WHEN UPPER(TRIM(COALESCE(b.sp57, ''))) = 'COMP'
                    AND UPPER(TRIM(COALESCE(b.rp57, ''))) = 'COMP'
                    AND (b.permit_type IS NULL OR TRIM(COALESCE(b.permit_type, '')) = '')
                    AND TRIM(COALESCE(b.permit_comment, '')) = ''
                    AND TRIM(COALESCE(b.land_mgmt_comments, '')) = ''
                    THEN 'Permit not required.'

                WHEN b.work_plan_date_iso IS NOT NULL
                    AND b.pry_int IS NOT NULL
                    AND CAST(STRFTIME('%Y', b.work_plan_date_iso) AS INTEGER) > b.pry_int
                    THEN 'WPD ahead of PRY. Please pull in the job.'
                
                WHEN b.permit_type IS NULL
                    AND TRIM(COALESCE(b.permit_comment, '')) = ''
                    AND TRIM(COALESCE(b.land_mgmt_comments, '')) = ''
                    THEN 'No data available. Permit likely not required.'

                WHEN b.permit_status = 'No permit required.'
                    THEN 'Please confirm no permit is required and complete SP/RP57.'

                WHEN b.permit_status = 'Permit obtained.'
                    AND b.permit_expiration_iso IS NOT NULL
                    AND date(b.permit_expiration_iso) >= date(?)
                    THEN 'Please confirm permit is obtained and complete SP/RP57.'

                WHEN b.anticipated_app_iso IS NOT NULL
                    AND date(b.anticipated_app_iso) > date(?)
                    THEN 'Anticipated application date is in the future. No action required.'

                WHEN b.anticipated_issue_iso IS NOT NULL
                    AND date(b.anticipated_issue_iso) > date(?)
                    THEN 'Anticipated issue date is in the future. No action required.'

                WHEN b.permit_status = 'On Hold'
                    THEN 'Job on hold. No action required.'

                ELSE 'check'
            END AS action_text
        FROM __land_base b
        LEFT JOIN __land_latest l ON l."Order" = b."Order"
    """, (today_iso, today_iso, today_iso))

    # 8) Final projection & upsert
    cur.executescript("""
        DROP TABLE IF EXISTS __land_final;
        CREATE TEMP TABLE __land_final AS
        SELECT
            "Order"                               AS "Order",
            notif_status                          AS "Notification Status",
            sap_status                            AS "SAP Status",
            sp57                                  AS "SP57",
            rp57                                  AS "RP57",
            permit_type                           AS "Permit Type",
            permit_status                         AS "Permit Status",
            anticipated_app_date                  AS "Anticipated Application Date",
            anticipated_issue_date                AS "Anticipated Issue Date",
            permit_expiration_date                AS "Permit Expiration Date",
            land_mgmt_comments                    AS "Land Management Comments",
            permit_comment                        AS "Permit Comment",
            COALESCE(latest_date_mdy,'Not enough data') AS "Latest Comment Date",
            COALESCE(latest_lan_id,'Not enough data')   AS "LAN ID",
            COALESCE(latest_comment,'Not enough data')  AS "Latest Comment",
            ''                                    AS "Parsed Action",
            ''                                    AS "Parsed Anticipated Issue Date",
            ''                                    AS "Parsed Permit Expiration Date",
            action_text                           AS "Action"
        FROM __land_with_action;
    """)

    before = conn.total_changes

    # Keep only current pending-set rows
    cur.execute('DELETE FROM land_tracker WHERE "Order" NOT IN (SELECT "Order" FROM __land_final)')

    # Upsert results
    cols_csv = ", ".join(f'"{c}"' for c in LAND_COLS)
    cur.executescript(f"""
        INSERT OR REPLACE INTO land_tracker ({cols_csv})
        SELECT {cols_csv}
        FROM __land_final;
    """)
    conn.commit()

    # 9) Fill parsed columns
    rows2 = cur.execute('SELECT "Order","Latest Comment" FROM land_tracker').fetchall()
    updates = []
    for order_id, latest_comment in rows2:
        p_action, p_anticip, p_exp = parse_comment_semantics(latest_comment or "")
        updates.append((p_action, p_anticip, p_exp, order_id))

    if updates:
        cur.executemany(
            'UPDATE land_tracker SET "Parsed Action" = ?, "Parsed Anticipated Issue Date" = ?, "Parsed Permit Expiration Date" = ? WHERE "Order" = ?',
            updates
        )
        conn.commit()

    # 10) Parsed override
    cur.execute(
        'UPDATE land_tracker SET "Action" = ? WHERE "Parsed Action" = ?',
        ('Review complete. No permit needed.', 'Review complete. No permit needed.')
    )
    conn.commit()

    # 11) Monument-survey override
    # If Parsed Action = "Monument survey complete.":
    #   - and both SP57 & RP57 are COMP  -> "Monument survey complete. No action required."
    #   - otherwise                      -> "check"
    cur.execute(
        'UPDATE land_tracker '
        'SET "Action" = ? '
        'WHERE "Parsed Action" = ?',
        ('check', 'Monument survey complete.')
    )
    cur.execute(
        'UPDATE land_tracker '
        'SET "Action" = ? '
        'WHERE "Parsed Action" = ? '
        '  AND UPPER(TRIM("SP57")) = "COMP" '
        '  AND UPPER(TRIM("RP57")) = "COMP"',
        ('Monument survey complete. No action required.', 'Monument survey complete.')
    )
    conn.commit()

    # 12) Permit-expired rule:
    # If max(Permit Expiration Date, Parsed Permit Expiration Date) is in the past
    # AND Notification Status = OPEN -> Permit expired action
    today_iso = today.isoformat()

    exp1_iso = _to_iso_case_flex('NULLIF(TRIM(COALESCE("Permit Expiration Date","")), "")')
    exp2_iso = _to_iso_case_flex('NULLIF(TRIM(COALESCE("Parsed Permit Expiration Date","")), "")')

    cur.execute(f"""
        UPDATE land_tracker
        SET "Action" = 'Permit expired. Please request for extension.'
        WHERE UPPER(TRIM(COALESCE("Notification Status",""))) = 'OPEN'
        AND (
                {exp1_iso} IS NOT NULL
            OR {exp2_iso} IS NOT NULL
        )
        AND max(
                COALESCE(julianday({exp1_iso}), -1e15),
                COALESCE(julianday({exp2_iso}), -1e15)
            ) < julianday(?);
    """, (today_iso,))
    conn.commit()

    return conn.total_changes - before