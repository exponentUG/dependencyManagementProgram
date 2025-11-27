# helpers/wmp_tracker_builder/open_dependencies/build.py
from __future__ import annotations
import sqlite3
from datetime import datetime

# Use ordered tuple (not a set) so we have deterministic placeholders & bindings
AP_ALLOWED = ("PEND", "UNSC", "CONS")


def _ensure_table_and_columns(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS open_dependencies (
            "Order" INTEGER PRIMARY KEY,
            "Permit" TEXT
        )
    """)
    cur.execute("PRAGMA table_info(open_dependencies)")
    cols = {row[1] for row in cur.fetchall()}
    if "Land" not in cols:
        cur.execute('ALTER TABLE open_dependencies ADD COLUMN "Land" TEXT')
    if "FAA" not in cols:
        cur.execute('ALTER TABLE open_dependencies ADD COLUMN "FAA" TEXT')
    if "Environment" not in cols:
        cur.execute('ALTER TABLE open_dependencies ADD COLUMN "Environment" TEXT')
    if "Joint Pole" not in cols:
        cur.execute('ALTER TABLE open_dependencies ADD COLUMN "Joint Pole" TEXT')
    if "MiscTSK" not in cols:
        cur.execute('ALTER TABLE open_dependencies ADD COLUMN "MiscTSK" TEXT')
    if "Open Dependencies" not in cols:
        cur.execute('ALTER TABLE open_dependencies ADD COLUMN "Open Dependencies" TEXT')
    # NEW: Stage of Job column
    if "Stage of Job" not in cols:
        cur.execute('ALTER TABLE open_dependencies ADD COLUMN "Stage of Job" TEXT')
    conn.commit()


def build_open_dependencies(conn: sqlite3.Connection) -> int:
    _ensure_table_and_columns(conn)
    cur = conn.cursor()

    # Tracked orders
    cur.executescript("""
        DROP TABLE IF EXISTS __od_orders;
        CREATE TEMP TABLE __od_orders AS
        SELECT DISTINCT "Order" AS order_num
        FROM order_tracking_list;
    """)

    # EPW one-per-order + ISO normalize
    cur.executescript("""
        DROP TABLE IF EXISTS __epw_one_per_order;
        CREATE TEMP TABLE __epw_one_per_order AS
        WITH c AS (
            SELECT "Order Number" AS order_num,
                   "EPW Expiration Date" AS epw_raw,
                   rowid AS rid
            FROM epw_data
        )
        SELECT c1.order_num, c1.epw_raw
        FROM c c1
        WHERE c1.rid = (SELECT MIN(c2.rid) FROM c c2 WHERE c2.order_num = c1.order_num);

        DROP TABLE IF EXISTS __epw_norm;
        CREATE TEMP TABLE __epw_norm AS
        SELECT
            o.order_num AS "Order",
            CASE
              WHEN e.epw_raw IS NOT NULL
                   AND LENGTH(e.epw_raw)=10
                   AND SUBSTR(e.epw_raw,3,1)='/' AND SUBSTR(e.epw_raw,6,1)='/'
                THEN SUBSTR(e.epw_raw,7,4) || '-' || SUBSTR(e.epw_raw,1,2) || '-' || SUBSTR(e.epw_raw,4,2)
              ELSE NULL
            END AS epw_iso
        FROM __od_orders o
        LEFT JOIN __epw_one_per_order e ON e.order_num = o.order_num;
    """)

    # MPP one-per-order + ISO normalize (Permit Exp Date from mpp_data)
    cur.executescript("""
        DROP TABLE IF EXISTS __mpp_one_per_order;
        CREATE TEMP TABLE __mpp_one_per_order AS
        WITH c AS (
            SELECT "Order" AS order_num,
                   "Permit Exp Date" AS mpp_raw,
                   rowid AS rid
            FROM mpp_data
        )
        SELECT c1.order_num, c1.mpp_raw
        FROM c c1
        WHERE c1.rid = (SELECT MIN(c2.rid) FROM c c2 WHERE c2.order_num = c1.order_num);

        DROP TABLE IF EXISTS __mpp_norm;
        CREATE TEMP TABLE __mpp_norm AS
        SELECT
            o.order_num AS "Order",
            CASE
              WHEN m.mpp_raw IS NOT NULL
                   AND LENGTH(m.mpp_raw)=10
                   AND SUBSTR(m.mpp_raw,3,1)='/' AND SUBSTR(m.mpp_raw,6,1)='/'
                THEN SUBSTR(m.mpp_raw,7,4) || '-' || SUBSTR(m.mpp_raw,1,2) || '-' || SUBSTR(m.mpp_raw,4,2)
              ELSE NULL
            END AS mpp_iso
        FROM __od_orders o
        LEFT JOIN __mpp_one_per_order m ON m.order_num = o.order_num;
    """)

    # Land one-per-order + ISO normalize
    # When multiple rows per Order in land_data, pick the row with the LATEST "Permit Created Date";
    # if all created dates are NULL/invalid for an Order, fall back to the earliest row (MIN rowid).
    cur.executescript("""
        DROP TABLE IF EXISTS __land_one_per_order;
        CREATE TEMP TABLE __land_one_per_order AS
        WITH c AS (
            SELECT
                "Order"               AS order_num,
                "Permit Expiration"   AS land_raw,
                "Permit Created Date" AS created_raw,
                rowid                 AS rid
            FROM land_data
        ),
        c_norm AS (
            SELECT
                order_num,
                land_raw,
                created_raw,
                CASE
                  WHEN created_raw IS NOT NULL
                       AND LENGTH(created_raw)=10
                       AND SUBSTR(created_raw,3,1)='/' AND SUBSTR(created_raw,6,1)='/'
                    THEN SUBSTR(created_raw,7,4) || '-' || SUBSTR(created_raw,1,2) || '-' || SUBSTR(created_raw,4,2)
                  ELSE NULL
                END AS created_iso,
                rid
            FROM c
        ),
        picked AS (
            SELECT c1.*
            FROM c_norm c1
            WHERE c1.rid = (
                SELECT c2.rid
                FROM c_norm c2
                WHERE c2.order_num = c1.order_num
                ORDER BY (c2.created_iso IS NOT NULL) DESC, c2.created_iso DESC, c2.rid ASC
                LIMIT 1
            )
        )
        SELECT p.order_num, p.land_raw
        FROM picked p;

        DROP TABLE IF EXISTS __land_norm;
        CREATE TEMP TABLE __land_norm AS
        SELECT
            o.order_num AS "Order",
            CASE
              WHEN l.land_raw IS NOT NULL
                   AND LENGTH(l.land_raw)=10
                   AND SUBSTR(l.land_raw,3,1)='/' AND SUBSTR(l.land_raw,6,1)='/'
                THEN SUBSTR(l.land_raw,7,4) || '-' || SUBSTR(l.land_raw,1,2) || '-' || SUBSTR(l.land_raw,4,2)
              ELSE NULL
            END AS land_iso
        FROM __od_orders o
        LEFT JOIN __land_one_per_order l ON l.order_num = o.order_num;
    """)

    today_iso = datetime.now().date().isoformat()
    placeholders = ", ".join(["?"] * len(AP_ALLOWED))  # "?, ?, ?"

    # Build params list in the exact order the placeholders appear in the SQL
    params = []
    # Permit gate: ps NOT IN (?,?,?), then date(?)
    params.extend(AP_ALLOWED)      # 3
    params.append(today_iso)       # +1 = 4
    # Land gate: ps NOT IN (?,?,?), then date(?)
    params.extend(AP_ALLOWED)      # +3 = 7
    params.append(today_iso)       # +1 = 8
    # FAA gate: ps NOT IN (?,?,?)
    params.extend(AP_ALLOWED)      # +3 = 11
    # Environment gate: ps NOT IN (?,?,?)
    params.extend(AP_ALLOWED)      # +3 = 14
    # Joint Pole gate: ps NOT IN (?,?,?)
    params.extend(AP_ALLOWED)      # +3 = 17  # matches total placeholders

    # NEW: make Land=Closed if land_tracker says "Review complete. No permit needed." (or "Permit not required.")
    land_tracker_exists = bool(
        cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='land_tracker'").fetchone()
    )

    cur.execute("DROP TABLE IF EXISTS __open_dep_final")
    sql_open_dep = f"""
        CREATE TEMP TABLE __open_dep_final AS
        WITH src AS (
            SELECT
                o.order_num AS "Order",
                UPPER(TRIM(COALESCE(st."Primary Status",''))) AS ps,
                UPPER(TRIM(COALESCE(st."SP56",'')))           AS sp56,
                UPPER(TRIM(COALESCE(st."RP56",'')))           AS rp56,
                UPPER(TRIM(COALESCE(st."SP57",'')))           AS sp57,
                UPPER(TRIM(COALESCE(st."RP57",'')))           AS rp57,
                UPPER(TRIM(COALESCE(st."PC24",'')))           AS pc24,
                UPPER(TRIM(COALESCE(st."DS76",'')))           AS ds76,
                UPPER(TRIM(COALESCE(st."PC21",'')))           AS pc21,
                UPPER(TRIM(COALESCE(st."PC20",'')))           AS pc20,
                UPPER(TRIM(COALESCE(st."AP10",'')))           AS ap10,
                UPPER(TRIM(COALESCE(st."AP25",'')))           AS ap25,
                UPPER(TRIM(COALESCE(st."DS28",'')))           AS ds28,
                UPPER(TRIM(COALESCE(st."DS73",'')))           AS ds73,
                e.epw_iso  AS epw_iso,
                mp.mpp_iso AS mpp_iso,
                CASE
                  WHEN e.epw_iso IS NOT NULL AND mp.mpp_iso IS NOT NULL
                    THEN CASE WHEN e.epw_iso >= mp.mpp_iso THEN e.epw_iso ELSE mp.mpp_iso END
                  ELSE COALESCE(e.epw_iso, mp.mpp_iso)
                END         AS permit_exp_iso,
                ln.land_iso AS land_iso
                {" , lt.\"Action\" AS lt_action" if land_tracker_exists else " , NULL AS lt_action"}
            FROM __od_orders o
            LEFT JOIN sap_tracker st ON st."Order" = o.order_num
            LEFT JOIN __epw_norm e   ON e."Order"   = o.order_num
            LEFT JOIN __mpp_norm mp  ON mp."Order"  = o.order_num
            LEFT JOIN __land_norm ln ON ln."Order"  = o.order_num
            { "LEFT JOIN land_tracker lt ON lt.\"Order\" = o.order_num" if land_tracker_exists else "" }
        )
        SELECT
            "Order",
            -- Permit (unchanged)
            CASE
              WHEN ps NOT IN ({placeholders})
                THEN 'Closed'
              ELSE
                CASE
                  WHEN sp56 IN ('INPR','ACTD')
                       OR rp56 IN ('INPR','INPT','ACTD')
                       OR (rp56='COMP' AND (permit_exp_iso IS NULL OR date(permit_exp_iso) < date(?)))
                    THEN 'Pending'
                  ELSE 'Closed'
                END
            END AS "Permit",

            -- Land (short-circuit to Closed if lt_action says review complete/no permit)
            CASE
              WHEN ps NOT IN ({placeholders})
                THEN 'Closed'
              ELSE
                CASE
                  WHEN { "TRIM(lt_action)='Review complete. No permit needed.'" if land_tracker_exists else "0" }
                    THEN 'Closed'
                  WHEN { "TRIM(lt_action)='Permit not required.'" if land_tracker_exists else "0" }
                    THEN 'Closed'
                  WHEN sp57 IN ('INPR','ACTD')
                       OR rp57 IN ('INPR','INPT','ACTD')
                       OR (rp57='COMP' AND (land_iso IS NULL OR date(land_iso) < date(?)))
                    THEN 'Pending'
                  ELSE 'Closed'
                END
            END AS "Land",

            -- FAA (unchanged)
            CASE
              WHEN ps NOT IN ({placeholders})
                THEN 'Closed'
              ELSE
                CASE
                  WHEN pc24='INPR' OR ds76='INPR'
                    THEN 'Pending'
                  ELSE 'Closed'
                END
            END AS "FAA",

            -- Environment (unchanged)
            CASE
              WHEN ps NOT IN ({placeholders})
                THEN 'Closed'
              ELSE
                CASE
                  WHEN pc21='INPR' THEN 'Pending'
                  ELSE 'Closed'
                END
            END AS "Environment",

            -- Joint Pole (unchanged)
            CASE
              WHEN ps NOT IN ({placeholders})
                THEN 'Closed'
              ELSE
                CASE
                  WHEN pc20='INPR' THEN 'Pending'
                  ELSE 'Closed'
                END
            END AS "Joint Pole",

            -- MiscTSK (unchanged)
            CASE
              WHEN ps <> 'PEND'
                THEN 'Closed'
              ELSE
                CASE
                  WHEN 'CLEAR TASK' IN (ap10, ap25, ds28, ds73)
                    THEN 'Pending'
                  ELSE 'Closed'
                END
            END AS "MiscTSK"
        FROM src
    """
    cur.execute(sql_open_dep, params)

    before = conn.total_changes

    # Upsert gate columns into open_dependencies
    cur.executescript("""
        INSERT OR REPLACE INTO open_dependencies
            ("Order","Permit","Land","FAA","Environment","Joint Pole","MiscTSK")
        SELECT "Order","Permit","Land","FAA","Environment","Joint Pole","MiscTSK"
        FROM __open_dep_final;
    """)

    # Compute "Open Dependencies" from the six gate columns
    cur.execute("""
        UPDATE open_dependencies
        SET "Open Dependencies" = CASE
          WHEN (
            (CASE WHEN "Permit"='Pending' THEN ' Permit' ELSE '' END) ||
            (CASE WHEN "Land"='Pending' THEN ' Land' ELSE '' END) ||
            (CASE WHEN "FAA"='Pending' THEN ' FAA' ELSE '' END) ||
            (CASE WHEN "Environment"='Pending' THEN ' Environment' ELSE '' END) ||
            (CASE WHEN "Joint Pole"='Pending' THEN ' Joint Pole' ELSE '' END) ||
            (CASE WHEN "MiscTSK"='Pending' THEN ' MiscTSK' ELSE '' END)
          ) = '' THEN 'None'
          ELSE TRIM(SUBSTR(
            (CASE WHEN "Permit"='Pending' THEN ' Permit' ELSE '' END) ||
            (CASE WHEN "Land"='Pending' THEN ' Land' ELSE '' END) ||
            (CASE WHEN "FAA"='Pending' THEN ' FAA' ELSE '' END) ||
            (CASE WHEN "Environment"='Pending' THEN ' Environment' ELSE '' END) ||
            (CASE WHEN "Joint Pole"='Pending' THEN ' Joint Pole' ELSE '' END) ||
            (CASE WHEN "MiscTSK"='Pending' THEN ' MiscTSK' ELSE '' END),
          2))
        END
    """)

    # ---------- NEW: compute "Stage of Job" ----------
    #
    # Rules (using Primary Status (ps), Notif Status (ns), Open Dependencies (od_open)):
    # 1) ps IN (UNSC, CONS) AND od_open = 'None'                -> "Ready for Construction."
    # 2) ps IN (UNSC, CONS) AND od_open <> 'None'               -> "Dependencies not met. Return to PEND."
    # 3) ps = PEND         AND od_open = 'None'                 -> "Dependencies met."
    # 4) ps = PEND         AND od_open <> 'None'                -> "Pending dependencies."
    # 5) ps IN (CNCL, ESTS, DEFR, APPR) AND ns = 'CNCL'         -> "Cancelled/Deferred."
    # 6) ps IN (UNSE, ESTS, ADER, APPR)                         -> "Estimating."
    # 7) ps IN ('DCNL','PROD','DOCC','FICL','CLSD','MAPP')      -> "Construction Complete."
    # 8) otherwise                                              -> "Unknown stage of job."
    #
    # Note: we check "Cancelled/Deferred" BEFORE "Estimating" so a CNCL notification wins.
    cur.execute("""
        UPDATE open_dependencies AS od
        SET "Stage of Job" = COALESCE(
            (
                SELECT CASE 
                    WHEN ps IN ('CNCL','ESTS','DEFR','APPR') AND ns = 'CNCL'
                        THEN 'Cancelled/Deferred.'
                    WHEN ps IN ('UNSC','CONS') AND od_open = 'None'
                        THEN 'Ready for Construction.'
                    WHEN ps IN ('UNSC','CONS') AND od_open <> 'None'
                        THEN 'Dependencies not met. Return to PEND.'
                    WHEN ps = 'PEND' AND od_open = 'None'
                        THEN 'Dependencies met.'
                    WHEN ps = 'PEND' AND od_open <> 'None'
                        THEN 'Pending dependencies.'
                    WHEN ps IN ('UNSE','ESTS','ADER','APPR')
                        THEN 'Estimating.'
                    WHEN ps IN ('DCNL','PROD','DOCC','FICL','CLSD','MAPP')
                        THEN 'Construction Complete.'
                    ELSE 'Unknown stage of job.'
                END
                FROM (
                    SELECT
                        UPPER(TRIM(COALESCE(st."Primary Status", ''))) AS ps,
                        UPPER(TRIM(COALESCE(m."Notif Status",  '')))  AS ns,
                        COALESCE(od."Open Dependencies", '')          AS od_open
                    FROM sap_tracker st
                    LEFT JOIN mpp_data m
                        ON m."Order" = st."Order"
                    WHERE st."Order" = od."Order"
                    LIMIT 1
                ) AS sub
            ),
            'Unknown stage of job.'
        )
    """)

    conn.commit()
    return conn.total_changes - before
