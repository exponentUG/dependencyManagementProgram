# helpers/wmp_tracker_builder/sap_tracker/pivot.py
from __future__ import annotations
import sqlite3

PENDING_STATUSES = {"ESTS", "UNSE", "ADER", "APPR"}  # case-insensitive
AP_ALLOWED_STATUSES = {"PEND", "UNSC", "CONS"}       # case-insensitive


def update_codes_batch(conn: sqlite3.Connection) -> int:
    cur = conn.cursor()

    # Index to speed up sap_data lookups
    cur.execute('CREATE INDEX IF NOT EXISTS idx_sap_data_order_code ON sap_data("Order","Code")')

    # --- PIVOT: include PC21 in the code set
    cur.executescript("""
        DROP TABLE IF EXISTS __codes_pivot;
        CREATE TEMP TABLE __codes_pivot AS
        WITH target_orders AS (
            SELECT DISTINCT "Order" AS order_num FROM order_tracking_list
        ),
        filtered AS (
            SELECT s."Order" AS order_num, s."Code",
                   NULLIF(s."TaskUsrStatus",'') AS tus
            FROM sap_data s
            INNER JOIN target_orders t ON t.order_num = s."Order"
            WHERE s."Code" IN (
                'SP56','RP56','SP57','RP57',
                'DS42','PC20','PC21','DS76','PC24','DS11',
                'AP10','AP25','DS28','DS73'
            )
        )
        SELECT
            t.order_num AS "Order",
            -- SP56
            MAX(CASE WHEN Code='SP56' THEN COALESCE(tus, NULL) END) AS tus_sp56,
            MAX(CASE WHEN Code='SP56' THEN 1 ELSE 0 END) AS has_sp56,
            -- RP56
            MAX(CASE WHEN Code='RP56' THEN COALESCE(tus, NULL) END) AS tus_rp56,
            MAX(CASE WHEN Code='RP56' THEN 1 ELSE 0 END) AS has_rp56,
            -- SP57
            MAX(CASE WHEN Code='SP57' THEN COALESCE(tus, NULL) END) AS tus_sp57,
            MAX(CASE WHEN Code='SP57' THEN 1 ELSE 0 END) AS has_sp57,
            -- RP57
            MAX(CASE WHEN Code='RP57' THEN COALESCE(tus, NULL) END) AS tus_rp57,
            MAX(CASE WHEN Code='RP57' THEN 1 ELSE 0 END) AS has_rp57,
            -- DS42
            MAX(CASE WHEN Code='DS42' THEN COALESCE(tus, NULL) END) AS tus_ds42,
            MAX(CASE WHEN Code='DS42' THEN 1 ELSE 0 END) AS has_ds42,
            -- PC20
            MAX(CASE WHEN Code='PC20' THEN COALESCE(tus, NULL) END) AS tus_pc20,
            MAX(CASE WHEN Code='PC20' THEN 1 ELSE 0 END) AS has_pc20,
            -- PC21   <-- NEW
            MAX(CASE WHEN Code='PC21' THEN COALESCE(tus, NULL) END) AS tus_pc21,
            MAX(CASE WHEN Code='PC21' THEN 1 ELSE 0 END) AS has_pc21,
            -- DS76
            MAX(CASE WHEN Code='DS76' THEN COALESCE(tus, NULL) END) AS tus_ds76,
            MAX(CASE WHEN Code='DS76' THEN 1 ELSE 0 END) AS has_ds76,
            -- PC24
            MAX(CASE WHEN Code='PC24' THEN COALESCE(tus, NULL) END) AS tus_pc24,
            MAX(CASE WHEN Code='PC24' THEN 1 ELSE 0 END) AS has_pc24,
            -- DS11
            MAX(CASE WHEN Code='DS11' THEN COALESCE(tus, NULL) END) AS tus_ds11,
            MAX(CASE WHEN Code='DS11' THEN 1 ELSE 0 END) AS has_ds11,
            -- AP10
            MAX(CASE WHEN Code='AP10' THEN COALESCE(tus, NULL) END) AS tus_ap10,
            MAX(CASE WHEN Code='AP10' THEN 1 ELSE 0 END) AS has_ap10,
            -- AP25
            MAX(CASE WHEN Code='AP25' THEN COALESCE(tus, NULL) END) AS tus_ap25,
            MAX(CASE WHEN Code='AP25' THEN 1 ELSE 0 END) AS has_ap25,
            -- DS28
            MAX(CASE WHEN Code='DS28' THEN COALESCE(tus, NULL) END) AS tus_ds28,
            MAX(CASE WHEN Code='DS28' THEN 1 ELSE 0 END) AS has_ds28,
            -- DS73
            MAX(CASE WHEN Code='DS73' THEN COALESCE(tus, NULL) END) AS tus_ds73,
            MAX(CASE WHEN Code='DS73' THEN 1 ELSE 0 END) AS has_ds73
        FROM target_orders t
        LEFT JOIN filtered f ON f.order_num = t.order_num
        GROUP BY t.order_num
    """)
    conn.commit()

    pe_placeholders = ",".join("?" for _ in PENDING_STATUSES)
    ap_placeholders = ",".join("?" for _ in AP_ALLOWED_STATUSES)

    # we cannot say NOTN anymore for the leaps tasks anymore. we need to say unknown there. and that is for non-estimated jobs.

    cur.execute("DROP TABLE IF EXISTS __codes_final")
    cur.execute(f"""
        CREATE TEMP TABLE __codes_final AS
        SELECT
            p."Order",
            -- SP56
            CASE WHEN UPPER(COALESCE(st."Primary Status",'')) IN ({pe_placeholders}) THEN 'Pending Estimation'
                 WHEN COALESCE(p.has_sp56,0)=1 THEN COALESCE(p.tus_sp56,'ACTD') ELSE 'NOTN' END AS SP56,    
            -- RP56
            CASE WHEN UPPER(COALESCE(st."Primary Status",'')) IN ({pe_placeholders}) THEN 'Pending Estimation'
                 WHEN COALESCE(p.has_rp56,0)=1 THEN COALESCE(p.tus_rp56,'ACTD') ELSE 'NOTN' END AS RP56,
            -- SP57
            CASE WHEN UPPER(COALESCE(st."Primary Status",'')) IN ({pe_placeholders}) THEN 'Pending Estimation'
                 WHEN COALESCE(p.has_sp57,0)=1 THEN COALESCE(p.tus_sp57,'ACTD') ELSE 'NOTN' END AS SP57,
            -- RP57
            CASE WHEN UPPER(COALESCE(st."Primary Status",'')) IN ({pe_placeholders}) THEN 'Pending Estimation'
                 WHEN COALESCE(p.has_rp57,0)=1 THEN COALESCE(p.tus_rp57,'ACTD') ELSE 'NOTN' END AS RP57,
            -- DS42
            CASE WHEN UPPER(COALESCE(st."Primary Status",'')) IN ({pe_placeholders}) THEN 'Pending Estimation'
                 WHEN COALESCE(p.has_ds42,0)=1 THEN COALESCE(p.tus_ds42,'INPR') ELSE 'NOTN' END AS DS42,
            -- PC20
            CASE WHEN UPPER(COALESCE(st."Primary Status",'')) IN ({pe_placeholders}) THEN 'Pending Estimation'
                 WHEN COALESCE(p.has_pc20,0)=1 THEN COALESCE(p.tus_pc20,'INPR') ELSE 'NOTN' END AS PC20,
            -- DS76
            CASE WHEN UPPER(COALESCE(st."Primary Status",'')) IN ({pe_placeholders}) THEN 'Pending Estimation'
                 WHEN COALESCE(p.has_ds76,0)=1 THEN COALESCE(p.tus_ds76,'INPR') ELSE 'NOTN' END AS DS76,
            -- PC24
            CASE WHEN UPPER(COALESCE(st."Primary Status",'')) IN ({pe_placeholders}) THEN 'Pending Estimation'
                 WHEN COALESCE(p.has_pc24,0)=1 THEN COALESCE(p.tus_pc24,'INPR') ELSE 'NOTN' END AS PC24,
            -- DS11
            CASE WHEN UPPER(COALESCE(st."Primary Status",'')) IN ({pe_placeholders}) THEN 'Pending Estimation'
                 WHEN COALESCE(p.has_ds11,0)=1 THEN COALESCE(p.tus_ds11,'INPR') ELSE 'NOTN' END AS DS11,
            -- PC21
            CASE WHEN UPPER(COALESCE(st."Primary Status",'')) IN ({pe_placeholders}) THEN 'Pending Estimation'
                 WHEN COALESCE(p.has_pc21,0)=1 THEN COALESCE(p.tus_pc21,'INPR') ELSE 'NOTN' END AS PC21,
            -- AP10 (gated)
            CASE WHEN UPPER(COALESCE(st."Primary Status",'')) IN ({ap_placeholders})
                    THEN CASE WHEN COALESCE(p.has_ap10,0)=1
                               THEN CASE WHEN UPPER(COALESCE(p.tus_ap10,''))='INPR' THEN 'Clear Task' ELSE '-' END
                              ELSE '-' END
                 ELSE '-' END AS AP10,
            -- AP25 (gated)
            CASE WHEN UPPER(COALESCE(st."Primary Status",'')) IN ({ap_placeholders})
                    THEN CASE WHEN COALESCE(p.has_ap25,0)=1
                               THEN CASE WHEN UPPER(COALESCE(p.tus_ap25,''))='INPR' THEN 'Clear Task' ELSE '-' END
                              ELSE '-' END
                 ELSE '-' END AS AP25,
            -- DS28 (gated)
            CASE WHEN UPPER(COALESCE(st."Primary Status",'')) IN ({ap_placeholders})
                    THEN CASE WHEN COALESCE(p.has_ds28,0)=1
                               THEN CASE WHEN UPPER(COALESCE(p.tus_ds28,''))='INPR' THEN 'Clear Task' ELSE '-' END
                              ELSE '-' END
                 ELSE '-' END AS DS28,
            -- DS73 (gated)
            CASE WHEN UPPER(COALESCE(st."Primary Status",'')) IN ({ap_placeholders})
                    THEN CASE WHEN COALESCE(p.has_ds73,0)=1
                               THEN CASE WHEN UPPER(COALESCE(p.tus_ds73,''))='INPR' THEN 'Clear Task' ELSE '-' END
                              ELSE '-' END
                 ELSE '-' END AS DS73
        FROM __codes_pivot p
        LEFT JOIN sap_tracker st ON st."Order" = p."Order"
    """, tuple(PENDING_STATUSES) * 10 + tuple(AP_ALLOWED_STATUSES) * 4)
    conn.commit()

    # Index temp + target tables on Order so the final UPDATE is cheaper
    cur.execute('CREATE INDEX IF NOT EXISTS idx_codes_final_order ON __codes_final("Order")')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_sap_tracker_order ON sap_tracker("Order")')
    conn.commit()

    # -------------------------
    # Single-pass UPDATE
    # -------------------------
    before = conn.total_changes

    # One UPDATE instead of 14 separate ones; only touch rows that have a match
    cur.executescript("""
        UPDATE sap_tracker
        SET
            "SP56" = COALESCE(
                (SELECT f.SP56 FROM __codes_final f WHERE f."Order" = sap_tracker."Order"),
                "SP56"
            ),
            "RP56" = COALESCE(
                (SELECT f.RP56 FROM __codes_final f WHERE f."Order" = sap_tracker."Order"),
                "RP56"
            ),
            "SP57" = COALESCE(
                (SELECT f.SP57 FROM __codes_final f WHERE f."Order" = sap_tracker."Order"),
                "SP57"
            ),
            "RP57" = COALESCE(
                (SELECT f.RP57 FROM __codes_final f WHERE f."Order" = sap_tracker."Order"),
                "RP57"
            ),
            "DS42" = COALESCE(
                (SELECT f.DS42 FROM __codes_final f WHERE f."Order" = sap_tracker."Order"),
                "DS42"
            ),
            "PC20" = COALESCE(
                (SELECT f.PC20 FROM __codes_final f WHERE f."Order" = sap_tracker."Order"),
                "PC20"
            ),
            "PC21" = COALESCE(
                (SELECT f.PC21 FROM __codes_final f WHERE f."Order" = sap_tracker."Order"),
                "PC21"
            ),
            "DS76" = COALESCE(
                (SELECT f.DS76 FROM __codes_final f WHERE f."Order" = sap_tracker."Order"),
                "DS76"
            ),
            "PC24" = COALESCE(
                (SELECT f.PC24 FROM __codes_final f WHERE f."Order" = sap_tracker."Order"),
                "PC24"
            ),
            "DS11" = COALESCE(
                (SELECT f.DS11 FROM __codes_final f WHERE f."Order" = sap_tracker."Order"),
                "DS11"
            ),
            "AP10" = COALESCE(
                (SELECT f.AP10 FROM __codes_final f WHERE f."Order" = sap_tracker."Order"),
                "AP10"
            ),
            "AP25" = COALESCE(
                (SELECT f.AP25 FROM __codes_final f WHERE f."Order" = sap_tracker."Order"),
                "AP25"
            ),
            "DS28" = COALESCE(
                (SELECT f.DS28 FROM __codes_final f WHERE f."Order" = sap_tracker."Order"),
                "DS28"
            ),
            "DS73" = COALESCE(
                (SELECT f.DS73 FROM __codes_final f WHERE f."Order" = sap_tracker."Order"),
                "DS73"
            )
        WHERE EXISTS (
            SELECT 1
            FROM __codes_final f
            WHERE f."Order" = sap_tracker."Order"
        );
    """)

    conn.commit()
    return conn.total_changes - before
