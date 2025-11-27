# helpers/wmp_tracker_builder/dependency_trackers/misctsk.py
from __future__ import annotations
import sqlite3

# Desired final column order for miscTSK_tracker
MTSK_COLS = [
    "Order",
    "Notification Status",
    "SAP Status",
    "AP10",
    "AP25",
    "DS28",
    "DS73",
]

def _ensure_table(conn: sqlite3.Connection) -> None:
    """
    Ensure miscTSK_tracker exists with the desired schema.
    If it exists but differs, migrate in-place while preserving data.
    """
    cur = conn.cursor()

    # Create if missing with full schema
    cur.execute("""
        CREATE TABLE IF NOT EXISTS miscTSK_tracker (
            "Order" INTEGER PRIMARY KEY,
            "Notification Status" TEXT,
            "SAP Status" TEXT,
            "AP10" TEXT,
            "AP25" TEXT,
            "DS28" TEXT,
            "DS73" TEXT
        )
    """)
    conn.commit()

    # Check actual columns
    cur.execute("PRAGMA table_info(miscTSK_tracker)")
    existing_cols = [row[1] for row in cur.fetchall()]

    if existing_cols == MTSK_COLS:
        return

    # Migrate to desired order (add any missing columns)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS miscTSK_tracker__new (
            "Order" INTEGER PRIMARY KEY,
            "Notification Status" TEXT,
            "SAP Status" TEXT,
            "AP10" TEXT,
            "AP25" TEXT,
            "DS28" TEXT,
            "DS73" TEXT
        )
    """)

    # Map existing columns, else NULL
    select_parts = []
    for c in MTSK_COLS:
        if c in existing_cols:
            select_parts.append(f'"{c}"')
        else:
            select_parts.append(f'NULL AS "{c}"')
    select_sql = ", ".join(select_parts)
    cols_csv = ", ".join(f'"{c}"' for c in MTSK_COLS)

    cur.execute((
        f'INSERT OR REPLACE INTO miscTSK_tracker__new ({cols_csv}) '
        f'SELECT {select_sql} FROM miscTSK_tracker'
    ))
    cur.execute("DROP TABLE miscTSK_tracker")
    cur.execute("ALTER TABLE miscTSK_tracker__new RENAME TO miscTSK_tracker")
    conn.commit()


def build_misctsk_tracker(conn: sqlite3.Connection) -> int:
    """
    Build/refresh miscTSK_tracker:
      - Orders from open_dependencies where MiscTSK='Pending'
      - Notification Status, SAP Status from mpp_data
      - AP10, AP25, DS28, DS73 from sap_tracker
    Returns affected rows (inserted/updated).
    """
    _ensure_table(conn)
    cur = conn.cursor()

    # 1) Orders with MiscTSK pending
    cur.executescript("""
        DROP TABLE IF EXISTS __mt_orders;
        CREATE TEMP TABLE __mt_orders AS
        SELECT od."Order"
        FROM open_dependencies od
        WHERE od."MiscTSK" = 'Pending';
    """)

    # 2) Pull mpp_data (Notification/SAP Status) in one pass
    cur.executescript("""
        DROP TABLE IF EXISTS __mt_mpp;
        CREATE TEMP TABLE __mt_mpp AS
        SELECT
            m."Order",
            m."Notif Status"   AS notif_status,
            m."Primary Status" AS sap_status
        FROM mpp_data m
        INNER JOIN __mt_orders o ON o."Order" = m."Order";
    """)

    # 3) Pull SAP codes in one pass
    cur.executescript("""
        DROP TABLE IF EXISTS __mt_sap;
        CREATE TEMP TABLE __mt_sap AS
        SELECT
            st."Order",
            st."AP10" AS ap10,
            st."AP25" AS ap25,
            st."DS28" AS ds28,
            st."DS73" AS ds73
        FROM sap_tracker st
        INNER JOIN __mt_orders o ON o."Order" = st."Order";
    """)

    # 4) Combine (LEFT JOIN to keep every order even if some data missing)
    cur.executescript("""
        DROP TABLE IF EXISTS __mt_final;
        CREATE TEMP TABLE __mt_final AS
        SELECT
            o."Order",
            COALESCE(m.notif_status, '') AS "Notification Status",
            COALESCE(m.sap_status,   '') AS "SAP Status",
            COALESCE(s.ap10, '') AS "AP10",
            COALESCE(s.ap25, '') AS "AP25",
            COALESCE(s.ds28, '') AS "DS28",
            COALESCE(s.ds73, '') AS "DS73"
        FROM __mt_orders o
        LEFT JOIN __mt_mpp m ON m."Order" = o."Order"
        LEFT JOIN __mt_sap s ON s."Order" = o."Order";
    """)

    # 5) Upsert into final table
    before = conn.total_changes
    cols_csv = ", ".join(f'"{c}"' for c in MTSK_COLS)

    # NEW: remove any rows no longer in the current pending set
    cur.execute('DELETE FROM miscTSK_tracker WHERE "Order" NOT IN (SELECT "Order" FROM __mt_final)')

    cur.executescript(f"""
        INSERT OR REPLACE INTO miscTSK_tracker ({cols_csv})
        SELECT {cols_csv}
        FROM __mt_final;
    """)
    conn.commit()
    return conn.total_changes - before

