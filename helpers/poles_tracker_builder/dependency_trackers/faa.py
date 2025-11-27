# helpers/wmp_tracker_builder/dependency_trackers/faa.py
from __future__ import annotations
import sqlite3

# Desired final column order for faa_tracker
FAA_COLS = [
    "Order",
    "Notification Status",
    "SAP Status",
    "DS76",
    "PC24",
]

def _ensure_table(conn: sqlite3.Connection) -> None:
    """
    Ensure faa_tracker exists with the desired schema.
    If it exists but differs, migrate in-place while preserving data.
    """
    cur = conn.cursor()

    # Create if missing with full schema
    cur.execute("""
        CREATE TABLE IF NOT EXISTS faa_tracker (
            "Order" INTEGER PRIMARY KEY,
            "Notification Status" TEXT,
            "SAP Status" TEXT,
            "DS76" TEXT,
            "PC24" TEXT
        )
    """)
    conn.commit()

    # Check actual columns
    cur.execute("PRAGMA table_info(faa_tracker)")
    existing_cols = [row[1] for row in cur.fetchall()]

    if existing_cols == FAA_COLS:
        return

    # Migrate to desired order (add any missing columns)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS faa_tracker__new (
            "Order" INTEGER PRIMARY KEY,
            "Notification Status" TEXT,
            "SAP Status" TEXT,
            "DS76" TEXT,
            "PC24" TEXT
        )
    """)

    # Map existing columns, else NULL
    select_parts = []
    for c in FAA_COLS:
        if c in existing_cols:
            select_parts.append(f'"{c}"')
        else:
            select_parts.append(f'NULL AS "{c}"')
    select_sql = ", ".join(select_parts)
    cols_csv = ", ".join(f'"{c}"' for c in FAA_COLS)

    cur.execute((
        f'INSERT OR REPLACE INTO faa_tracker__new ({cols_csv}) '
        f'SELECT {select_sql} FROM faa_tracker'
    ))
    cur.execute("DROP TABLE faa_tracker")
    cur.execute("ALTER TABLE faa_tracker__new RENAME TO faa_tracker")
    conn.commit()


def build_faa_tracker(conn: sqlite3.Connection) -> int:
    """
    Build/refresh faa_tracker:
      - Orders from open_dependencies where FAA='Pending'
      - Notification Status, SAP Status from mpp_data
      - DS76, PC24 from sap_tracker
    Returns affected rows (inserted/updated).
    """
    _ensure_table(conn)
    cur = conn.cursor()

    # 1) Orders with FAA pending
    cur.executescript("""
        DROP TABLE IF EXISTS __faa_orders;
        CREATE TEMP TABLE __faa_orders AS
        SELECT od."Order"
        FROM open_dependencies od
        WHERE od."FAA" = 'Pending';
    """)

    # 2) Pull mpp_data (Notification/SAP Status) in one pass
    cur.executescript("""
        DROP TABLE IF EXISTS __faa_mpp;
        CREATE TEMP TABLE __faa_mpp AS
        SELECT
            m."Order",
            m."Notif Status"   AS notif_status,
            m."Primary Status" AS sap_status
        FROM mpp_data m
        INNER JOIN __faa_orders o ON o."Order" = m."Order";
    """)

    # 3) Pull DS76/PC24 from sap_tracker in one pass
    cur.executescript("""
        DROP TABLE IF EXISTS __faa_sap;
        CREATE TEMP TABLE __faa_sap AS
        SELECT
            st."Order",
            st."DS76" AS ds76,
            st."PC24" AS pc24
        FROM sap_tracker st
        INNER JOIN __faa_orders o ON o."Order" = st."Order";
    """)

    # 4) Combine (LEFT JOIN to keep every order even if some data missing)
    cur.executescript("""
        DROP TABLE IF EXISTS __faa_final;
        CREATE TEMP TABLE __faa_final AS
        SELECT
            o."Order",
            COALESCE(m.notif_status, '') AS "Notification Status",
            COALESCE(m.sap_status,   '') AS "SAP Status",
            COALESCE(s.ds76, '') AS "DS76",
            COALESCE(s.pc24, '') AS "PC24"
        FROM __faa_orders o
        LEFT JOIN __faa_mpp m ON m."Order" = o."Order"
        LEFT JOIN __faa_sap s ON s."Order" = o."Order";
    """)

    # 5) Upsert into final table (but first drop any no-longer-pending)
    before = conn.total_changes
    cols_csv = ", ".join(f'"{c}"' for c in FAA_COLS)

    # NEW: keep only currently Pending FAA orders
    cur.execute('DELETE FROM faa_tracker WHERE "Order" NOT IN (SELECT "Order" FROM __faa_final)')

    cur.executescript(f"""
        INSERT OR REPLACE INTO faa_tracker ({cols_csv})
        SELECT {cols_csv}
        FROM __faa_final;
    """)
    conn.commit()
    return conn.total_changes - before

