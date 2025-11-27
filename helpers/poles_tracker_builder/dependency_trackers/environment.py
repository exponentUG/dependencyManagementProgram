# helpers/wmp_tracker_builder/dependency_trackers/environment.py
from __future__ import annotations
import sqlite3
import re
from datetime import date

# Desired final column order for environment_tracker
ENV_COLS = [
    "Order",
    "Notification Status",
    "SAP Status",
    "DS11",
    "PC21",
    "Environment Status",                 # from epw_data."Env Status"
    "Environment Update",                 # from epw_data."Enviro Update"
    "Environment Anticipated Out Date",   # from manual_tracker
    "Environment Notes",                  # from manual_tracker
    "Action",                             # NEW
]

# --- simple date parser for M/D/YYYY, MM/DD/YYYY, M-D-YYYY, or YYYY-MM-DD -> ISO YYYY-MM-DD
_rx_mdY = re.compile(r"^\s*(\d{1,2})[/-](\d{1,2})[/-](\d{4})\s*$")
_rx_Ymd = re.compile(r"^\s*(\d{4})-(\d{1,2})-(\d{1,2})\s*$")

def _to_iso(mm: int, dd: int, yyyy: int) -> str | None:
    try:
        return date(yyyy, mm, dd).isoformat()
    except Exception:
        return None

def _parse_to_iso(text: str | None) -> str | None:
    if not text:
        return None
    t = str(text).strip()
    if not t:
        return None
    m = _rx_mdY.match(t)
    if m:
        mm, dd, yyyy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return _to_iso(mm, dd, yyyy)
    m = _rx_Ymd.match(t)
    if m:
        yyyy, mm, dd = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return _to_iso(mm, dd, yyyy)
    return None

def _ensure_table(conn: sqlite3.Connection) -> None:
    """
    Ensure environment_tracker exists with the desired schema.
    If it exists but differs, migrate in place while preserving data.
    """
    cur = conn.cursor()

    # Create if missing with the full desired schema/order
    cur.execute("""
        CREATE TABLE IF NOT EXISTS environment_tracker (
            "Order" INTEGER PRIMARY KEY,
            "Notification Status" TEXT,
            "SAP Status" TEXT,
            "DS11" TEXT,
            "PC21" TEXT,
            "Environment Status" TEXT,
            "Environment Update" TEXT,
            "Environment Anticipated Out Date" TEXT,
            "Environment Notes" TEXT,
            "Action" TEXT
        )
    """)
    conn.commit()

    # Check actual columns
    cur.execute("PRAGMA table_info(environment_tracker)")
    existing_cols = [row[1] for row in cur.fetchall()]

    # If the layout already matches, nothing to do
    if existing_cols == ENV_COLS:
        return

    # Migrate to desired order (add any missing columns)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS environment_tracker__new (
            "Order" INTEGER PRIMARY KEY,
            "Notification Status" TEXT,
            "SAP Status" TEXT,
            "DS11" TEXT,
            "PC21" TEXT,
            "Environment Status" TEXT,
            "Environment Update" TEXT,
            "Environment Anticipated Out Date" TEXT,
            "Environment Notes" TEXT,
            "Action" TEXT
        )
    """)

    # Map existing -> new ('' for missing)
    select_parts = []
    for c in ENV_COLS:
        if c in existing_cols:
            select_parts.append(f'"{c}"')
        else:
            select_parts.append(f"'' AS \"{c}\"")
    select_sql = ", ".join(select_parts)
    cols_csv = ", ".join(f'"{c}"' for c in ENV_COLS)

    cur.execute((
        f'INSERT OR REPLACE INTO environment_tracker__new ({cols_csv}) '
        f'SELECT {select_sql} FROM environment_tracker'
    ))
    cur.execute("DROP TABLE environment_tracker")
    cur.execute("ALTER TABLE environment_tracker__new RENAME TO environment_tracker")
    conn.commit()

def build_environment_tracker(conn: sqlite3.Connection) -> int:
    """
    Build/refresh environment_tracker:
      - Orders from open_dependencies where Environment='Pending'
      - Notification Status, SAP Status from mpp_data
      - DS11, PC21 from sap_tracker
      - Environment Status, Environment Update from epw_data (join on epw_data."Order Number" = Order)
      - Environment Anticipated Out Date, Environment Notes from manual_tracker
      - Action from parsed anticipated out date (rule-based)
    Returns affected rows (inserted/updated).
    """
    _ensure_table(conn)
    cur = conn.cursor()

    # 1) Orders with Environment pending
    cur.executescript("""
        DROP TABLE IF EXISTS __env_orders;
        CREATE TEMP TABLE __env_orders AS
        SELECT od."Order"
        FROM open_dependencies od
        WHERE od."Environment" = 'Pending';
    """)

    # 2) Pull mpp_data (Notification/SAP Status)
    cur.executescript("""
        DROP TABLE IF EXISTS __env_mpp;
        CREATE TEMP TABLE __env_mpp AS
        SELECT
            m."Order",
            m."Notif Status"   AS notif_status,
            m."Primary Status" AS sap_status
        FROM mpp_data m
        INNER JOIN __env_orders o ON o."Order" = m."Order";
    """)

    # 3) Pull DS11/PC21 from sap_tracker
    cur.executescript("""
        DROP TABLE IF EXISTS __env_sap;
        CREATE TEMP TABLE __env_sap AS
        SELECT
            st."Order",
            st."DS11" AS ds11,
            st."PC21" AS pc21
        FROM sap_tracker st
        INNER JOIN __env_orders o ON o."Order" = st."Order";
    """)

    # 4) Pull Environment fields from epw_data (join on "Order Number")
    cur.executescript("""
        DROP TABLE IF EXISTS __env_epw;
        CREATE TEMP TABLE __env_epw AS
        SELECT
            e."Order Number"  AS order_num,
            e."Env Status"    AS env_status,
            e."Enviro Update" AS env_update
        FROM epw_data e
        INNER JOIN __env_orders o ON o."Order" = e."Order Number";
    """)

    # 4.5) Pull manual fields from manual_tracker (optional per order)
    cur.executescript("""
        DROP TABLE IF EXISTS __env_manual;
        CREATE TEMP TABLE __env_manual AS
        SELECT
            mt."Order" AS order_num,
            mt."Environment Anticipated Out Date" AS env_out_date_manual,
            mt."Environment Notes" AS env_notes_manual
        FROM manual_tracker mt
        INNER JOIN __env_orders o ON o."Order" = mt."Order";
    """)

    # 5) Combine (LEFT JOIN to keep every order even if some data is missing)
    cur.executescript("""
        DROP TABLE IF EXISTS __env_final;
        CREATE TEMP TABLE __env_final AS
        SELECT
            o."Order",
            COALESCE(m.notif_status, '')  AS "Notification Status",
            COALESCE(m.sap_status,   '')  AS "SAP Status",
            COALESCE(s.ds11, '')          AS "DS11",
            COALESCE(s.pc21, '')          AS "PC21",
            COALESCE(ep.env_status, '')   AS "Environment Status",
            COALESCE(ep.env_update, '')   AS "Environment Update",
            COALESCE(man.env_out_date_manual, '') AS "Environment Anticipated Out Date",
            COALESCE(man.env_notes_manual, '')    AS "Environment Notes"
        FROM __env_orders o
        LEFT JOIN __env_mpp    m   ON m."Order"     = o."Order"
        LEFT JOIN __env_sap    s   ON s."Order"     = o."Order"
        LEFT JOIN __env_epw    ep  ON ep.order_num  = o."Order"
        LEFT JOIN __env_manual man ON man.order_num = o."Order";
    """)

    # 6) Read rows, compute Action in Python, upsert
    before = conn.total_changes
    cur.execute(f"""
        SELECT
            "Order",
            "Notification Status",
            "SAP Status",
            "DS11",
            "PC21",
            "Environment Status",
            "Environment Update",
            "Environment Anticipated Out Date",
            "Environment Notes"
        FROM __env_final
    """)
    rows = cur.fetchall()

    today = date.today()
    out = []
    for (order_id,
         notif_status, sap_status, ds11, pc21,
         env_status, env_update, env_out_date_text, env_notes) in rows:

        iso = _parse_to_iso(env_out_date_text)
        if not iso:
            action = "Please provide anticipated out date."
        else:
            d = date.fromisoformat(iso)
            if d >= today:
                action = "In progress."
            else:
                action = "Past anticipated out date. Please provide update or close PC21."

        out.append((
            order_id,
            notif_status or "",
            sap_status or "",
            ds11 or "",
            pc21 or "",
            env_status or "",
            env_update or "",
            env_out_date_text or "",
            env_notes or "",
            action
        ))

    cols_csv = ", ".join(f'"{c}"' for c in ENV_COLS)
    placeholders = ", ".join(["?"] * len(ENV_COLS))

    # NEW: keep only currently Pending-Environment orders
    cur.execute('DELETE FROM environment_tracker WHERE "Order" NOT IN (SELECT "Order" FROM __env_final)')

    cur.executemany(f"""
        INSERT OR REPLACE INTO environment_tracker ({cols_csv})
        VALUES ({placeholders})
    """, out)
    conn.commit()
    return conn.total_changes - before

