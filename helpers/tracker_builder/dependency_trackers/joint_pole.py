# helpers/wmp_tracker_builder/dependency_trackers/joint_pole_tracker.py
from __future__ import annotations
import sqlite3
import re
from datetime import date, timedelta

# Final column order for joint_pole_tracker
JP_COLS = [
    "Order",
    "Notification Status",
    "SAP Status",
    "DS42",
    "PC20",
    "Sent to OU Date",
    "Anticipated Out Date",
    "Action",
]

_rx_mdY = re.compile(r"^\s*(\d{1,2})[/-](\d{1,2})[/-](\d{4})\s*$")
_rx_Ymd = re.compile(r"^\s*(\d{4})-(\d{1,2})-(\d{1,2})\s*$")

def _parse_date_mdy_or_iso(text: str | None) -> date | None:
    """Accept M/D/YYYY, MM/DD/YYYY, M-D-YYYY, or YYYY-MM-DD -> date, else None."""
    if not text:
        return None
    t = str(text).strip()
    if not t:
        return None
    m = _rx_mdY.match(t)
    if m:
        mm, dd, yyyy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return date(yyyy, mm, dd)
        except Exception:
            return None
    m = _rx_Ymd.match(t)
    if m:
        yyyy, mm, dd = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return date(yyyy, mm, dd)
        except Exception:
            return None
    return None

def _fmt_mdy(d: date | None) -> str | None:
    """Return MM/DD/YYYY string or None."""
    if d is None:
        return None
    return f"{d.month:02d}/{d.day:02d}/{d.year:04d}"

def _ensure_table(conn: sqlite3.Connection) -> None:
    """Create joint_pole_tracker if missing; migrate to desired schema/order if needed."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS joint_pole_tracker (
            "Order" INTEGER PRIMARY KEY,
            "Notification Status" TEXT,
            "SAP Status" TEXT,
            "DS42" TEXT,
            "PC20" TEXT,
            "Sent to OU Date" TEXT,
            "Anticipated Out Date" TEXT,
            "Action" TEXT
        )
    """)
    conn.commit()

    # Align column order by migrating if needed
    cur.execute("PRAGMA table_info(joint_pole_tracker)")
    existing_cols = [row[1] for row in cur.fetchall()]
    if existing_cols == JP_COLS:
        return

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS joint_pole_tracker__new (
            "Order" INTEGER PRIMARY KEY,
            "Notification Status" TEXT,
            "SAP Status" TEXT,
            "DS42" TEXT,
            "PC20" TEXT,
            "Sent to OU Date" TEXT,
            "Anticipated Out Date" TEXT,
            "Action" TEXT
        )
    """)
    select_parts = []
    for c in JP_COLS:
        if c in existing_cols:
            select_parts.append(f'"{c}"')
        else:
            # fill missing as NULL by default (per your ask)
            select_parts.append(f'NULL AS "{c}"')
    select_sql = ", ".join(select_parts)
    cols_csv = ", ".join(f'"{c}"' for c in JP_COLS)

    cur.execute((
        f'INSERT OR REPLACE INTO joint_pole_tracker__new ({cols_csv}) '
        f'SELECT {select_sql} FROM joint_pole_tracker'
    ))
    cur.execute("DROP TABLE joint_pole_tracker")
    cur.execute("ALTER TABLE joint_pole_tracker__new RENAME TO joint_pole_tracker")
    conn.commit()

def build_joint_pole_tracker(conn: sqlite3.Connection) -> int:
    """
    Build/refresh joint_pole_tracker:
      Orders: open_dependencies where "Joint Pole" = 'Pending'
      Notification/SAP: from mpp_data ("Notif Status", "Primary Status")
      DS42/PC20: from sap_tracker
      Sent to OU Date: from manual_tracker ("Sent to OU Date")
      Anticipated Out Date: 45 days after "Sent to OU Date" (if present)
      Action:
        - if Anticipated Out Date in past -> "Past OU Days. Please complete PC20."
        - elif in future/today -> "In progress."
        - else -> "Check SAP Status"
    Returns affected rows.
    """
    _ensure_table(conn)
    cur = conn.cursor()

    # 1) Orders with Joint Pole pending
    cur.executescript("""
        DROP TABLE IF EXISTS __jp_orders;
        CREATE TEMP TABLE __jp_orders AS
        SELECT od."Order"
        FROM open_dependencies od
        WHERE od."Joint Pole" = 'Pending';
    """)

    # 2) Pull Notification/SAP Status from mpp_data
    cur.executescript("""
        DROP TABLE IF EXISTS __jp_mpp;
        CREATE TEMP TABLE __jp_mpp AS
        SELECT
            m."Order",
            m."Notif Status"   AS notif_status,
            m."Primary Status" AS sap_status
        FROM mpp_data m
        INNER JOIN __jp_orders o ON o."Order" = m."Order";
    """)

    # 3) Pull DS42/PC20 from sap_tracker
    cur.executescript("""
        DROP TABLE IF EXISTS __jp_sap;
        CREATE TEMP TABLE __jp_sap AS
        SELECT
            st."Order",
            st."DS42" AS ds42,
            st."PC20" AS pc20
        FROM sap_tracker st
        INNER JOIN __jp_orders o ON o."Order" = st."Order";
    """)

    # 4) Pull Sent to OU Date from manual_tracker (optional per order)
    cur.executescript("""
        DROP TABLE IF EXISTS __jp_manual;
        CREATE TEMP TABLE __jp_manual AS
        SELECT
            mt."Order" AS order_num,
            mt."Sent to OU Date" AS sent_to_ou
        FROM manual_tracker mt
        INNER JOIN __jp_orders o ON o."Order" = mt."Order";
    """)

    # 5) Combine (keep all orders; leave Sent to OU Date as NULL if missing)
    cur.executescript("""
        DROP TABLE IF EXISTS __jp_final;
        CREATE TEMP TABLE __jp_final AS
        SELECT
            o."Order",
            COALESCE(m.notif_status, '') AS "Notification Status",
            COALESCE(m.sap_status,   '') AS "SAP Status",
            COALESCE(s.ds42, '')         AS "DS42",
            COALESCE(s.pc20, '')         AS "PC20",
            man.sent_to_ou               AS "Sent to OU Date"
        FROM __jp_orders o
        LEFT JOIN __jp_mpp m  ON m."Order" = o."Order"
        LEFT JOIN __jp_sap s  ON s."Order" = o."Order"
        LEFT JOIN __jp_manual man ON man.order_num = o."Order";
    """)

    # 6) Read rows, compute Anticipated Out Date + Action in Python, upsert
    before = conn.total_changes
    cur.execute("""
        SELECT
            "Order",
            "Notification Status",
            "SAP Status",
            "DS42",
            "PC20",
            "Sent to OU Date"
        FROM __jp_final
    """)
    rows = cur.fetchall()

    today = date.today()
    out = []
    for (order_id, notif_status, sap_status, ds42, pc20, sent_ou_text) in rows:
        sent_dt = _parse_date_mdy_or_iso(sent_ou_text)
        if sent_dt:
            anticipated_dt = sent_dt + timedelta(days=45)
            anticipated_txt = _fmt_mdy(anticipated_dt)
            if anticipated_dt < today:
                action = "Past OU Days. Please complete PC20."
            else:
                action = "In progress."
        else:
            anticipated_txt = None
            action = "Check SAP Status"

        out.append((
            order_id,
            notif_status or "",
            sap_status or "",
            ds42 or "",
            pc20 or "",
            sent_ou_text if (sent_ou_text not in ("", None)) else None,  # keep NULL if missing/blank
            anticipated_txt,  # may be None (NULL)
            action,
        ))

    cols_csv = ", ".join(f'"{c}"' for c in JP_COLS)
    placeholders = ", ".join(["?"] * len(JP_COLS))
    # Remove any rows that are no longer in the current pending set
    cur.execute('DELETE FROM joint_pole_tracker WHERE "Order" NOT IN (SELECT "Order" FROM __jp_final)')
    cur.executemany(f"""
        INSERT OR REPLACE INTO joint_pole_tracker ({cols_csv})
        VALUES ({placeholders})
    """, out)
    conn.commit()
    return conn.total_changes - before
