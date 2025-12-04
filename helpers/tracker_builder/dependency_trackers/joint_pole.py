from __future__ import annotations
import sqlite3
import re
from datetime import date, timedelta  # timedelta kept in case we expand logic

# Final column order for joint_pole_tracker
JP_COLS = [
    "Order",
    "Notification Status",
    "SAP Status",
    "DS42",
    "PC20",
    "Primary Intent Status",
    "Status Date",
    "Due By",
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


def _ensure_table(conn: sqlite3.Connection) -> None:
    """Create joint_pole_tracker if missing; migrate to desired schema/order if needed."""
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS joint_pole_tracker (
            "Order" INTEGER PRIMARY KEY,
            "Notification Status" TEXT,
            "SAP Status" TEXT,
            "DS42" TEXT,
            "PC20" TEXT,
            "Primary Intent Status" TEXT,
            "Status Date" TEXT,
            "Due By" TEXT,
            "Action" TEXT
        )
    """
    )
    conn.commit()

    # Align column order by migrating if needed
    cur.execute("PRAGMA table_info(joint_pole_tracker)")
    existing_cols = [row[1] for row in cur.fetchall()]
    if existing_cols == JP_COLS:
        return

    # Rebuild to match JP_COLS, filling missing columns as NULL
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS joint_pole_tracker__new (
            "Order" INTEGER PRIMARY KEY,
            "Notification Status" TEXT,
            "SAP Status" TEXT,
            "DS42" TEXT,
            "PC20" TEXT,
            "Primary Intent Status" TEXT,
            "Status Date" TEXT,
            "Due By" TEXT,
            "Action" TEXT
        )
    """
    )
    select_parts: list[str] = []
    for c in JP_COLS:
        if c in existing_cols:
            select_parts.append(f'"{c}"')
        else:
            select_parts.append(f'NULL AS "{c}"')
    select_sql = ", ".join(select_parts)
    cols_csv = ", ".join(f'"{c}"' for c in JP_COLS)

    cur.execute(
        f"""
        INSERT OR REPLACE INTO joint_pole_tracker__new ({cols_csv})
        SELECT {select_sql} FROM joint_pole_tracker
    """
    )
    cur.execute("DROP TABLE joint_pole_tracker")
    cur.execute('ALTER TABLE joint_pole_tracker__new RENAME TO joint_pole_tracker')
    conn.commit()


def build_joint_pole_tracker(conn: sqlite3.Connection) -> int:
    """
    Build/refresh joint_pole_tracker:

      Orders: open_dependencies where "Joint Pole" = 'Pending'

      From mpp_data:
        - "Notification Status" <- "Notif Status"
        - "SAP Status"          <- "Primary Status"
        - "WPD" (Work Plan Date) used only for estimator logic

      From sap_tracker:
        - "DS42"
        - "PC20"

      From joint_pole_data (per Order No, picking row with latest "Last Chgd"):
        - "Primary Intent Status"
        - "Status Date"
        - "Due By"

      Action (derived in Python):

          if Primary Intent Status is NULL:
              "Review."
          elif contains "DRAFT":
              "Intent in draft. Please review and provide update."
          elif contains "Deleted" or "Cancelled":
              "Intent has been deleted/cancelled. Please review and complete task if joint pole not required."
          elif contains "estimator" and Work Plan Date > 90 days in the future:
              "Pending estimating review. No action required."
          elif contains "estimator" and Work Plan Date <= 90 days in the future:
              "Pending estimating review and WPD in less that 90 days. Please provide update."
          elif contains "Construction":
              "Released to construction. Please complete PC20."
          elif contains "Engineering":
              "Estimation and joint pole attention needed."
          elif contains "sent to ou" and Due By in the past:
              "OU days exceeded. Please complete PC20."
          elif contains "sent to ou" and Due By today or in the future:
              "Pending OU review."
          elif contains "ready" and Status Date > 7 days ago:
              "Intent in ready to send status. Please send to OU for review."
          elif contains "ready" and Status Date <= 7 days ago:
              "Intent in ready to send status. Pending clerical review."
          else:
              "check"
    """
    _ensure_table(conn)
    cur = conn.cursor()

    # 1) Orders with Joint Pole pending
    cur.executescript(
        """
        DROP TABLE IF EXISTS __jp_orders;
        CREATE TEMP TABLE __jp_orders AS
        SELECT od."Order"
        FROM open_dependencies od
        WHERE od."Joint Pole" = 'Pending';
    """
    )

    # 2) Pull Notification/SAP Status + WPD from mpp_data
    cur.executescript(
        """
        DROP TABLE IF EXISTS __jp_mpp;
        CREATE TEMP TABLE __jp_mpp AS
        SELECT
            m."Order",
            m."Notif Status"    AS notif_status,
            m."Primary Status"  AS sap_status,
            m."Work Plan Date"  AS wpd
        FROM mpp_data m
        INNER JOIN __jp_orders o ON o."Order" = m."Order";
    """
    )

    # 3) Pull DS42/PC20 from sap_tracker
    cur.executescript(
        """
        DROP TABLE IF EXISTS __jp_sap;
        CREATE TEMP TABLE __jp_sap AS
        SELECT
            st."Order",
            st."DS42" AS ds42,
            st."PC20" AS pc20
        FROM sap_tracker st
        INNER JOIN __jp_orders o ON o."Order" = st."Order";
    """
    )

    # 4) Base combined data (with WPD)
    cur.executescript(
        """
        DROP TABLE IF EXISTS __jp_base;
        CREATE TEMP TABLE __jp_base AS
        SELECT
            o."Order",
            COALESCE(m.notif_status, '') AS "Notification Status",
            COALESCE(m.sap_status,   '') AS "SAP Status",
            COALESCE(s.ds42, '')         AS "DS42",
            COALESCE(s.pc20, '')         AS "PC20",
            m.wpd                         AS "WPD"
        FROM __jp_orders o
        LEFT JOIN __jp_mpp m ON m."Order" = o."Order"
        LEFT JOIN __jp_sap s ON s."Order" = o."Order";
    """
    )

    # ----------------------------
    # 5) Build mapping from joint_pole_data (Python side)
    # ----------------------------
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='joint_pole_data'"
    )
    has_joint_pole = cur.fetchone() is not None

    joint_best: dict[int, tuple[str | None, str | None, str | None]] = {}

    if has_joint_pole:
        cur.execute(
            """
            SELECT
                "Order No",
                "Primary Intent Status",
                "Status Date",
                "Due By",
                "Last Chgd"
            FROM joint_pole_data
            WHERE "Order No" IN (SELECT "Order" FROM __jp_orders)
        """
        )
        rows = cur.fetchall()

        # Dict: order_no -> (primary_intent_status, status_date, due_by, best_dt)
        tmp: dict[int, tuple[str | None, str | None, str | None, date | None]] = {}

        for order_no, primary_intent, status_date, due_by, last_chgd in rows:
            if order_no is None:
                continue
            dt_new = _parse_date_mdy_or_iso(last_chgd)

            if order_no not in tmp:
                tmp[order_no] = (primary_intent, status_date, due_by, dt_new)
                continue

            # Compare with existing
            cur_primary, cur_status, cur_due, dt_best = tmp[order_no]

            # If we had no date yet but new has a date -> take new
            if dt_best is None and dt_new is not None:
                tmp[order_no] = (primary_intent, status_date, due_by, dt_new)
                continue

            # If both have dates, choose the latest
            if dt_best is not None and dt_new is not None and dt_new > dt_best:
                tmp[order_no] = (primary_intent, status_date, due_by, dt_new)
                continue

            # If dt_new is None and dt_best is not None -> keep existing
            # If both None -> keep first (do nothing)

        # Strip off the dt from the mapping
        for k, (p, s, d, _dt) in tmp.items():
            joint_best[k] = (p, s, d)

    # ----------------------------
    # 6) Read base rows, attach joint_pole_data fields + Action, and upsert
    # ----------------------------
    before = conn.total_changes
    today = date.today()

    cur.execute(
        """
        SELECT
            "Order",
            "Notification Status",
            "SAP Status",
            "DS42",
            "PC20",
            "WPD"
        FROM __jp_base
    """
    )
    base_rows = cur.fetchall()

    out: list[tuple] = []
    for order_id, notif_status, sap_status, ds42, pc20, wpd_text in base_rows:
        # Defaults from joint_pole_data
        if order_id in joint_best:
            primary_intent_status, status_date, due_by = joint_best[order_id]
        else:
            primary_intent_status = None
            status_date = None
            due_by = None

        pis = primary_intent_status  # shorter alias
        pis_lower = (pis or "").lower()

        due_dt = _parse_date_mdy_or_iso(due_by)
        status_dt = _parse_date_mdy_or_iso(status_date)
        wpd_dt = _parse_date_mdy_or_iso(wpd_text)

        # ---------------- Action rules ----------------
        if pis is None or str(pis).strip() == "":
            action = "Review."
        elif "draft" in pis_lower:
            action = "Intent in draft. Please review and provide update."
        elif ("deleted" in pis_lower) or ("cancelled" in pis_lower) or ("canceled" in pis_lower):
            action = (
                "Intent has been deleted/cancelled. Please review and complete task if joint pole not required."
            )
        elif "estimator" in pis_lower and wpd_dt is not None:
            diff_days = (wpd_dt - today).days
            if diff_days > 90:
                action = "Pending estimating review. No action required."
            else:
                action = (
                    "Pending estimating review and WPD in less that 90 days. Please provide update."
                )
        elif "construction" in pis_lower:
            action = "Released to construction. Please complete PC20."
        elif "engineering" in pis_lower:
            action = "Estimation and joint pole attention needed."
        elif "sent to ou" in pis_lower and due_dt is not None and due_dt < today:
            action = "OU days exceeded. Please complete PC20."
        elif "sent to ou" in pis_lower and due_dt is not None and due_dt >= today:
            # Treat 'today' as not exceeded yet
            action = "Pending OU review."
        elif "ready" in pis_lower and status_dt is not None:
            diff_days = (today - status_dt).days
            if diff_days > 7:
                action = "Intent in ready to send status. Please send to OU for review."
            elif diff_days >= 0:
                # 0–7 days old (or future dates treated as 'pending' as well)
                action = "Intent in ready to send status. Pending clerical review."
            else:
                # status_dt in the future but we still consider it 'pending clerical'
                action = "Intent in ready to send status. Pending clerical review."
        else:
            action = "check"
        # ------------------------------------------------

        out.append(
            (
                order_id,
                notif_status or "",
                sap_status or "",
                ds42 or "",
                pc20 or "",
                primary_intent_status,
                status_date,
                due_by,
                action,
            )
        )

    cols_csv = ", ".join(f'"{c}"' for c in JP_COLS)
    placeholders = ", ".join(["?"] * len(JP_COLS))

    # Remove any rows that are no longer in the current pending set
    cur.execute(
        'DELETE FROM joint_pole_tracker WHERE "Order" NOT IN (SELECT "Order" FROM __jp_orders)'
    )

    if out:
        cur.executemany(
            f"""
            INSERT OR REPLACE INTO joint_pole_tracker ({cols_csv})
            VALUES ({placeholders})
        """,
            out,
        )

    conn.commit()
    return conn.total_changes - before
