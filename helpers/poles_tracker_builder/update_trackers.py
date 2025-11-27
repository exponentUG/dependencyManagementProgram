# helpers/wmp_tracker_builder/update_trackers.py
from __future__ import annotations
import sqlite3
from typing import Tuple

from .sap_tracker.pivot import update_codes_batch
from .open_dependencies.build import build_open_dependencies
from .dependency_trackers.permit import build_permit_tracker  # NEW
from .dependency_trackers.misctsk import build_misctsk_tracker  # <-- NEW
from .dependency_trackers.faa import build_faa_tracker   # <-- NEW
from .dependency_trackers.environment import build_environment_tracker   # <-- NEW
from .dependency_trackers.land import build_land_tracker   # <-- NEW
from .dependency_trackers.joint_pole import build_joint_pole_tracker

# PC21 moved to immediately AFTER DS11
DESIRED_ORDER = [
    "Order", "Primary Status",
    "SP56", "RP56", "SP57", "RP57",
    "DS42", "PC20", "DS76", "PC24", "DS11", "PC21",
    "AP10", "AP25", "DS28", "DS73",
]

def _ensure_sap_tracker_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    # Create (if missing) with the correct column order (PC21 after DS11)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sap_tracker (
            "Order" INTEGER PRIMARY KEY,
            "Primary Status" TEXT,
            "SP56" TEXT,
            "RP56" TEXT,
            "SP57" TEXT,
            "RP57" TEXT,
            "DS42" TEXT,
            "PC20" TEXT,
            "DS76" TEXT,
            "PC24" TEXT,
            "DS11" TEXT,
            "PC21" TEXT,
            "AP10" TEXT,
            "AP25" TEXT,
            "DS28" TEXT,
            "DS73" TEXT
        )
    """)
    conn.commit()

    # If existing order differs, migrate into a new table with desired order
    cur.execute("PRAGMA table_info(sap_tracker)")
    existing_cols = [row[1] for row in cur.fetchall()]
    have_all = all(col in existing_cols for col in DESIRED_ORDER)
    if (existing_cols != DESIRED_ORDER) or not have_all:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sap_tracker__new (
                "Order" INTEGER PRIMARY KEY,
                "Primary Status" TEXT,
                "SP56" TEXT,
                "RP56" TEXT,
                "SP57" TEXT,
                "RP57" TEXT,
                "DS42" TEXT,
                "PC20" TEXT,
                "DS76" TEXT,
                "PC24" TEXT,
                "DS11" TEXT,
                "PC21" TEXT,
                "AP10" TEXT,
                "AP25" TEXT,
                "DS28" TEXT,
                "DS73" TEXT
            )
        """)
        select_parts = [(f'"{c}"' if c in existing_cols else f'NULL AS "{c}"') for c in DESIRED_ORDER]
        select_sql = ", ".join(select_parts)
        cols_csv = ", ".join(f'"{c}"' for c in DESIRED_ORDER)

        cur.execute((
            "INSERT OR REPLACE INTO sap_tracker__new ({cols}) "
            "SELECT {sel} FROM sap_tracker"
        ).format(cols=cols_csv, sel=select_sql))

        cur.execute("DROP TABLE sap_tracker")
        cur.execute("ALTER TABLE sap_tracker__new RENAME TO sap_tracker")
        conn.commit()

def build_sap_tracker_initial(db_path: str) -> Tuple[int, int]:
    with sqlite3.connect(db_path) as conn:
        c = conn.cursor()

        # Validate sources (add epw_data for permit tracker build)
        for t in ("order_tracking_list", "mpp_data", "sap_data", "epw_data"):
            c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (t,))
            if not c.fetchone():
                raise RuntimeError(f"Required table '{t}' not found in DB: {db_path}")

        _ensure_sap_tracker_schema(conn)

        # Count orders
        c.execute('SELECT COUNT(DISTINCT "Order") FROM order_tracking_list')
        total_orders = c.fetchone()[0] or 0

        # Seed Order + Primary Status
        before = conn.total_changes
        c.executescript("""
            WITH orders AS (
                SELECT DISTINCT "Order" AS order_num FROM order_tracking_list
            ),
            mpp_first AS (
                SELECT m."Order" AS order_num, m."Primary Status" AS primary_status
                FROM mpp_data m
                GROUP BY m."Order"
            ),
            final AS (
                SELECT o.order_num AS "Order",
                       mf.primary_status AS "Primary Status"
                FROM orders o
                LEFT JOIN mpp_first mf ON mf.order_num = o.order_num
            )
            INSERT OR REPLACE INTO sap_tracker ("Order", "Primary Status")
            SELECT "Order", "Primary Status" FROM final;
        """)
        conn.commit()
        rows_written = conn.total_changes - before

        # One-pass fill for all code columns (includes PC21 already)
        rows_written += update_codes_batch(conn)

        # Build open_dependencies after sap_tracker
        rows_written += build_open_dependencies(conn)

        # Build permit_tracker after open_dependencies
        rows_written += build_permit_tracker(conn)
        rows_written += build_misctsk_tracker(conn)   # <-- NEW
        rows_written += build_faa_tracker(conn)          # <-- NEW
        rows_written += build_environment_tracker(conn)   # <-- NEW
        rows_written += build_land_tracker(conn)           # <-- NEW
        rows_written += build_joint_pole_tracker(conn)

        return rows_written, total_orders
