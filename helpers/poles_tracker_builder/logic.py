# helpers/wmp_tracker_builder/logic.py
from __future__ import annotations
from typing import Tuple, Optional, Dict, Any
from datetime import datetime
import pandas as pd
import sqlite3

# --- DB path & helpers now from poles RFC DB ---
from services.db.poles_rfc_db import DB_PATH

# Row fetchers for the Order Information tab
from services.db.poles_rfc_db import (
    get_mpp_first_row_by_order,
    get_sap_code_summary_by_order,
    get_epw_first_row_by_order,
    get_land_first_row_by_order,
)

# Import/update helpers for Step 1 (same interface as before)
from services.db.poles_rfc_db import (
    load_and_filter_csv,
    replace_mpp_data,
    seed_order_tracking_list_if_empty,
    update_order_tracking_list_from_mpp,
    get_order_tracking_df,
)

def _to_int_or_none(x: Any) -> Optional[int]:
    """Best-effort conversion of an order string like '  300123456  ' -> 300123456."""
    if x is None:
        return None
    try:
        return int(str(x).strip())
    except Exception:
        return None


def run_import_and_updates(csv_path: str) -> Tuple[int, int, int]:
    """
    1) Load + filter CSV, replace mpp_data
    2) Seed order_tracking_list if empty (first run)
    3) Otherwise append new orders from mpp_data
    Returns: (rows_in_mpp, seeded_count, appended_count)
    """
    df = load_and_filter_csv(csv_path)
    rows = replace_mpp_data(df)
    seeded = seed_order_tracking_list_if_empty()
    appended = 0
    if seeded == 0:
        _, appended = update_order_tracking_list_from_mpp()
    return rows, seeded, appended


def export_order_list_to_excel(default_path: str | None = None) -> str:
    """
    Export order_tracking_list to Excel. Returns the saved path.
    If default_path provided, it will be used; otherwise caller should supply a save-as path.
    """
    if default_path is None:
        raise ValueError("default_path is required or supply a file path from the UI dialog.")
    df = get_order_tracking_df()  # returns a DataFrame with single column "Order"
    # Keep it simple; autosizing is optional and can be added later if needed
    with pd.ExcelWriter(default_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Orders")
    return default_path

def fetch_open_dependencies_for_order(order_str: str) -> Optional[Dict[str, str]]:
    """
    Return {"Open Dependencies": "<value>"} for the given Order from open_dependencies,
    or None if no matching row is found.
    """
    order_num = _to_int_or_none(order_str)
    if order_num is None:
        return None

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        # If the table can have multiple rows per order, prefer the first inserted row.
        row = cur.execute(
            """
            SELECT "Open Dependencies"
            FROM open_dependencies
            WHERE "Order" = ?
            LIMIT 1
            """,
            (order_num,),
        ).fetchone()

    if not row:
        return None

    val = row[0] if row[0] is not None else ""
    return {"Open Dependencies": str(val)}

def today_strings() -> tuple[str, str]:
    """
    Returns two date strings for UI/filenames:
      - 'MM/DD/YYYY'
      - 'MM-DD-YYYY'
    """
    now = datetime.now()
    return now.strftime("%m/%d/%Y"), now.strftime("%m-%d-%Y")


# -----------------------------
# Order Information lookups
# -----------------------------
def fetch_mpp_first_for_order(order_text: str) -> Optional[Dict]:
    """
    Accepts raw text from the UI, coerces to int, returns the first matching mpp_data row as a dict.
    Returns None if invalid input or not found.
    """
    if not order_text or str(order_text).strip() == "":
        return None
    try:
        order_num = int(str(order_text).strip())
    except ValueError:
        return None
    return get_mpp_first_row_by_order(order_num)


def fetch_sap_summary_for_order(order_text: str) -> Optional[pd.DataFrame]:
    """
    Coerces text -> int and returns SAP summary (one row per Code).
    Returns empty DataFrame if not found; None if input invalid.
    """
    if not order_text or str(order_text).strip() == "":
        return None
    try:
        order_num = int(str(order_text).strip())
    except ValueError:
        return None
    return get_sap_code_summary_by_order(order_num)


def fetch_epw_first_for_order(order_text: str) -> Optional[Dict]:
    """
    Coerces text -> int and returns the first EPW row (as dict) for that order number,
    matched against the 'Order Number' column in epw_data. Returns None if invalid input or not found.
    """
    if not order_text or str(order_text).strip() == "":
        return None
    try:
        order_num = int(str(order_text).strip())
    except ValueError:
        return None
    return get_epw_first_row_by_order(order_num)


def fetch_land_first_for_order(order_text: str) -> Optional[Dict]:
    """
    Coerces text -> int and returns the first Land row (as dict) for that order number,
    matched against the 'Order' column in land_data. Returns None if invalid input or not found.
    """
    if not order_text or str(order_text).strip() == "":
        return None
    try:
        order_num = int(str(order_text).strip())
    except ValueError:
        return None
    return get_land_first_row_by_order(order_num)
