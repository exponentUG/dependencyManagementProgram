# services/db/poles_db.py
from __future__ import annotations
import os
import sqlite3
from dataclasses import dataclass  # (kept in case other modules import it from here)
from typing import Iterable, List, Tuple, Optional, Dict
from datetime import datetime
import pandas as pd
from pathlib import Path

# ------------------------
# Paths & DB location
# ------------------------
DATA_DIR = "data"
DB_NAME = "poles_rfc_tracker.sqlite3"
DB_PATH = os.path.join(DATA_DIR, DB_NAME)  # kept for backward compatibility

def default_db_path() -> str:
    """
    Absolute path for the tracker DB. Also ensures the 'data/' folder exists.
    Use this from UI/helpers instead of DB_PATH when you need an absolute path.
    """
    abs_data = os.path.abspath(DATA_DIR)
    os.makedirs(abs_data, exist_ok=True)
    return os.path.join(abs_data, DB_NAME)

def get_connection(path: Optional[str] = None) -> sqlite3.Connection:
    """
    Open a SQLite connection with safe, ETL-friendly PRAGMAs.
    """
    dbp = path or default_db_path()
    os.makedirs(os.path.dirname(dbp), exist_ok=True)
    conn = sqlite3.connect(dbp)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA cache_size=-100000;")  # ~100MB
    except Exception:
        pass
    return conn

# ------------------------
# MPP schema & filters
# ------------------------
# Columns & target dtypes (storage dtypes tuned for SQLite)
# Note: SQLite has dynamic typing; we coerce in Python and store as TEXT/INTEGER
MPP_SCHEMA: Dict[str, str] = {
    "Region": "TEXT",                       #not needed
    "Div": "TEXT",
    "Notification": "INTEGER",
    "Order": "INTEGER",
    "Planning Order": "INTEGER",                       #not needed
    "Resource": "TEXT",                       #not needed
    "Work Plan Date": "TEXT",       # stored as "MM/DD/YYYY"
    "Permit Exp Date": "TEXT",      # stored as "MM/DD/YYYY"
    "CLICK Start Date": "TEXT",     # NEW: stored as "MM/DD/YYYY"
    "CLICK End Date": "TEXT",       # NEW: stored as "MM/DD/YYYY"
    "Project Reporting Year": "INTEGER",
    "Program": "TEXT",
    "Sub-Category": "TEXT",
    "Est Req": "TEXT",
    "Priority": "TEXT",
    "MAT": "TEXT",
    "Notif Status": "TEXT",
    "Order User Status": "TEXT",                       #not needed
    "Primary Status": "TEXT",
    "Job Owner": "TEXT",                       #not needed
    "Project Managed Flag": "TEXT",
}

ALLOWED_MAT = {
    "07C", "07D", "07O"
}
ALLOWED_YEARS = {2025, 2026, 2027, 2028, 2029, 2030}
REQUIRED_PM_FLAG = "N"
ALLOWED_SAP_STATUS = ["UNSC", "CONS"]

# ------------------------
# DB bootstrap
# ------------------------
def ensure_db() -> None:
    """Ensure data dir and DB exist; create tables if missing (with current schema)."""
    dbp = default_db_path()
    with sqlite3.connect(dbp) as conn:
        cur = conn.cursor()
        # mpp_data table (create if missing; on replace we’ll overwrite via to_sql)
        cols_sql = ", ".join([f'"{c}" {t}' for c, t in MPP_SCHEMA.items()])
        cur.execute(f'''
            CREATE TABLE IF NOT EXISTS mpp_data (
                {cols_sql}
            )
        ''')
        # order_tracking_list (unique Order list we track forever)
        cur.execute('''
            CREATE TABLE IF NOT EXISTS order_tracking_list (
                "Order" INTEGER PRIMARY KEY
            )
        ''')
        # Placeholder shells; real importers will replace these
        cur.execute('CREATE TABLE IF NOT EXISTS sap_data (dummy TEXT);')
        cur.execute('CREATE TABLE IF NOT EXISTS epw_data (dummy TEXT);')
        cur.execute('CREATE TABLE IF NOT EXISTS land_data (dummy TEXT);')
        conn.commit()

# ------------------------
# Coercion helpers
# ------------------------
def _coerce_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")

def _coerce_text(series: pd.Series) -> pd.Series:
    # Preserve None; trim strings
    s = series.where(series.notna(), None)
    return s.astype(object).apply(lambda v: v.strip() if isinstance(v, str) else v)

def _coerce_date_mdy(series: pd.Series) -> pd.Series:
    """Coerce to MM/DD/YYYY text; blank if invalid. Robust to:
       - ISO datetimes with/without timezone (e.g., 2025-11-10T13:45:00Z)
       - 'YYYY-MM-DD HH:MM', 'MM/DD/YYYY HH:MM'
       - Excel serial dates (1900 system)
       - Epoch seconds / milliseconds
       - 2-digit years and month-name formats
    """
    def from_excel_serial(x):
        # Pandas handles Excel 1900 system via origin='1899-12-30'
        try:
            return pd.to_datetime(float(x), unit='D', origin='1899-12-30', errors='coerce')
        except Exception:
            return pd.NaT

    def from_epoch_number(x):
        # Decide seconds vs millis by magnitude
        try:
            x = float(x)
        except Exception:
            return pd.NaT
        if x > 1e12:   # micro/nano—too big, bail
            return pd.NaT
        if x > 1e11:   # ~> 1973 in milliseconds
            return pd.to_datetime(x, unit='ms', errors='coerce')
        if x > 1e9:    # ~> 2001 in seconds
            return pd.to_datetime(x, unit='s', errors='coerce')
        return pd.NaT

    def to_mdy(val) -> str:
        if val is None:
            return ""
        s = str(val).strip()

        if s == "" or s.upper() in {"NA", "N/A", "NULL", "NONE", "NAN", "-", "—", "TBD"}:
            return ""

        # If it's already a Timestamp or datetime-like
        if isinstance(val, (pd.Timestamp, )):
            dt = pd.Timestamp(val)
            if pd.isna(dt):
                return ""
            return dt.strftime("%m/%d/%Y")

        # Numeric? Try Excel serial or epoch
        if isinstance(val, (int, float)) or s.replace('.', '', 1).isdigit():
            # 1) Excel serial
            dt = from_excel_serial(val)
            if not pd.isna(dt):
                return dt.strftime("%m/%d/%Y")
            # 2) Epoch
            dt = from_epoch_number(val)
            if not pd.isna(dt):
                return dt.strftime("%m/%d/%Y")

        # 1) Let pandas try broadly (handles ISO, timezone, and most datetime strings)
        dt = pd.to_datetime(s, errors="coerce", infer_datetime_format=True, utc=False)
        if not pd.isna(dt):
            return pd.Timestamp(dt).strftime("%m/%d/%Y")

        # 2) Try common explicit formats (including 2-digit years and month names)
        fmts = [
            "%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d",
            "%m/%d/%y", "%m-%d-%y",
            "%Y/%m/%d", "%d-%b-%Y", "%d-%b-%y", "%b %d, %Y",
            "%m/%d/%Y %H:%M", "%Y-%m-%d %H:%M", "%m/%d/%y %H:%M",
        ]
        for fmt in fmts:
            try:
                dt = datetime.strptime(s, fmt)
                return dt.strftime("%m/%d/%Y")
            except Exception:
                pass

        return ""

    return series.apply(to_mdy)

_DATE_COLS = {"Work Plan Date", "Permit Exp Date", "CLICK Start Date", "CLICK End Date"}

def _apply_target_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce df columns to schema dtypes for consistent storage."""
    out = pd.DataFrame()
    for col, dtype in MPP_SCHEMA.items():
        if col not in df.columns:
            out[col] = pd.Series([None] * len(df))
            continue
        s = df[col]
        if dtype == "INTEGER":
            out[col] = _coerce_int(s)
        elif col in _DATE_COLS:
            out[col] = _coerce_date_mdy(s)
        else:
            out[col] = _coerce_text(s)
    return out

# ------------------------
# MPP CSV → DataFrame (filtered)
# ------------------------
def load_and_filter_csv(csv_path: str) -> pd.DataFrame:
    """Read CSV (only needed cols), apply filters & dtype coercion faster."""
    needed_cols = [
        "Region",
        "Div",
        "Notification",
        "Order",
        "Planning Order",
        "Resource",
        "Work Plan Date",
        "Permit Exp Date",
        "CLICK Start Date",
        "CLICK End Date",
        "Project Reporting Year",
        "Program",
        "Sub-Category",
        "Est Req",
        "Priority",
        "MAT",
        "Notif Status",
        "Order User Status",
        "Primary Status",
        "Job Owner",
        "Project Managed Flag",
    ]

    # --- Single-pass read (no header-only pre-pass) ---
    read_kwargs = dict(
        usecols=lambda c: c in needed_cols,  # loads intersection; no error on missing
        dtype=str,                           # keep as plain Python strings
        na_filter=False,                     # faster: don't try to infer NaNs
        low_memory=False,
    )

    # Try pyarrow for speed, fallback to default
    try:
        df = pd.read_csv(csv_path, engine="pyarrow", **read_kwargs)
    except Exception:
        df = pd.read_csv(csv_path, **read_kwargs)

    # Ensure all needed columns exist (even if missing in file)
    for c in needed_cols:
        if c not in df.columns:
            df[c] = ""

    # Reorder columns
    df = df[needed_cols]

    # ---------- FAST FILTERING ON RAW STRINGS ----------
    # Precompute uppercase once (on object dtype; cheaper than string[EA])
    mat_u   = df["MAT"].str.upper()
    pm_u    = df["Project Managed Flag"].str.upper()
    notif_u = df["Notif Status"].str.upper()
    sap_status = df["Primary Status"].str.upper()


    allowed_mat   = {m.upper() for m in ALLOWED_MAT}
    # treat PRY as string for filtering to avoid full numeric conversion
    allowed_years = {str(y) for y in ALLOWED_YEARS}
    allowed_sap_status = {y.upper for y in ALLOWED_SAP_STATUS}
    pry           = df["Project Reporting Year"].astype(str)

    mask = (
        mat_u.isin(allowed_mat)
        & pry.isin(allowed_years)
        & (pm_u == REQUIRED_PM_FLAG.upper())
        & (notif_u != "COMP")
        & sap_status.isin(allowed_sap_status)
    )

    df = df.loc[mask].reset_index(drop=True)

    # ---------- Only now apply expensive dtype coercion ----------
    df = _apply_target_dtypes(df)

    return df

# ------------------------
# Write mpp_data table
# ------------------------
def replace_mpp_data(df: pd.DataFrame) -> int:
    """Replace mpp_data with df; returns rows written."""
    ensure_db()
    dbp = default_db_path()
    with sqlite3.connect(dbp) as conn:
        df.to_sql("mpp_data", conn, if_exists="replace", index=False)
        return len(df)

# ------------------------
# Order-tracking list management
# ------------------------
def _existing_orders(conn: sqlite3.Connection) -> set:
    cur = conn.cursor()
    cur.execute('SELECT "Order" FROM order_tracking_list')
    rows = cur.fetchall()
    return {r[0] for r in rows}

def update_order_tracking_list_from_mpp() -> Tuple[int, int]:
    """
    Reads Orders from mpp_data, appends any new into order_tracking_list.
    Returns (existing_count_before, inserted_count).
    """
    ensure_db()
    dbp = default_db_path()
    with sqlite3.connect(dbp) as conn:
        cur = conn.cursor()
        df_orders = pd.read_sql_query('SELECT DISTINCT "Order" FROM mpp_data WHERE "Order" IS NOT NULL', conn)
        new_orders = set(
            df_orders["Order"]
            .dropna()
            .astype("Int64")
            .dropna()
            .astype(int)
            .tolist()
        )
        have = _existing_orders(conn)
        to_insert = sorted(list(new_orders - have))
        if to_insert:
            cur.executemany(
                'INSERT OR IGNORE INTO order_tracking_list("Order") VALUES (?)',
                [(int(o),) for o in to_insert]
            )
        conn.commit()
        return (len(have), len(to_insert))

def seed_order_tracking_list_if_empty() -> int:
    """If order_tracking_list empty, copy all orders from mpp_data."""
    ensure_db()
    dbp = default_db_path()
    with sqlite3.connect(dbp) as conn:
        cur = conn.cursor()
        cur.execute('SELECT COUNT(1) FROM order_tracking_list')
        has = cur.fetchone()[0]
        if has > 0:
            return 0
        df_orders = pd.read_sql_query('SELECT DISTINCT "Order" FROM mpp_data WHERE "Order" IS NOT NULL', conn)
        vals = (
            df_orders["Order"]
            .dropna()
            .astype("Int64")
            .dropna()
            .astype(int)
            .tolist()
        )
        if vals:
            cur.executemany(
                'INSERT OR IGNORE INTO order_tracking_list("Order") VALUES (?)',
                [(int(v),) for v in vals]
            )
            conn.commit()
            return len(vals)
        return 0

def get_order_tracking_df() -> pd.DataFrame:
    ensure_db()
    dbp = default_db_path()
    with sqlite3.connect(dbp) as conn:
        df = pd.read_sql_query('SELECT "Order" FROM order_tracking_list ORDER BY "Order" ASC', conn)
    return df

# ------------------------
# LOOKUP HELPERS
# ------------------------
def _fmt_mdy(dt: pd.Timestamp | None) -> str:
    if dt is None or pd.isna(dt):
        return ""
    return pd.Timestamp(dt).strftime("%m/%d/%Y")

def get_mpp_first_row_by_order(order_num: int) -> Optional[Dict]:
    """Return the first row for an order from mpp_data as a dict (or None)."""
    ensure_db()
    dbp = default_db_path()
    with sqlite3.connect(dbp) as conn:
        df = pd.read_sql_query(
            'SELECT * FROM mpp_data WHERE "Order" = ? LIMIT 1',
            conn,
            params=(int(order_num),),
        )
        if df.empty:
            return None
        rec = df.iloc[0].where(pd.notna(df.iloc[0]), None).to_dict()
        return rec

def get_mpp_rows_by_order(order_num: int) -> pd.DataFrame:
    """Return all rows for an order from mpp_data (may be multiple)."""
    ensure_db()
    dbp = default_db_path()
    with sqlite3.connect(dbp) as conn:
        df = pd.read_sql_query(
            'SELECT * FROM mpp_data WHERE "Order" = ?',
            conn,
            params=(int(order_num),),
        )
    return df

def get_sap_rows_by_order(order_num: int) -> pd.DataFrame:
    """
    Raw rows for an order from sap_data. Expected columns include:
    "Order", "Code", "ActualStart", "Completed On", "TaskUsrStatus", "Completed By".
    """
    ensure_db()
    dbp = default_db_path()
    with sqlite3.connect(dbp) as conn:
        df = pd.read_sql_query(
            'SELECT * FROM sap_data WHERE "Order" = ?',
            conn,
            params=(int(order_num),),
        )
    return df

def get_sap_code_summary_by_order(order_num: int) -> pd.DataFrame:
    """
    Returns one row per unique Code for a given Order with columns:
    Code | ActualStart | Completed On | TaskUsrStatus | Completed By

    If multiple rows exist for a Code, takes the row with the latest
    coalesced date among ("Completed On", "ActualStart").
    """
    df = get_sap_rows_by_order(order_num)
    if df.empty:
        return pd.DataFrame(columns=["Code", "ActualStart", "Completed On", "TaskUsrStatus", "Completed By"])

    # Ensure required columns exist
    for c in ["Code", "ActualStart", "Completed On", "TaskUsrStatus", "Completed By"]:
        if c not in df.columns:
            df[c] = None

    # Parse dates
    astart = pd.to_datetime(df["ActualStart"], errors="coerce", infer_datetime_format=True)
    cdone  = pd.to_datetime(df["Completed On"], errors="coerce", infer_datetime_format=True)

    # Sort key: prefer the latest among Completed On / ActualStart
    sort_key = pd.concat([cdone.rename("k1"), astart.rename("k2")], axis=1).max(axis=1)
    df = df.assign(_sort_key=sort_key)

    # Keep the most recent record per Code
    df_sorted = df.sort_values(by=["_sort_key"], ascending=False)

    # Deduplicate by Code
    keep = df_sorted.drop_duplicates(subset=["Code"], keep="first").copy()

    # Format date columns to MM/DD/YYYY text
    keep["ActualStart"]  = pd.to_datetime(keep["ActualStart"], errors="coerce").apply(_fmt_mdy)
    keep["Completed On"] = pd.to_datetime(keep["Completed On"], errors="coerce").apply(_fmt_mdy)

    cols = ["Code", "ActualStart", "Completed On", "TaskUsrStatus", "Completed By"]
    return keep.loc[:, cols].reset_index(drop=True)

def get_epw_first_row_by_order(order_num: int) -> Optional[Dict]:
    """
    Return the first row for an order from epw_data (matched on 'Order Number') as a dict, or None.
    """
    ensure_db()
    dbp = default_db_path()
    with sqlite3.connect(dbp) as conn:
        df = pd.read_sql_query(
            'SELECT * FROM epw_data WHERE "Order Number" = ? LIMIT 1',
            conn,
            params=(int(order_num),),
        )
        if df.empty:
            return None
        rec = df.iloc[0].where(pd.notna(df.iloc[0]), None).to_dict()
        return rec

def get_land_first_row_by_order(order_num: int) -> Optional[Dict]:
    """
    Return the first row for an order from land_data (matched on 'Order') as a dict, or None.
    """
    ensure_db()
    dbp = default_db_path()
    with sqlite3.connect(dbp) as conn:
        df = pd.read_sql_query(
            'SELECT * FROM land_data WHERE "Order" = ? LIMIT 1',
            conn,
            params=(int(order_num),),
        )
        if df.empty:
            return None
        rec = df.iloc[0].where(pd.notna(df.iloc[0]), None).to_dict()
        return rec

def fetch_order_tracking_list(db_path: str) -> List[int]:
    """
    Read order_tracking_list.Order and return as list[int].
    Ignores nulls/non-numeric safely.
    """
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query('SELECT "Order" FROM order_tracking_list', conn)

    if df.empty or "Order" not in df.columns:
        return []

    orders = (
        pd.to_numeric(df["Order"], errors="coerce")
        .dropna()
        .astype(int)
        .tolist()
    )
    return orders
