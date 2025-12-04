# helpers/tracker_builder/pull_joint_pole_data.py
from __future__ import annotations

import sqlite3
from typing import Tuple, Set
import pandas as pd

DATE_FMT = "%m/%d/%Y"


def _fmt_date(val):
    """Normalize any Excel-ish date into MM/DD/YYYY string or None."""
    if pd.isna(val) or str(val).strip() == "":
        return None
    dt = pd.to_datetime(val, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.strftime(DATE_FMT)


def _ci(df: pd.DataFrame, name: str) -> str | None:
    """
    Case-insensitive column finder.
    Returns the actual column name in df that matches `name` (ignoring case/whitespace),
    or None if not found.
    """
    name = name.strip().lower()
    for c in df.columns:
        if str(c).strip().lower() == name:
            return c
    return None


def pull_joint_pole_data(
    db_path: str,
    xlsx_path: str,
    ALLOWED_MAT: Set[str],
    REMOVE_BTAG: bool = False,
    REMOVE_SAP_STATUS: bool = False,
    SAP_STATUS_TO_KEEP: Set[str] | None = None,
) -> Tuple[str, int]:
    """
    Read Excel 'Sheet1' -> normalize to 'joint_pole_data'.

    Expected columns in Sheet1 (case-insensitive):
        Order No
        Intent No
        REV
        Primary Intent Status
        Secondary Intent Status
        Status Date
        Due By
        Pre-App
        Last Chgd
        Chgd By
        Prep. By
        ORDER Short Desc
        ORDER Stat
        Taxing
        Child Intent
        MAT code
        Community
        Location Count

    - 'Order No' is stored as Int64 (can be non-unique).
    - Date-like columns are normalized to MM/DD/YYYY or None.
    - All other columns stored as strings (None for blanks).
    - Filter to ALLOWED_MAT using 'MAT code' (case-insensitive).
    - Existing 'joint_pole_data' table is fully replaced on each ingest.
    """
    df = pd.read_excel(xlsx_path, sheet_name="Sheet1")

    wanted = [
        "Order No",
        "Intent No",
        "REV",
        "Primary Intent Status",
        "Secondary Intent Status",
        "Status Date",
        "Due By",
        "Pre-App",
        "Last Chgd",
        "Chgd By",
        "Prep. By",
        "ORDER Short Desc",
        "ORDER Stat",
        "Taxing",
        "Child Intent",
        "MAT code",
        "Community",
        "Location Count",
    ]

    cm = {w: _ci(df, w) for w in wanted}
    missing = [k for k, v in cm.items() if v is None]
    if missing:
        raise ValueError(f"Joint Pole: missing columns {missing}")

    out = pd.DataFrame()

    # numeric key (can be non-unique)
    out["Order No"] = pd.to_numeric(df[cm["Order No"]], errors="coerce").astype("Int64")

    # date-like columns
    date_cols = ["Status Date", "Due By", "Pre-App", "Last Chgd"]
    for col in date_cols:
        out[col] = df[cm[col]].apply(_fmt_date)

    # everything else as strings
    string_cols = [c for c in wanted if c not in (["Order No"] + date_cols)]
    for col in string_cols:
        src = df[cm[col]]
        out[col] = src.astype(str).where(src.notna(), None)

    # Filter MAT against allowed set (case-insensitive)
    out = out[out["MAT code"].astype(str).str.upper().isin(ALLOWED_MAT)]

    # Drop rows with no Order No
    out = out.dropna(subset=["Order No"])

    # Optional generic filters (kept for reuse in other programs)
    if REMOVE_BTAG and "Priority" in out.columns:
        out = out[out["Priority"] != "B"]

    if REMOVE_SAP_STATUS and SAP_STATUS_TO_KEEP and "Order Status" in out.columns:
        out = out[out["Order Status"].astype(str).str.upper().isin(SAP_STATUS_TO_KEEP)]

    with sqlite3.connect(db_path) as conn:
        out.to_sql("joint_pole_data", conn, if_exists="replace", index=False)
        n = len(out)

    return "joint_pole_data", n
