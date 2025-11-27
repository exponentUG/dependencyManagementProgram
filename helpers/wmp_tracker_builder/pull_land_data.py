# helpers/wmp_tracker_builder/pull_land_data.py
from __future__ import annotations

import sqlite3
from typing import Tuple, Set
import pandas as pd

DATE_FMT = "%m/%d/%Y"
ALLOWED_MAT: Set[str] = {
    "06G","08S","2AJ","2AR","48L","49B","49C","49D","49E","49H","49I","49S","49T","49X","56S",
    "06B","06A","06D","06E","06H","06I","08J","3UT","3US","3UP","3UD","3UE","3UF","3UL","2AE","2BD",
    "KAF","KBC"
}

def _fmt_date(val):
    if pd.isna(val) or str(val).strip() == "":
        return None
    dt = pd.to_datetime(val, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.strftime(DATE_FMT)

def _ci(df: pd.DataFrame, name: str) -> str | None:
    name = name.strip().lower()
    for c in df.columns:
        if str(c).strip().lower() == name:
            return c
    return None

def pull_land_data(db_path: str, xlsx_path: str) -> Tuple[str, int]:
    """
    Read Excel 'Export' → normalize to 'land_data', filter MAT Code to ALLOWED_MAT.
    """
    df = pd.read_excel(xlsx_path, sheet_name="Export")

    names = [
        "Order",                #needed
        "Notification",
        "Name",
        "User Status",
        "SP57 Status",
        "RP57 Status",
        "Land Surveying Status",
        "Land Mgmt Project Status",
        "Land Mgmt Project Status Comments",
        "Land Returned to LOB Details",
        "MAT Code",
        "Priority",
        "Est Req",
        "Est Resource",
        "Est Sup",
        "Est Name",
        "Estimator",
        "Permit Owner Name",
        "Job Owner",
        "Permit Status",                #needed
        "Permit Land Intake Status",
        "Permit Type",                #needed
        "Permit Name",
        "Permit Comment",
        "Return to LOB Reason",
        "Anticipated Application",                #needed
        "Anticipated Issued Date",                #needed
        "Application Date",                #needed
        "Permit Issued Date",                #needed
        "Permit Expiration",                #needed
        "DSDD Required",
        "DSDD Tasks",
        "Permit Rider",
        "Permit Rider Type",
        "Permit Rider Comment",
        "Permit Agency",
        "Annual Permit",                #needed
        "Long Lead Permit",                #needed
        "Long Lead Permit Reason",
        "Exception to Policy",
        "Exception to Policy Status",
        "Exception to Policy Status Comment",
        "Scope of work comments",
        "Record Type Name",
        "Project Land Scope Comments",
        "Permit Created Date"
    ]
    cm = {n: _ci(df, n) for n in names}
    missing = [k for k, v in cm.items() if v is None]
    if missing:
        raise ValueError(f"LAND: missing columns {missing}")

    out = pd.DataFrame()

    # numeric
    out["Order"] = pd.to_numeric(df[cm["Order"]], errors="coerce").astype("Int64")
    out["Notification"] = pd.to_numeric(df[cm["Notification"]], errors="coerce").astype("Int64")

    # strings
    str_cols = [c for c in names if c not in {
        "Order","Notification","Anticipated Application","Anticipated Issued Date",
        "Application Date","Permit Issued Date","Permit Expiration","Permit Created Date"
    }]
    for c in str_cols:
        out[c] = df[cm[c]].astype(str).where(df[cm[c]].notna(), None)

    # dates
    for c in ["Anticipated Application","Anticipated Issued Date","Application Date",
              "Permit Issued Date","Permit Expiration","Permit Created Date"]:
        out[c] = df[cm[c]].apply(_fmt_date)

    # filter
    out = out[out["MAT Code"].str.upper().isin(ALLOWED_MAT)]
    out = out.dropna(subset=["Order"])

    with sqlite3.connect(db_path) as conn:
        out.to_sql("land_data", conn, if_exists="replace", index=False)
        n = len(out)
    return "land_data", n
