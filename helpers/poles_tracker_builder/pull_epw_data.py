# helpers/wmp_tracker_builder/pull_epw_data.py
from __future__ import annotations

import sqlite3
from typing import Tuple, Set
import pandas as pd

DATE_FMT = "%m/%d/%Y"
ALLOWED_MAT: Set[str] = {
    "07C", "07D", "07O"
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

def pull_epw_data(db_path: str, xlsx_path: str) -> Tuple[str, int]:
    """
    Read Excel 'Export' → normalize to 'epw_data', filter MAT to ALLOWED_MAT.
    Datatypes to store per spec.
    """
    df = pd.read_excel(xlsx_path, sheet_name="Export")

    wanted = [
        "Division",                       #not needed
        "Order Number",
        "Total",                       #not needed
        "Work Plan Date",                       #not needed
        "Click Start Date",                       #not needed
        "LEAPs Expected Out Date",
        "Order Status",                       #not needed
        "MAT",
        "Priority",                       #not needed
        "LEAPs Status",                       #not needed
        "EPW Status",                       #not needed
        "Land Status",                       #not needed
        "Env Status",                       #not needed
        "Open Dependency",
        "WPD Running Lead Time Sufficient?",                       #not needed
        "WPD Running Lead Time",                       #not needed
        "Cycle Time",
        "Last WPD Edit Date",
        "Epermit Update",
        "EPW Submit Days in Age",
        "EPW Expiration Date",
        "Land Update",                       #not needed
        "Latest Land Permit Status",                       #not needed
        "Land Submit Days in Age",                       #not needed
        "Land Permits Update with Agency",                       #not needed
        "Enviro Update",                       #not needed
        "Master Order Created Date",                       #not needed
        "EPW Project Created Date",                       #not needed
        "Land/Enviro Created Date"                       #not needed
    ]
    cm = {w: _ci(df, w) for w in wanted}
    missing = [k for k, v in cm.items() if v is None]
    if missing:
        raise ValueError(f"EPW: missing columns {missing}")

    out = pd.DataFrame()

    # numbers
    out["Order Number"] = pd.to_numeric(df[cm["Order Number"]], errors="coerce").astype("Int64")
    out["Total"] = pd.to_numeric(df[cm["Total"]], errors="coerce")
    out["WPD Running Lead Time"] = pd.to_numeric(df[cm["WPD Running Lead Time"]], errors="coerce")
    out["Cycle Time"] = pd.to_numeric(df[cm["Cycle Time"]], errors="coerce")
    out["EPW Submit Days in Age"] = pd.to_numeric(df[cm["EPW Submit Days in Age"]], errors="coerce")
    out["Land Submit Days in Age"] = pd.to_numeric(df[cm["Land Submit Days in Age"]], errors="coerce")

    # dates
    for col in ["Work Plan Date","Click Start Date","LEAPs Expected Out Date","Last WPD Edit Date",
                "EPW Expiration Date","Master Order Created Date","EPW Project Created Date","Land/Enviro Created Date"]:
        out[col] = df[cm[col]].apply(_fmt_date)

    # strings
    for col in ["Division","Order Status","MAT","Priority","LEAPs Status","EPW Status","Land Status","Env Status",
                "Open Dependency","WPD Running Lead Time Sufficient?","Epermit Update","Land Update",
                "Latest Land Permit Status","Land Permits Update with Agency","Enviro Update"]:
        out[col] = df[cm[col]].astype(str).where(df[cm[col]].notna(), None)

    # filter MAT and clean
    out = out[out["MAT"].str.upper().isin(ALLOWED_MAT)]
    out = out.dropna(subset=["Order Number"])

    with sqlite3.connect(db_path) as conn:
        out.to_sql("epw_data", conn, if_exists="replace", index=False)
        n = len(out)
    return "epw_data", n
