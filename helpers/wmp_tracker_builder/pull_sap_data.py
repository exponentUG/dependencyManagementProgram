# helpers/wmp_tracker_builder/pull_sap_data.py
from __future__ import annotations

import sqlite3
from typing import Tuple
import pandas as pd

DATE_FMT = "%m/%d/%Y"

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

def pull_sap_data(db_path: str, xlsx_path: str) -> Tuple[str, int]:
    """
    Read Excel 'Sheet1' and store normalized columns in 'sap_data'.
    Store datatypes as:
      Order:number, Code:str, ActualStart:date, Completed On:date, TaskUsrStatus:str, Completed By:str
    """
    df = pd.read_excel(xlsx_path, sheet_name="Sheet1")

    cm = {
        "Order": _ci(df, "Order"),
        "Code": _ci(df, "Code"),
        "ActualStart": _ci(df, "ActualStart"),
        "Completed On": _ci(df, "Completed On"),
        "TaskUsrStatus": _ci(df, "TaskUsrStatus"),
        "Completed By": _ci(df, "Completed By"),
    }
    missing = [k for k, v in cm.items() if v is None]
    if missing:
        raise ValueError(f"SAP: missing columns {missing}")

    out = pd.DataFrame()
    out["Order"] = pd.to_numeric(df[cm["Order"]], errors="coerce").astype("Int64")
    out["Code"] = df[cm["Code"]].astype(str).where(df[cm["Code"]].notna(), None)
    out["ActualStart"] = df[cm["ActualStart"]].apply(_fmt_date)
    out["Completed On"] = df[cm["Completed On"]].apply(_fmt_date)
    out["TaskUsrStatus"] = df[cm["TaskUsrStatus"]].astype(str).where(df[cm["TaskUsrStatus"]].notna(), None)
    out["Completed By"] = df[cm["Completed By"]].astype(str).where(df[cm["Completed By"]].notna(), None)

    out = out.dropna(subset=["Order"])

    with sqlite3.connect(db_path) as conn:
        out.to_sql("sap_data", conn, if_exists="replace", index=False)
        n = len(out)
    return "sap_data", n
