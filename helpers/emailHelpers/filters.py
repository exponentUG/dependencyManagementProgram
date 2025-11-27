# pip install pandas openpyxl pywin32
import pandas as pd
from datetime import date

def norm(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip()

#ePermitStatus filter
def filter_ePermitStatus(df: pd.DataFrame, ePermitStatus: str) -> pd.DataFrame:
    return df[norm(df["E Permit Status"]).str.lower() == ePermitStatus].copy()

#drops the rows where the value of RP56 and SP56 is "COMP"
def filter_drop_both_comp(df: pd.DataFrame) -> pd.DataFrame:
    both_comp = (norm(df["RP56"]).str.upper() == "COMP") & (norm(df["SP56"]).str.upper() == "COMP")
    return df[~both_comp].copy()

#removes the RP56 and SP56 columns
def drop_rp_sp(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop(columns=["RP56", "SP56"], errors="ignore")

#removes the rows where the permit has expired based on the current date
def filter_not_expired(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["Permit Expiration Date"] = pd.to_datetime(df["Permit Expiration Date"], errors="coerce")
    today = pd.Timestamp(date.today())
    return df[~(df["Permit Expiration Date"] < today)].copy()

#parse the dates to display correctly in the table that is created
def parse_dates_for_display(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ("Work Plan Date", "Permit Expiration Date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df