# pip install pandas openpyxl pywin32
import pandas as pd
from pathlib import Path

#load data from the specified workbook and sheet
def load_sheet(file_path: str, sheet_name: str) -> pd.DataFrame:

    p = Path(file_path)
    if not p.exists():
        raise FileNotFoundError(f"Excel file not found: {file_path}")
    df = pd.read_excel(file_path, sheet_name=sheet_name, engine="openpyxl")
    df.columns = df.columns.str.strip()
    return df

#keep the columns that you specify, in the order you specify them
def align_and_reorder_columns(df: pd.DataFrame, canon_columns: list[str]) -> pd.DataFrame:
    rename_map = {}
    # handle likely typo
    if "WMP Comitments" in df.columns and "WMP Commitments" not in df.columns:
        rename_map["WMP Comitments"] = "WMP Commitments"

    # apply renames if any
    if rename_map:
        df = df.rename(columns=rename_map)

    # ensure all canon columns exist (if missing, create empty)
    for col in canon_columns:
        if col not in df.columns:
            df[col] = pd.NA

    # return only canon columns in the desired order
    return df[canon_columns].copy()