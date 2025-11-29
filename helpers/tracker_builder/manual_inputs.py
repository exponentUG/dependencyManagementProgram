# helpers/wmp_tracker_builder/manual_inputs.py
from __future__ import annotations
import sqlite3
from typing import List, Tuple, Dict
import pandas as pd

# Map of sheet -> (order_col, notes_col, target_manual_col)
TRACKER_SHEET_MAP: Dict[str, Tuple[str, str, str]] = {
    "Permit": ("Order", "Permit Notes", "Permit Notes"),
    "Land": ("Order", "Land Notes", "Land Notes"),
    "FAA": ("Order", "FAA Notes", "FAA Notes"),
    "Environment": ("Order", "Environment Notes", "Environment Notes"),
    "Joint Pole": ("Order", "Joint Pole Notes", "Joint Pole Notes"),
}

# Columns that manual_tracker should have
MANUAL_COLS = [
    "Order",
    "Environment Anticipated Out Date",
    "Environment Notes",
    "Sent to OU Date",
    "Permit Notes",
    "Land Notes",
    "FAA Notes",
    "Joint Pole Notes",
]

def _ensure_schema(conn: sqlite3.Connection) -> None:
    """Create table if missing and migrate old schemas by adding new columns."""
    cur = conn.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS manual_tracker (
        "Order" INTEGER PRIMARY KEY,
        "Environment Anticipated Out Date" TEXT,
        "Environment Notes" TEXT,
        "Sent to OU Date" TEXT,
        "Permit Notes" TEXT,
        "Land Notes" TEXT,
        "FAA Notes" TEXT,
        "Joint Pole Notes" TEXT
    );
    """)
    cur.execute("PRAGMA table_info(manual_tracker)")
    existing = {row[1] for row in cur.fetchall()}

    # Add any missing columns (migrate older dbs)
    for col in MANUAL_COLS:
        if col != "Order" and col not in existing:
            cur.execute(f'ALTER TABLE manual_tracker ADD COLUMN "{col}" TEXT')

    cur.execute('CREATE UNIQUE INDEX IF NOT EXISTS ux_manual_tracker_order ON manual_tracker("Order")')
    conn.commit()

def save_pasted_pairs(conn: sqlite3.Connection, field_canonical: str, pairs: list[tuple[int, str]]) -> int:
    """
    Persist pasted (Order, Value) rows into manual_tracker.
    Only the provided column is updated (UPSERT); other columns remain intact.
    """
    _ensure_schema(conn)
    cur = conn.cursor()

    fld = field_canonical.strip().lower()

    # Decide which column to update (keep old behavior + allow new)
    if fld.startswith("environment anticipated"):
        col_quoted = '"Environment Anticipated Out Date"'
    elif fld.startswith("environment notes"):
        col_quoted = '"Environment Notes"'
    elif fld.startswith("sent to ou"):
        col_quoted = '"Sent to OU Date"'
    elif fld.startswith("permit notes"):
        col_quoted = '"Permit Notes"'
    elif fld.startswith("land notes"):
        col_quoted = '"Land Notes"'
    elif fld.startswith("faa notes"):
        col_quoted = '"FAA Notes"'
    elif fld.startswith("joint pole notes"):
        col_quoted = '"Joint Pole Notes"'
    else:
        raise ValueError(f"Unsupported field: {field_canonical}")

    parsed_pairs: list[tuple[int, str]] = []
    for o, v in pairs:
        try:
            oid = int(str(o).strip())
        except Exception:
            continue
        val = "" if v is None else str(v).strip()
        parsed_pairs.append((oid, val))

    if not parsed_pairs:
        return 0

    sql = f"""
    INSERT INTO manual_tracker ("Order", {col_quoted})
    VALUES (?, ?)
    ON CONFLICT("Order") DO UPDATE SET
        {col_quoted} = excluded.{col_quoted};
    """
    before = conn.total_changes
    cur.executemany(sql, parsed_pairs)
    conn.commit()
    return conn.total_changes - before

def save_from_tracker_excel(conn: sqlite3.Connection, xlsx_path: str) -> int:
    """
    Read tracker Excel (Permit/Land/FAA/Environment/Joint Pole tabs) and upsert notes
    into manual_tracker using Order as key.

    - Adds any new Orders found in those sheets.
    - Overwrites note columns if non-blank values exist in Excel.
    - Overwrites Environment Anticipated Out Date + Sent to OU Date
      if non-blank values exist in Excel.
    """
    _ensure_schema(conn)

    # 1) Pull existing manual_tracker
    existing_df = pd.read_sql_query(
        'SELECT * FROM manual_tracker',
        conn
    )

    if existing_df.empty:
        existing_df = pd.DataFrame(columns=MANUAL_COLS)

    # Normalize Order to Int64
    if "Order" in existing_df.columns:
        existing_df["Order"] = pd.to_numeric(existing_df["Order"], errors="coerce").astype("Int64")

    # 2) Read each required sheet and extract Order + Notes (+ dates where applicable)
    extracted_frames: List[pd.DataFrame] = []
    xl = pd.ExcelFile(xlsx_path)

    for sheet, (order_col, notes_col, target_col) in TRACKER_SHEET_MAP.items():
        if sheet not in xl.sheet_names:
            # If tracker is missing sheet, just skip it
            continue

        df = xl.parse(sheet)

        if order_col not in df.columns or notes_col not in df.columns:
            # If expected columns missing, skip
            continue

        # Start with Order and the notes column
        cols_to_take = [order_col, notes_col]

        # Add Environment Anticipated Out Date from Environment sheet, if present
        if sheet == "Environment" and "Environment Anticipated Out Date" in df.columns:
            cols_to_take.append("Environment Anticipated Out Date")

        # Add Sent to OU Date from Joint Pole sheet, if present
        if sheet == "Joint Pole" and "Sent to OU Date" in df.columns:
            cols_to_take.append("Sent to OU Date")

        sub = df[cols_to_take].copy()
        sub.rename(columns={order_col: "Order", notes_col: target_col}, inplace=True)
        sub["Order"] = pd.to_numeric(sub["Order"], errors="coerce").astype("Int64")

        # Normalize core notes column: blank/whitespace/"-" -> NA
        sub[target_col] = sub[target_col].astype(str).str.strip()
        sub.loc[sub[target_col].isin(["", "nan", "NaN", "-"]), target_col] = pd.NA

        # Normalize date columns similarly (treat blanks as NA)
        for dcol in ("Environment Anticipated Out Date", "Sent to OU Date"):
            if dcol in sub.columns:
                sub[dcol] = sub[dcol].astype(str).str.strip()
                sub.loc[sub[dcol].isin(["", "nan", "NaN", "-"]), dcol] = pd.NA

        extracted_frames.append(sub.dropna(subset=["Order"]))

    if not extracted_frames:
        return 0

    notes_df = extracted_frames[0]
    for f in extracted_frames[1:]:
        notes_df = notes_df.merge(f, on="Order", how="outer")

    # 3) Build union of Orders
    all_orders = pd.Series(pd.concat([existing_df["Order"], notes_df["Order"]]).unique(), dtype="Int64")
    all_orders_df = pd.DataFrame({"Order": all_orders})

    # 4) Start from existing (so we preserve anything not overwritten by Excel)
    merged = all_orders_df.merge(existing_df, on="Order", how="left")

    # Ensure all manual columns exist in merged
    for col in MANUAL_COLS:
        if col not in merged.columns:
            merged[col] = pd.NA

    # 5) Overlay non-null excel values by Order (notes + the two dates)
    merged = merged.merge(notes_df, on="Order", how="left", suffixes=("", "_new"))

    overlay_cols = [
        "Permit Notes",
        "Land Notes",
        "FAA Notes",
        "Joint Pole Notes",
        "Environment Notes",
        "Environment Anticipated Out Date",
        "Sent to OU Date",
    ]

    for col in overlay_cols:
        new_col = f"{col}_new"
        if new_col in merged.columns:
            merged[col] = merged[new_col].combine_first(merged[col])
            merged.drop(columns=[new_col], inplace=True)

    # 6) Write back: UPSERT row-by-row
    cols_to_write = MANUAL_COLS  # includes preserved + updated columns
    before = conn.total_changes
    cur = conn.cursor()

    placeholders = ", ".join(["?"] * len(cols_to_write))
    col_csv = ", ".join([f'"{c}"' for c in cols_to_write])
    update_csv = ", ".join([f'"{c}"=excluded."{c}"' for c in cols_to_write if c != "Order"])

    sql = f"""
    INSERT INTO manual_tracker ({col_csv})
    VALUES ({placeholders})
    ON CONFLICT("Order") DO UPDATE SET
        {update_csv};
    """

    rows = []
    for _, r in merged.iterrows():
        row_vals = []
        for c in cols_to_write:
            v = r.get(c, pd.NA)
            if pd.isna(v):
                row_vals.append(None)
            else:
                row_vals.append(str(v) if c != "Order" else int(v))
        rows.append(tuple(row_vals))

    cur.executemany(sql, rows)
    conn.commit()
    return conn.total_changes - before
