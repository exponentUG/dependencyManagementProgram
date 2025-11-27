# services/emailingServices/wmp/miscTSK/ds73.py
from __future__ import annotations
import os
import sqlite3
from typing import Dict, List

import pandas as pd

from services.db.wmp_db import default_db_path
from helpers.emailHelpers.email import open_outlook_drafts_by_div


# ---------- DB helpers ----------

def _fetch_orders_and_divs(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Step 1–3:
      - From open_dependencies, get Orders where "Open Dependencies" == 'MiscTSK'
      - Join to sap_tracker and keep only DS73 == 'Clear Task'
      - Join to mpp_data to get Div
    """
    sql = """
        SELECT DISTINCT
            s."Order"              AS "Order",
            COALESCE(m."Div", '')  AS "Div"
        FROM open_dependencies od
        INNER JOIN sap_tracker s
            ON s."Order" = od."Order"
        LEFT JOIN mpp_data m
            ON m."Order" = s."Order"
        WHERE UPPER(TRIM(COALESCE(od."Open Dependencies", ''))) = 'MISCTSK'
          AND UPPER(TRIM(COALESCE(s."DS73", ''))) = 'CLEAR TASK'
        ORDER BY "Div", "Order";
    """
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    return pd.DataFrame(rows, columns=["Order", "Div"])


def _build_recipients_map(conn: sqlite3.Connection, div_values: List[str]) -> Dict[str, Dict[str, List[str]]]:
    """
    Build a recipients_map compatible with open_outlook_drafts_by_div:

        {
            "NB": {"to": ["someone@pge.com"], "cc": ["p6b6@pge.com", ...]},
            ...
        }

    Uses ds73_contact_list for To:, and always puts the three CCs.
    If a Div has no row in ds73_contact_list, we still create an entry
    with empty To and the standard CC list.
    """
    base_cc = ["p6b6@pge.com", "pusd@pge.com", "s2f6@pge.com"]
    rec_map: Dict[str, Dict[str, List[str]]] = {}

    cur = conn.cursor()
    cur.execute('SELECT "DIV", "LAN ID" FROM ds73_contact_list')
    for div, lan in cur.fetchall():
        if not div:
            continue
        email = (lan or "").strip()
        if not email:
            continue
        key = str(div).strip().upper()
        rec_map[key] = {
            "to": [email],
            "cc": list(base_cc),
        }

    # Ensure every Div we’re actually emailing on has at least CC-only entry
    for div in div_values:
        key = str(div or "").strip().upper() or "UNKNOWN_DIV"
        if key not in rec_map:
            rec_map[key] = {
                "to": [],
                "cc": list(base_cc),
            }

    return rec_map


# ---------- Main entry ----------

def wmp_miscTSK_ds73() -> int:
    """
    Create Outlook draft emails for DS73 closure for MiscTSK orders.

    Flow:
      1) From open_dependencies:
           "Open Dependencies" == 'MiscTSK'
      2) Filter to those where sap_tracker.DS73 == 'Clear Task'
      3) Fetch Div for each order from mpp_data
      4) Group by Div, look up recipient from ds73_contact_list,
         and open Outlook drafts with an Order/Div table.

    Returns:
        int: number of orders included across all drafts.
    """
    db_path = default_db_path()
    if not os.path.isfile(db_path):
        raise FileNotFoundError(
            f"Database not found at {db_path!r}. Run the WMP tracker builder first."
        )

    with sqlite3.connect(db_path) as conn:
        df = _fetch_orders_and_divs(conn)

        if df.empty:
            print("No orders found for MiscTSK + DS73 = 'Clear Task'. No emails created.")
            return 0

        recipients_map = _build_recipients_map(conn, df["Div"].tolist())

    # Email template
    subject = "Request for DS73 Task Closure"
    body = (
        "Hi,<br><br>"
        "I hope you are doing well. Can you please review the order(s) below and close DS73 in SAP?"
        "<br><br>"
    )

    # Uses your existing helper to:
    #  - group by Div
    #  - build HTML table with Order & Div
    #  - create one draft per Div
    open_outlook_drafts_by_div(
        df=df,
        columns_order=["Order", "Div"],
        recipients_map=recipients_map,
        subject=subject,
        body=body,
    )

    print(f"Prepared DS73 email drafts for {len(df)} order(s).")
    return int(len(df))
