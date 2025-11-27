import html, re
import pandas as pd

# ----------------- Outlook drafts by Div -----------------
def open_outlook_drafts_by_mat(df: pd.DataFrame, columns_order: list[str], recipients_map: dict, subject: str, body: str) -> None:
    try:
        import win32com.client as win32
    except ImportError as e:
        raise ImportError("pywin32 is required for Outlook. Install with: pip install pywin32") from e

    app = win32.Dispatch("Outlook.Application")
    rec_map = _normalize_map(recipients_map)

    df = df.copy()
    df["Mat Code"] = df["Mat Code"].astype(str).str.strip().replace({"": "Unknown Mat Code"})

    groups = df.groupby("Mat Code", dropna=False)
    print(f"Creating Outlook drafts for {len(groups)} Mat Code group(s)...")

    for mat, g in groups:
        if g.empty:
            continue

        html_table = df_to_excelish_html(g, columns_order)

        info = rec_map.get(str(mat).upper(), {"to": [], "cc": []})
        to_addrs = "; ".join(info.get("to", []))
        cc_addrs = "; ".join(info.get("cc", []))

        mail = app.CreateItem(0)  # olMailItem
        mail.Subject = subject #<---------- Subject for email [EDIT_THIS]
        mail.HTMLBody = body + html_table + "<br>Thank You"
        if to_addrs:
            mail.To = to_addrs
        if cc_addrs:
            mail.CC = cc_addrs

        mail.Display(True)
        print(f" - Draft for Mat='{mat}' | To: {to_addrs or '(none)'} | CC: {cc_addrs or '(none)'} | Rows: {len(g)}")

# ----------------- Outlook drafts by Div -----------------
def open_outlook_drafts_by_div(df: pd.DataFrame, columns_order: list[str], recipients_map: dict, subject: str, body: str) -> None:
    try:
        import win32com.client as win32
    except ImportError as e:
        raise ImportError("pywin32 is required for Outlook. Install with: pip install pywin32") from e

    app = win32.Dispatch("Outlook.Application")
    rec_map = _normalize_map(recipients_map)

    df = df.copy()
    df["Div"] = df["Div"].astype(str).str.strip().replace({"": "UNKNOWN_DIV"})

    groups = df.groupby("Div", dropna=False)
    print(f"Creating Outlook drafts for {len(groups)} Div group(s)...")

    for div, g in groups:
        if g.empty:
            continue

        html_table = df_to_excelish_html(g, columns_order)

        info = rec_map.get(str(div).upper(), {"to": [], "cc": []})
        to_addrs = "; ".join(info.get("to", []))
        cc_addrs = "; ".join(info.get("cc", []))

        mail = app.CreateItem(0)  # olMailItem
        mail.Subject = subject #<---------- Subject for email [EDIT_THIS]
        mail.HTMLBody = body + html_table + "<br>Thank You"
        if to_addrs:
            mail.To = to_addrs
        if cc_addrs:
            mail.CC = cc_addrs

        mail.Display(True)
        print(f" - Draft for Div='{div}' | To: {to_addrs or '(none)'} | CC: {cc_addrs or '(none)'} | Rows: {len(g)}")

# ----------------- Outlook drafts by Div -----------------
def open_outlook_drafts_by_lan_id(df: pd.DataFrame, subject: str, body: str) -> None:
    try:
        import win32com.client as win32
    except ImportError as e:
        raise ImportError("pywin32 is required for Outlook. Install with: pip install pywin32") from e

    app = win32.Dispatch("Outlook.Application")

    df = df.copy()
    df["LAN ID"] = df["LAN ID"].astype(str).str.strip().replace({"": "UNKNOWN_LAN_ID"})

    groups = df.groupby("LAN ID", dropna=False)
    print(f"Creating Outlook drafts for {len(groups)} LAN ID group(s)...")

    for lan_id, g in groups:
        if g.empty:
            continue

        html_table = df_to_excelish_html(g, ["Order", "Notification", "Sub-Category", "Work Plan Date", "Permit Status", "Anticipated Application Date", "Latest Comment Date", "Latest Comment", "Latest Comment from Land Management", "Action"])

        to_addrs = lan_id.lower() + "@pge.com"
        cc_addrs = "JZT8@pge.com; S2F6@pge.com; S8B2@pge.com; B1O1@pge.com; B2GA@pge.com; VDM4@pge.com"
        print(to_addrs)

        mail = app.CreateItem(0)  # olMailItem
        mail.Subject = subject #<---------- Subject for email [EDIT_THIS]
        mail.HTMLBody = body + html_table + "<br>Thank You"
        if to_addrs:
            mail.To = to_addrs
        if cc_addrs:
            mail.CC = cc_addrs

        mail.Display(True)
        print(f" - Draft for Div='{lan_id}' | To: {to_addrs or '(none)'} | CC: {cc_addrs or '(none)'} | Rows: {len(g)}")

# ----------------- email rendering -----------------
def fmt_mdy(ts) -> str:
    if pd.isna(ts):
        return ""
    ts = pd.Timestamp(ts)
    return f"{ts.month}/{ts.day}/{ts.year}"

def df_to_excelish_html(df: pd.DataFrame, columns_order: list[str]) -> str:
    header_bg = "#4472C4"
    header_text = "#FFFFFF"
    alt_row_bg = "#E9F2FF"
    border = "#D9D9D9"
    font = "Calibri, Arial, sans-serif"

    cols = [c for c in columns_order if c in df.columns]
    g = df[cols].copy()

    # format dates if present
    for col in ("Work Plan Date", "Permit Expiration Date", "Anticipated Application Date", "Latest Comment Date"):
        if col in g.columns:
            g[col] = g[col].apply(fmt_mdy)

    # 1) escape everything for safety
    g = g.fillna("")
    g = g.map(lambda x: html.escape(str(x)))

    # 2) turn newlines into <br> ONLY for the multiline columns  (NEW)
    multiline_cols = ["Latest Comment", "Latest Comment from Land Management"]  # (NEW)
    for c in multiline_cols:                                                    # (NEW)
        if c in g.columns:
            g[c] = g[c].str.replace("\r\n", "<br>").str.replace("\n", "<br>")

    table_style = (
        f'border-collapse:collapse; font-family:{font}; font-size:11pt; color:#000000;'
    )
    th_style = (
        f'background:{header_bg}; color:{header_text}; padding:6px 8px; '
        f'border:1px solid {border}; text-align:left; white-space:nowrap;'
    )
    td_style = f'padding:6px 8px; border:1px solid {border}; white-space:nowrap;'

    rows = []
    head = "".join([f"<th nowrap style='{th_style}'>{html.escape(str(c))}</th>" for c in g.columns])
    rows.append(f"<tr>{head}</tr>")

    for i, (_, row) in enumerate(g.iterrows(), start=1):
        bg = alt_row_bg if i % 2 == 0 else "#FFFFFF"
        cells = "".join([f"<td style='{td_style} background:{bg};'>{val}</td>" for val in row])
        rows.append(f"<tr>{cells}</tr>")

    return f"<table style='{table_style}'>{''.join(rows)}</table>"

#Recipients Helper
def _split_addrs(val) -> list[str]:
    if val is None:
        return []
    if isinstance(val, list):
        raw = val
    else:
        s = str(val)
        for sep in ["\n", ";", ","]:
            s = s.replace(sep, "|")
        raw = s.split("|")
    out, seen = [], set()
    for a in raw:
        a = a.strip()
        if a and a.lower() not in seen:
            seen.add(a.lower())
            out.append(a)
    return out

def _normalize_map(rec_map: dict) -> dict:
    norm_map = {}
    for k, v in (rec_map or {}).items():
        norm_map[k.upper()] = {"to": _split_addrs(v.get("to")), "cc": _split_addrs(v.get("cc"))}
    return norm_map