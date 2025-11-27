# helpers/misc/comment_filtering.py
import re
import pandas as pd
from typing import Optional


def extract_top_comment_triplets(
    df_all: pd.DataFrame,
    comment_col: str = "Comment",
    *,
    land_mgmt_col: str = "Land Management Project Status Comments__c",
    order_col: str = "Order",
    notification_col: str = "Notification",
    sub_cateogry_col: str = "Sub-Category",
    work_plan_date_col: str = "Work Plan Date",
    permit_status_col: str = "Permit Status",
    anticipated_application_date_col: str = "Anticipated Application Date",
    action_col: str = "Action",
    assume_year_from_today: Optional[pd.Timestamp] = None,
    max_scan_lines: int = 10,
) -> pd.DataFrame:
    """
    Build a final DataFrame with the following columns, in order:
      1.  Order
      2.  Notification
      3.  Sub-Category
      4.  Work Plan Date
      5.  Permit Status
      6.  Anticipated Application Date
      7.  Latest Comment Date  (newer of Comment vs Land Mgmt)
      8.  Latest Comment                      (two latest lines from Comment as-is)
      9.  Latest Comment from Land Management (two latest lines from Land Mgmt as-is)
      10. LAN ID  (from the same source as Latest Comment Date; with small fallback)
      11. Action

    Notes
    -----
    - Recognizes comment lines that start with either M/D[/YY|YYYY] **or** YYYY/M/D.
    - When a date is M/D without a year, current year is assumed.
    - The "Original" multi-line columns keep up to two meaningful lines, joined with CRLF.
    """

    if assume_year_from_today is None:
        assume_year_from_today = pd.Timestamp.today()

    # --- Patterns ---

    # Accept either M/D[/Y] or YYYY/M/D at the start
    DATE_ALT = r"(?:\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?|\d{4}[/-]\d{1,2}[/-]\d{1,2})"

    pat_full = re.compile(
        rf"""^\s*
            (?P<date>{DATE_ALT})
            \s*[^A-Za-z0-9\r\n]*\s*
            (?P<lan>[A-Za-z0-9]{{4}})
            \s*[^A-Za-z0-9\r\n]*\s*
            (?P<body>.*)
            $""",
        re.VERBOSE,
    )

    pat_leading_date = re.compile(rf"^\s*({DATE_ALT})")

    # Decorative headers/prefaces (***, --- , ===, <<< >>>, markdown headings)
    pat_ignore_line = re.compile(
        r"""^\s*(?:\*{2,}.+\*{2,}|[-=_]{3,}|[<>]{3,}|#+\s.*)$""", re.IGNORECASE
    )
    # Simple "fluff" lines to drop entirely (expand list if needed)
    pat_fluff_full = re.compile(
        r"""^\s*(read on arrival|red on arrival)\s*$""", re.IGNORECASE
    )

    # --- Helpers ---

    def normalize_text(text: str) -> str:
        if not isinstance(text, str):
            return ""
        return (
            text.replace("—", "-")
            .replace("–", "-")
            .replace("\u00a0", " ")
        )

    def find_first_comment_line_block(text: str) -> str:
        """Pick the first plausible comment line for parsing (date/LAN/body)."""
        t = normalize_text(text)
        if not t:
            return ""
        candidates, fallback = [], ""
        count = 0
        for raw_line in t.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            count += 1
            if count > max_scan_lines:
                break
            if pat_ignore_line.match(line) or pat_fluff_full.match(line):
                continue
            if pat_full.match(line):
                return line
            if pat_leading_date.match(line):
                candidates.append(line)
                continue
            if not fallback:
                fallback = line
        return candidates[0] if candidates else fallback

    def latest_two_original_lines(text: str, scan_cap: int = 50) -> str:
        """
        Return up to two latest 'meaningful' lines from the raw text, joined by CRLF.
        Skips blank, decorative, and fluff-only lines. Keeps content as-is.
        """
        t = normalize_text(text)
        if not t:
            return ""
        picked = []
        count = 0
        for raw_line in t.splitlines():
            # keep original spacing for that line (but no trailing newline)
            line_stripped = raw_line.strip()
            if not line_stripped:
                continue
            count += 1
            if count > scan_cap:
                break
            if pat_ignore_line.match(line_stripped) or pat_fluff_full.match(line_stripped):
                continue
            picked.append(raw_line.rstrip())
            if len(picked) == 2:
                break
        return "\r\n".join(picked)

    def normalize_date_str(datestr: str) -> pd.Timestamp:
        """
        Normalize date strings that may be:
          - M/D[/YY|YYYY]
          - YYYY/M/D
        If month/day only, inject current year. Use yearfirst=True for YYYY/M/D.
        """
        if not datestr:
            return pd.NaT
        s = datestr.strip()

        # Detect YYYY/M/D explicitly
        is_yearfirst_literal = bool(re.match(r"^\d{4}[/-]\d{1,2}[/-]\d{1,2}$", s))

        # If not yearfirst and it looks like M/D (two parts), add current year
        if not is_yearfirst_literal:
            sep = "/" if "/" in s else ("-" if "-" in s else None)
            if sep is None:
                return pd.NaT
            parts = s.split(sep)
            if len(parts) == 2:  # no year -> use current year
                s = sep.join([parts[0], parts[1], str(assume_year_from_today.year)])

        # Parse with pandas; force yearfirst if the literal is YYYY/M/D
        return pd.to_datetime(s, errors="coerce", yearfirst=is_yearfirst_literal)

    def parse_line(line: str):
        if not line:
            return pd.NaT, None, ""
        m = pat_full.match(line)
        if not m:
            m_date = pat_leading_date.match(line)
            lead_date = normalize_date_str(m_date.group(1)) if m_date else pd.NaT
            return lead_date, None, line
        d = normalize_date_str(m.group("date"))
        lan = m.group("lan")
        body = m.group("body").strip()
        return d, lan, body

    def parse_series(series: pd.Series) -> pd.Series:
        """Return a Series of tuples (date, lan, body) aligned to index."""
        if series is None or not isinstance(series, pd.Series):
            series = pd.Series(index=df_all.index, dtype="object")
        top_lines = series.apply(find_first_comment_line_block)
        return top_lines.apply(parse_line)

    # --- Parse both sources ---
    parsed_comment = parse_series(df_all.get(comment_col))
    parsed_land = parse_series(df_all.get(land_mgmt_col))

    # --- Build intermediate 'out' with all parsed/original fields ---
    out = pd.DataFrame(index=df_all.index)
    out["Order"] = df_all.get(order_col, pd.Series(index=df_all.index, dtype="object"))
    out["Notification"] = df_all.get(notification_col, pd.Series(index=df_all.index, dtype="object"))
    out["Sub-Category"] = df_all.get(sub_cateogry_col, pd.Series(index=df_all.index, dtype="object"))
    out["Work Plan Date"] = df_all.get(work_plan_date_col, pd.Series(index=df_all.index, dtype="object"))
    out["Permit Status"] = df_all.get(permit_status_col, pd.Series(index=df_all.index, dtype="object"))
    out["Anticipated Application Date"] = df_all.get(anticipated_application_date_col, pd.Series(index=df_all.index, dtype="object"))

    # Parsed (Comment)
    out["Latest Comment Date (Comment)"] = pd.to_datetime(parsed_comment.apply(lambda t: t[0]), errors="coerce")
    out["LAN ID (Comment)"] = parsed_comment.apply(lambda t: t[1]).astype("string")
    out["Latest Comment (Comment)"] = parsed_comment.apply(lambda t: t[2]).astype("string")

    # Parsed (Land Management)
    out["Latest Comment Date (Land Management)"] = pd.to_datetime(parsed_land.apply(lambda t: t[0]), errors="coerce")
    out["LAN ID (Land Management)"] = parsed_land.apply(lambda t: t[1]).astype("string")
    out["Latest Comment (Land Management)"] = parsed_land.apply(lambda t: t[2]).astype("string")

    # Original (two latest lines as-is, with CRLF)
    comment_series = df_all.get(comment_col, pd.Series(index=df_all.index, dtype="object"))
    land_series = df_all.get(land_mgmt_col, pd.Series(index=df_all.index, dtype="object"))
    out["Comment (Original)"] = comment_series.apply(latest_two_original_lines).astype("string")
    out["Land Management Project Status Comments__c (Original)"] = land_series.apply(latest_two_original_lines).astype("string")

    out["Action"] = df_all.get(action_col, pd.Series(index=df_all.index, dtype="object"))

    # --- Final coalescing and selection ---

    # Choose the newer non-NaT date between Comment and Land Mgmt
    comment_dt = out["Latest Comment Date (Comment)"]
    land_dt = out["Latest Comment Date (Land Management)"]

    best_is_comment = comment_dt.notna() & (land_dt.isna() | (comment_dt >= land_dt))

    # 7) Latest Comment Date = newer of the two
    latest_comment_date = comment_dt.where(best_is_comment, land_dt)

    # 8) Latest Comment = two latest lines from Comment (Original)
    latest_comment = out["Comment (Original)"]

    # 9) Latest Comment from Land Management = two latest lines from Land Mgmt (Original)
    latest_comment_from_land = out["Land Management Project Status Comments__c (Original)"]

    # 10) LAN ID = LAN from the same source as the Latest Comment Date
    lan_from_comment = out["LAN ID (Comment)"].astype("string")
    lan_from_land = out["LAN ID (Land Management)"].astype("string")
    lan_id = lan_from_comment.where(best_is_comment, lan_from_land)

    # Fallback: if chosen LAN is blank/NaN, try the other source
    empty_chosen = lan_id.isna() | (lan_id.str.strip() == "")
    lan_id = lan_id.where(~empty_chosen, lan_from_land.where(best_is_comment, lan_from_comment))

    # Assemble final DF in the exact order requested
    final_out = pd.DataFrame(
        {
            "Order": out["Order"],
            "Notification": out["Notification"],
            "Sub-Category": out["Sub-Category"],
            "Work Plan Date": out["Work Plan Date"],
            "Permit Status": out["Permit Status"],
            "Anticipated Application Date": out["Anticipated Application Date"],
            "Latest Comment Date": latest_comment_date,
            "Latest Comment": latest_comment,
            "Latest Comment from Land Management": latest_comment_from_land,
            "LAN ID": lan_id,
            "Action": out["Action"],
        }
    )

    return final_out