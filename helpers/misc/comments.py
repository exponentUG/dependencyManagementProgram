# helpers/misc/comments.py
from __future__ import annotations
import re
from datetime import date
from typing import Optional, Tuple

# Month map for 12JAN25 style
_MON = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT":10, "NOV":11, "DEC":12
}

_BANNER_LIKE = re.compile(r"^[^0-9]*$")  # no digits at all

# Core date patterns
_date_num_head = re.compile(r"""^\s*
    (?P<m>\d{1,2})[/-](?P<d>\d{1,2})(?:[/-](?P<y>\d{2,4}))?
    \s*[-:\.]?\s*
""", re.IGNORECASE | re.VERBOSE)

_date_mon_head = re.compile(r"""^\s*
    (?P<d>\d{1,2})(?P<mon>[A-Za-z]{3})(?P<y>\d{2,4})
    \s*[-:\.]?\s*
""", re.IGNORECASE | re.VERBOSE)

# Strict 4-char LAN token with boundary
_lan_token = re.compile(r"""^\s*(?P<lan>[A-Za-z0-9]{4})(?![A-Za-z0-9])\s*[-:\.]?\s*""")

# Bracketed header: [MM/DD[/YY|YYYY] LAN4]
_bracket = re.compile(r"""^\s*
    \[\s*(?P<date>\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?)\s+(?P<lan>[A-Za-z0-9]{4})\s*\]\s*
""", re.IGNORECASE | re.VERBOSE)

# NEW: date followed by (LAN)
# 10/7/25(A4EA)- text
_date_then_paren_lan = re.compile(r"""^\s*
    (?P<date>\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?)\s*
    \(\s*(?P<lan>[A-Za-z0-9]{4})\s*\)\s*[-:\u2013\u2014]?\s*
""", re.IGNORECASE | re.VERBOSE)

# NEW: parenthesized (date-LAN) or (date - LAN)
# (5/21/25-CSSB) text, (10/14/25 - JJWm) - text
_paren_date_lan = re.compile(r"""^\s*
    \(\s*(?P<date>\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?)\s*
    (?:[-\u2013\u2014]\s*)?(?P<lan>[A-Za-z0-9]{4})\s*\)\s*[-:\u2013\u2014]?\s*
""", re.IGNORECASE | re.VERBOSE)

# NEW: ISO date then hyphen LAN
# 2025-10-29-J56H - text
_iso_hyphen_lan = re.compile(r"""^\s*
    (?P<y>\d{4})-(?P<m>\d{2})-(?P<d>\d{2})\s*[-\u2013\u2014]\s*(?P<lan>[A-Za-z0-9]{4})
    \s*[-:\u2013\u2014]?\s*
""", re.IGNORECASE | re.VERBOSE)

def _is_noise_banner(s: str) -> bool:
    """
    Treat lines like 'RED ON ARRIVAL' or other all-caps, digit-less banners as noise.
    Also ignore empty/whitespace and lines wrapped in asterisks.
    """
    t = (s or "").strip()
    if not t:
        return True
    if t.startswith("*"):
        return True
    # all-caps, no digits, not too long -> banner-ish
    if t.upper() == t and _BANNER_LIKE.match(t) and len(t) <= 120:
        return True
    return False

def _to_iso(yyyy: int, mm: int, dd: int) -> Optional[str]:
    try:
        return date(yyyy, mm, dd).isoformat()
    except Exception:
        return None

def _yyyy_from_two(yy: int, today: date) -> int:
    # simple rule: 25 -> 2025
    return 2000 + yy

def _parse_mmdd_like(text: str, today: date) -> Tuple[Optional[str], Optional[str]]:
    m = re.match(r"""^\s*(?P<m>\d{1,2})[/-](?P<d>\d{1,2})(?:[/-](?P<y>\d{2,4}))?\s*$""", text)
    if not m:
        return None, None
    mm = int(m.group("m")); dd = int(m.group("d"))
    yraw = m.group("y")
    yyyy = (int(yraw) if yraw and len(yraw) == 4
            else _yyyy_from_two(int(yraw), today) if yraw
            else today.year)
    iso = _to_iso(yyyy, mm, dd)
    return (iso, f"{mm:02d}/{dd:02d}/{yyyy:04d}") if iso else (None, None)

def parse_comment_line(line: str, today: Optional[date] = None) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Returns (iso_date, mdy_text, lan_id, comment_text) from a single line.
    Supported headers (in order of precedence):
      - [MM/DD[/YY] LAN] comment
      - YYYY-MM-DD-LAN [-] comment
      - MM/DD[/YY](LAN) [-] comment
      - (MM/DD[/YY]-LAN) [-] comment
      - (MM/DD[/YY] - LAN) [-] comment
      - MM/DD[/YY] [-|:|.] LAN? comment
      - 12JAN25 [-|:|.] LAN? comment
    Lines starting with '***' are ignored.
    """
    if today is None:
        today = date.today()
    if not line:
        return None, None, None, None

    s = line.strip()
    if not s or s.startswith("*"):
        return None, None, None, None

    # 0) [MM/DD LAN]
    m = _bracket.match(s)
    if m:
        iso, mdy = _parse_mmdd_like(m.group("date"), today)
        lan = m.group("lan").upper()
        rest = s[m.end():].strip()
        return iso, mdy, lan, (rest or None)

    # 1) YYYY-MM-DD-LAN
    m = _iso_hyphen_lan.match(s)
    if m:
        yyyy = int(m.group("y")); mm = int(m.group("m")); dd = int(m.group("d"))
        iso = _to_iso(yyyy, mm, dd)
        mdy = f"{mm:02d}/{dd:02d}/{yyyy:04d}" if iso else None
        lan = m.group("lan").upper()
        rest = s[m.end():].strip()
        return iso, mdy, lan, (rest or None)

    # 2) MM/DD(Y) (LAN)
    m = _date_then_paren_lan.match(s)
    if m:
        iso, mdy = _parse_mmdd_like(m.group("date"), today)
        lan = m.group("lan").upper()
        rest = s[m.end():].strip()
        return iso, mdy, lan, (rest or None)

    # 3) (MM/DD(Y)-LAN)
    m = _paren_date_lan.match(s)
    if m:
        iso, mdy = _parse_mmdd_like(m.group("date"), today)
        lan = m.group("lan").upper()
        rest = s[m.end():].strip()
        return iso, mdy, lan, (rest or None)

    # 4) Plain numeric date head
    iso = mdy = lan = None
    rest = s
    m = _date_num_head.match(rest)
    if m:
        mm = int(m.group("m")); dd = int(m.group("d"))
        yraw = m.group("y")
        yyyy = (int(yraw) if yraw and len(yraw) == 4
                else _yyyy_from_two(int(yraw), today) if yraw
                else today.year)
        iso = _to_iso(yyyy, mm, dd)
        if iso:
            mdy = f"{mm:02d}/{dd:02d}/{yyyy:04d}"
        rest = rest[m.end():]
    else:
        # 5) Alpha-month head (12JAN25)
        m2 = _date_mon_head.match(rest)
        if m2:
            dd = int(m2.group("d")); mon = m2.group("mon").upper()
            mm = _MON.get(mon)
            if mm:
                yy = int(m2.group("y"))
                yyyy = yy if yy >= 100 else _yyyy_from_two(yy, today)
                iso = _to_iso(yyyy, mm, dd)
                if iso:
                    mdy = f"{mm:02d}/{dd:02d}/{yyyy:04d}"
                rest = rest[m2.end():]

    # 6) Optional LAN token after the date
    mlan = _lan_token.match(rest)
    if mlan:
        lan = mlan.group("lan").upper()
        rest = rest[mlan.end():]

    comment = rest.strip() if rest.strip() else None
    return iso, mdy, lan, comment

# --- replace the existing extract_latest_comment_block with this version ---
def extract_latest_comment_block(raw: str, today: Optional[date] = None):
    """
    Return the 'latest' parsed line from a multi-line comment block:
      1) Prefer the first line that yields a parsed DATE (iso_date not None)
      2) Else prefer a line that yields a LAN (lan not None)
      3) Else pick the first non-noise textual line
    Ignore banner-like lines such as 'RED ON ARRIVAL' or '*** ... ***'.
    """
    if today is None:
        today = date.today()
    if not raw:
        return None, None, None, None

    dated: list[tuple[Optional[str], Optional[str], Optional[str], Optional[str]]] = []
    with_lan: list[tuple[Optional[str], Optional[str], Optional[str], Optional[str]]] = []
    plain: list[tuple[Optional[str], Optional[str], Optional[str], Optional[str]]] = []

    for line in str(raw).splitlines():
        # skip obvious banners before parsing
        if _is_noise_banner(line):
            continue

        iso, mdy, lan, txt = parse_comment_line(line, today=today)

        # if parse_comment_line produced nothing meaningful, skip
        if not (iso or lan or (txt and txt.strip())):
            continue

        if iso:
            dated.append((iso, mdy, lan, txt))
        elif lan:
            with_lan.append((iso, mdy, lan, txt))
        else:
            # only keep if it's not banner-like (redundant guard for undated text)
            if not _is_noise_banner(txt or ""):
                plain.append((iso, mdy, lan, txt))

        # we still want the very top-most candidate of each type, so we do not break

    if dated:
        return dated[0]
    if with_lan:
        return with_lan[0]
    if plain:
        return plain[0]
    return None, None, None, None