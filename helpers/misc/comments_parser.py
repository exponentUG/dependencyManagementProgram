# helpers/misc/comments_parser.py
from __future__ import annotations
import re
from typing import Tuple, Optional

# --- Canonical action labels ---
NO_PERMIT_ACTION        = "Review complete. No permit needed."
PERMIT_OBTAINED_ACTION  = "Permit obtained."
PERMIT_APPLIED_ACTION   = "Permit applied for."
MONUMENT_DONE_ACTION    = "Monument survey complete."

# ============================================================
# 1) NO-PERMIT (review complete) — robust pattern set
# ============================================================

# Disqualifiers to avoid conditional “may/if … permit required” false positives
_DISQUALIFY = [
    r"\bto\s+avoid\s+permit\b",
    r"\bif\b.*\bpermit\b.*\brequired\b",
    r"\bmay\b.*\bpermit\b.*\brequired\b",
    r"\bwill\b.*\bpermit\b.*\brequired\b",
]
_disq_re = re.compile("|".join(_DISQUALIFY), re.IGNORECASE | re.DOTALL)

# Strong positives (explicit, multi-word forms)
_POSITIVE_NO_PERMIT = [
    # Existing
    r"\bno\s+additional\s+land\s+rights\s+(needed|required)\b",
    r"\bno\s+land\s+rights\s+(needed|required)\b",
    r"\bno\s+land\s+review\s+required\b",
    r"\bland\s+review\s+completed\b",
    r"\bcompleted\s+intake\b.*\bno\s+additional\s+land\s+rights\s+(required|needed)\b",
    r"\bno\s+caltrans\s+permit\s+(required|needed)\b",
    r"\bno\s+permit\s+needed\b.*\bannual\b",
    r"\bfacilities\s+located\s+outside\s+of\s+caltrans\s+right[-\s]?of[-\s]?way\b.*\bno\s+caltrans\s+permit\s+required\b",
    r"\bno\s+land\s+needed\b",
    r"\bno\s*ct\s*(?:/|or)\s*rr\s+permit\s+required\b",
    r"\bno\s*cal\s*trans(?:/|\/|\s*or\s*rr)?\s*permit\s+required\b",
    r"\bwork\s+covered\s+under\s+the\s+caltrans\s+annual\s+permit\b",
    r"\bcan\s+be\s+done\s+under\s+the\s+annual\b",
    r"\bland\s+tasks?\s+(completed|cleared)\b",
    r"\bno\s+land\s+issues\b",
    r"\bright[-\s]?of[-\s]?way.*\bsecures\s+land\s+rights\b",
    r"\bsecures\s+land\s+rights\b.*\b(tasks\s+cleared|land\s+tasks?\s+(completed|cleared))?\b",
    r"\bno\s+new\s+land\s+rights\b.*\b(ct|caltrans)\b.*\bpermit\s+needed\b",
    r"\breplied\s+to\b.*\bno\s+caltrans\s+permit\s+(is\s+)?needed\b",
    r"\brelinquish\w*\b.*\b(secures|no\s+additional\s+land\s+rights)\b",

    # NEW targeted variants you provided
    r"\bno\s*(?:cal\s*trans|caltrans)\s*(?:/|or)?\s*(?:rr|railroad)?\s*permit\s+(?:is\s+)?(?:required|needed)\b",
    r"\bno\s*(?:rr|railroad)\s+permit\s+(?:is\s+)?(?:required|needed)\b",
    r"\ball\s+work\s+to\s+take\s+place\s+within\s+public\s+road\s+right[-\s]?of[-\s]?way\b.*\b(released\s+from\s+land|job\s+released\s+from\s+land)\b",
    r"\bexisting\s+land\s+rights\b.*\b(cleared\s+land\s+tasks?|land\s+tasks?\s+(?:cleared|completed))\b",
    r"\b(?:cal\s*trans|caltrans)\b.*\bpermit\s+will\s+not\s+be\s+required\b",
    r"\bwork\s+on\s+private\s+property\b.*\b(cal\s*trans|caltrans)\s+annual\s+permit\b.*\bno\s+new\s+land\s+rights\b",
    r"\blike[-\s]?for[-\s]?like\b.*\bno\s+new\s+land\s+rights\b",
]

# Fuzzy tails that usually mean “no permit/rights” when not disqualified
_POS_FUZZY = [
    r"\bno\s+additional\s+land\s+rights\b",
    r"\bno\s+land\s+rights\b",
    r"\bno\s+land\s+needed\b",
    r"\b(tasks\s+cleared|land\s+tasks?\s+(completed|cleared))\b",
]
_pos_no_permit_re = [re.compile(p, re.IGNORECASE | re.DOTALL) for p in _POSITIVE_NO_PERMIT]
_pos_fuzzy_re     = [re.compile(p, re.IGNORECASE | re.DOTALL) for p in _POS_FUZZY]

def _matches_no_permit(text: str) -> bool:
    t = text or ""
    for rx in _pos_no_permit_re:
        if rx.search(t):
            return True
    if not _disq_re.search(t):
        for rx in _pos_fuzzy_re:
            if rx.search(t):
                return True
    return False

# ============================================================
# 2) PERMIT OBTAINED — with optional expiry extraction
# ============================================================

_OBTAINED_RE = re.compile(
    r"\b(permit|cal\s*trans\s*permit|caltrans\s*permit|railroad\s*permit|rider/extension)\s+obtained\b",
    re.IGNORECASE,
)

_EXPIRES_RE = re.compile(
    r"\b(expires?|expiration)\b\s*[:\-]?\s*"
    r"(?P<m>\d{1,2})[/-](?P<d>\d{1,2})[/-](?P<y>\d{2,4})",
    re.IGNORECASE,
)

def _yyyy_from_two(yy: int) -> int:
    return 2000 + yy

def _fmt_mdy(m: int, d: int, y: int) -> str:
    return f"{m}/{d}/{y}"

def _extract_expiry(text: str) -> Optional[str]:
    m = _EXPIRES_RE.search(text or "")
    if not m:
        return None
    mm = int(m.group("m")); dd = int(m.group("d")); yy = int(m.group("y"))
    yyyy = yy if yy >= 100 else _yyyy_from_two(yy)
    if not (1 <= mm <= 12 and 1 <= dd <= 31 and 2000 <= yyyy <= 2100):
        return None
    return _fmt_mdy(mm, dd, yyyy)

# ============================================================
# 3) PERMIT APPLIED — with optional anticipated issue date
# ============================================================

_APPLIED_TRIGGERS = [
    r"\b(applied|re[-\s]?applied)\b.*\bpermit\b",
    r"\bpermit\s+application\s+submitted\b",
    r"\bapplication\s+submitted\b.*\bpermit\b",
    r"\bsite\s+specific\s+permit\s+application\s+submitted\b",
    r"\bre[-\s]?opened\b.*\bappl(ied|ication)\b.*\bpermit\b",
]
_APPLIED_RE = re.compile("|".join(_APPLIED_TRIGGERS), re.IGNORECASE | re.DOTALL)

_ANTICIPATED_PHRASE_RE = re.compile(r"\banticipated\s+issu(?:e|ed)\s+date\b", re.IGNORECASE)

_ANTICIPATED_DATE_RE = re.compile(
    r"\banticipated\s+issu(?:e|ed)\s+date\b.*?[:\-]?\s*"
    r"(?P<m>\d{1,2})[/-](?P<d>\d{1,2})[/-](?P<y>\d{2,4})",
    re.IGNORECASE | re.DOTALL,
)

def _extract_anticipated(text: str) -> Optional[str]:
    m = _ANTICIPATED_DATE_RE.search(text or "")
    if not m:
        return None
    mm = int(m.group("m")); dd = int(m.group("d")); yy = int(m.group("y"))
    yyyy = yy if yy >= 100 else _yyyy_from_two(yy)
    if not (1 <= mm <= 12 and 1 <= dd <= 31 and 2000 <= yyyy <= 2100):
        return None
    return _fmt_mdy(mm, dd, yyyy)

def _matches_applied(text: str) -> bool:
    t = text or ""
    return bool(_APPLIED_RE.search(t) or _ANTICIPATED_PHRASE_RE.search(t))

# ============================================================
# 4) MONUMENT SURVEY COMPLETE — targeted patterns
# ============================================================

# We look for explicit “monument” phrases with completion/no-find signals.
_MONUMENT_POSITIVES = [
    # “Monument preservation research performed. No evidence of monumentation found …”
    r"\bmonument\s+preservation\s+research\s+performed\b.*\bno\s+evidence\s+of\s+monument(?:ation)?\s+found\b",
    # “no monument found per …”
    r"\bno\s+monument\s+found\b",
    # “monument preservation review completed, no monuments found”
    r"\bmonument\s+preservation\s+review\s+completed\b.*\bno\s+monuments?\s+found\b",
    # “Monuments were not found within construction limits … Task Complete”
    r"\bmonuments?\s+were\s+not\s+found\b.*\bconstruction\s+limits\b",
    r"\bmonuments?\s+were\s+not\s+found\b.*\btask\s+complete\b",
    # “No Monument in vicinity …”
    r"\bno\s+monument\s+in\s+vicinity\b",
    # generic but still specific: “monument … not found”
    r"\bmonuments?\b.*\bnot\s+found\b",
]

_monument_done_re = [re.compile(p, re.IGNORECASE | re.DOTALL) for p in _MONUMENT_POSITIVES]

def _matches_monument_done(text: str) -> bool:
    t = text or ""
    # Require the word “monument” to appear at least once to avoid overreach
    if not re.search(r"\bmonument", t, re.IGNORECASE):
        return False
    for rx in _monument_done_re:
        if rx.search(t):
            return True
    return False

# ============================================================
# 5) Main entry
# ============================================================

def parse_comment_semantics(latest_comment: str) -> Tuple[str, str, str]:
    """
    Input: latest free-text comment (from 'Latest Comment').
    Output: (Parsed Action, Parsed Anticipated Issue Date, Parsed Permit Expiration Date)

    Bucket priority:
      1) Permit obtained.              -> ("Permit obtained.", "-", expiry_or "-")
      2) Permit applied for.           -> ("Permit applied for.", anticipated_or "-", "-")
      3) Review complete (no permit).  -> ("Review complete. No permit needed.", "-", "-")
      4) Monument survey complete.     -> ("Monument survey complete.", "-", "-")
      5) Fallback                      -> ("check", "-", "-")
    """
    txt = latest_comment or ""

    # 1) Permit obtained
    if _OBTAINED_RE.search(txt):
        expiry = _extract_expiry(txt) or "-"
        return (PERMIT_OBTAINED_ACTION, "-", expiry)

    # 2) Permit applied
    if _matches_applied(txt):
        antic = _extract_anticipated(txt) or "-"
        return (PERMIT_APPLIED_ACTION, antic, "-")

    # 3) Review complete / No permit needed
    if _matches_no_permit(txt):
        return (NO_PERMIT_ACTION, "-", "-")

    # 4) Monument survey complete
    if _matches_monument_done(txt):
        return (MONUMENT_DONE_ACTION, "-", "-")

    # 5) Default
    return ("check", "-", "-")
