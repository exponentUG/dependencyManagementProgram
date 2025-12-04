# helpers/misc/comments_parser.py
from __future__ import annotations
import re
from typing import Tuple, Optional

# --- Canonical action labels ---
NO_PERMIT_ACTION        = "Review complete. No permit needed."
PERMIT_OBTAINED_ACTION  = "Permit obtained."
PERMIT_APPLIED_ACTION   = "Permit applied for."
MONUMENT_DONE_ACTION    = "Monument survey complete."
UNDER_REVIEW_ACTION     = "Under review."
REVIEWED_PERMIT_REQ_ACTION = "Request has been reviewed and permit is required."

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
    r"\bno\s+land\s+rights\s+(needed|required)?\b",
    r"\bno\s+land\s+review\s+required\b",
    r"\bland\s+review\s+completed\b",
    r"\bcompleted\s+intake\b.*\bno\s+additional\s+land\s+rights\s+(required|needed)\b",
    r"\bno\s+caltrans\s+permit\s+(required|needed)\b",

    # Generic "no permit needed."
    r"\bno\s+permit\s+needed\b",
    r"\bno\s+permit\s+is\s+needed\b",

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

    # --- NEW: variants from your examples ---

    # "No Caltrans permitting required ..."
    r"\bno\s*(?:cal\s*trans|caltrans)\s+permit\w*\s+(?:is\s+)?(?:required|needed)\b",

    # Generic "no permit required."
    r"\bno\s+permit\s+required\b",

    # Railroad-specific no-permit statements
    r"\bno\s+railroad\s+permit\s+(?:is\s+)?needed\b",
    r"\bno\s+railroad\s+permit\s+(?:is\s+)?required\b",

    # "Land rights secured per LD..., no additional Land needed"
    r"\bno\s+additional\s+land\s+needed\b",

    # "No new land rights needed ..."
    r"\bno\s+(?:new\s+)?land\s+rights\s+needed\b",
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
# 1b) UNDER REVIEW — targeted “batch for land review” patterns
# ============================================================

_UNDER_REVIEW_PATTERNS = [
    # e.g. "WR Team Batch for Land Review"
    r"\bwr\s*team\b.*\bbatch\b.*\bland\s+review\b",
    # e.g. "Batch for Land Review", "Batched for land review"
    r"\bbatch(?:ed)?\s+for\s+land\s+review\b",
]

_under_review_re = [re.compile(p, re.IGNORECASE | re.DOTALL) for p in _UNDER_REVIEW_PATTERNS]


def _matches_under_review(text: str) -> bool:
    t = text or ""
    for rx in _under_review_re:
        if rx.search(t):
            return True
    return False

# ============================================================
# 1c) REVIEWED & PERMIT REQUIRED — targeted patterns
# ============================================================

# Core "permit is required/needed" phrase (also catches "permit require")
_PERMIT_REQUIRED_RE = re.compile(
    r"\bpermit\s+require(?:d)?\b|\bpermit\s+needed\b",
    re.IGNORECASE,
)

# Context words that indicate land/Caltrans/Railroad style comments
_REVIEW_CONTEXT_RE = re.compile(
    r"\b(?:land\s+request|land\s+permitting\s+request|land\s+permitting|caltrans|railroad)\b",
    re.IGNORECASE,
)


def _matches_reviewed_permit_required(text: str) -> bool:
    """
    True when the comment clearly indicates the land request has been
    reviewed/received AND some permit is required/needed.

    Examples that should match:
      - "Land Request Reviewed. Caltrans Site-Specific Permit Required. ..."
      - "Land Permitting Request Received. Caltrans High Risk. Caltrans Site Specific Permit Required. ..."
      - "Land request reviewed. Railroad permit required. ..."
      - "Railroad: High risk. Railroad permit required. ..."
    """
    t = text or ""
    if not t:
        return False

    # Do NOT classify as "permit required" bucket if we already think it's "no permit needed".
    # (This protects things like "no permit required".)
    if _matches_no_permit(t):
        return False

    # Must have "permit required / needed / require"
    if not _PERMIT_REQUIRED_RE.search(t):
        return False

    # Require some land / Caltrans / railroad context
    if _REVIEW_CONTEXT_RE.search(t):
        return True

    # Fallback: if they explicitly say "Land Request Reviewed" or "Land request reviewed"
    # plus any "permit required/needed" text, treat as reviewed & permit required.
    if re.search(r"\bland\s+request\s+reviewed\b", t, re.IGNORECASE):
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
    # “Monument preservation research/field visit/analysis/review performed/completed ... no evidence of monumentation/monuments found”
    r"\bmonument\s+preservation\s+(?:research|field\s+visit|analysis|review)\s+(?:performed|completed)\b.*\bno\s+(?:evidence\s+of\s+monument(?:ation)?|monuments?)\s+found\b",

    # More generic "no evidence of monumentation found"
    r"\bno\s+evidence\s+of\s+monument(?:ation)?\s+found\b",

    # “no monuments found” variants
    r"\bno\s+monuments?\s+found\b",

    # “no monument found per …”
    r"\bno\s+monument\s+found\b",

    # “monument preservation review completed, no monuments found” (allow analysis/review)
    r"\bmonument\s+preservation\s+(?:review|analysis)\s+completed\b.*\bno\s+monuments?\s+found\b",

    # “Monuments were not found within construction limits …”
    r"\bmonuments?\s+were\s+not\s+found\b.*\bconstruction\s+limits\b",
    r"\bmonuments?\s+were\s+not\s+found\b.*\btask\s+complete\b",

    # “No Monument in vicinity …” (including leading underscores like "_No Monument...")
    r"(?:\b|_)no\s+monument\s+in\s+vicinity\b",

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
      1) Permit obtained.                               -> ("Permit obtained.", "-", expiry_or "-")
      2) Permit applied for.                            -> ("Permit applied for.", anticipated_or "-", "-")
      3) Request reviewed & permit required.            -> ("Request has been reviewed and permit is required.", "-", "-")
      4) Under review.                                  -> ("Under review.", "-", "-")
      5) Review complete (no permit).                   -> ("Review complete. No permit needed.", "-", "-")
      6) Monument survey complete.                      -> ("Monument survey complete.", "-", "-")
      7) Fallback                                       -> ("check", "-", "-")
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

    # 3) Request has been reviewed and permit is required
    if _matches_reviewed_permit_required(txt):
        return (REVIEWED_PERMIT_REQ_ACTION, "-", "-")

    # 4) Under review
    if _matches_under_review(txt):
        return (UNDER_REVIEW_ACTION, "-", "-")

    # 5) Review complete / No permit needed
    if _matches_no_permit(txt):
        return (NO_PERMIT_ACTION, "-", "-")

    # 6) Monument survey complete
    if _matches_monument_done(txt):
        return (MONUMENT_DONE_ACTION, "-", "-")

    # 7) Default
    return ("check", "-", "-")
