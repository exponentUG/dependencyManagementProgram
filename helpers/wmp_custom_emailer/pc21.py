# helpers/wmp_custom_emailer/pc21.py
from __future__ import annotations
from .common import (
    br, SPACER, render_orders_table_twocol, render_orders_table_onecol
)

TEMPLATE_NAME = "Environment: Request for PC21 Task Completion and Anticipated Out Date"

DEFAULTS = {
    "to": "allh@pge.com",
    "cc": "p6b6@pge.com; pusd@pge.com",
    "subject": "Request for PC21 Task Completion and Anticipated Out Date",
    "body_intro": (
        "Hi Amy,\n\n"
        "I hope you are doing well. The Anticipated ERTC Out Date for the following order(s) "
        "is in the past. Can you please complete the PC21 task or provide updated ERTC?"
    ),
    "body_missing": (
        "What are the anticipated ERTC for the order(s) below?"
    ),
    "post_text": "",
}

def build_html(intro: str,
               missing_body: str,
               post: str,
               orders_primary_pairs: list[tuple[str, str]],
               orders_secondary_list: list[str]) -> str:
    parts = [
        "<html><body>",
        "<div style='font-family:Arial;font-size:10pt;'>",
        br(intro),
        SPACER,
    ]
    if orders_primary_pairs:
        parts += [render_orders_table_twocol(orders_primary_pairs), SPACER]
    else:
        parts += [SPACER]
    parts += [br(missing_body), SPACER]
    if orders_secondary_list:
        parts += [render_orders_table_onecol(orders_secondary_list), SPACER]
    parts += [br(post), "</div>", "</body></html>"]
    return "".join(parts)

# Optional UI label helpers (used by main view to set titles)
LBL_PRIMARY = "Orders Needing PC21 Completion — Order, Anticipated ERTC Out Date"
LBL_SECONDARY = "Orders Missing Anticipated Out Date — Order"
