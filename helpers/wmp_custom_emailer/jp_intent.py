# helpers/wmp_custom_emailer/jp_intent.py
from __future__ import annotations
from .common import (
    br, SPACER, render_orders_table_onecol
)

TEMPLATE_NAME = "Joint Pole: Request to move order(s) to Ready to Send status"

DEFAULTS = {
    "to": "svbm@pge.com",
    "cc": "p6b6@pge.com; pusd@pge.com",
    "subject": "Joint Pole Status Update Needed",
    "body_intro": (
        "Hi Stuti,\n\n"
        "I hope you are doing well. For the following order(s), either the joint pole intent has not been created, or is in draft status. Can you please review and provide the needed estimation action."
    ),
    "post_text": "",
}

def build_html(intro: str,
               post: str,
               orders_primary_list: list[str]) -> str:
    parts = [
        "<html><body>",
        "<div style='font-family:Arial;font-size:10pt;'>",
        br(intro),
        SPACER,
    ]
    if orders_primary_list:
        parts += [render_orders_table_onecol(orders_primary_list), SPACER]
    parts += [br(post), "</div>", "</body></html>"]
    return "".join(parts)

# Optional UI label helper
LBL_PRIMARY = "Orders Missing Joint Pole Intent — Order"
