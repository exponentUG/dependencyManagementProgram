# helpers/wmp_custom_emailer/ap10.py
from __future__ import annotations
from .common import br, SPACER, render_orders_table_onecol

TEMPLATE_NAME = "MiscTSK: AP10 Closure Email"

DEFAULTS = {
    "to": "",
    "cc": "p6b6@pge.com; pusd@pge.com",
    "subject": "Request to Close AP10",
    "body_intro": (
        "Hi,\n\n"
        "I hope you are doing well. Can you please close AP10 for the following order(s):"
    ),
    "post_text": "",  # nothing
}

LBL_PRIMARY = "Orders for AP10 Closure — Order"

def build_html(intro: str, post: str, orders: list[str]) -> str:
    parts = [
        "<html><body>",
        "<div style='font-family:Arial;font-size:10pt;'>",
        br(intro),
        SPACER,
    ]
    if orders:
        parts += [render_orders_table_onecol(orders), SPACER]
    if post:
        parts += [br(post)]
    parts += ["</div>", "</body></html>"]
    return "".join(parts)
