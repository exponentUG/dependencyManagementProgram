from tkinter import ttk
from .base import FONT_BODY

def apply_theme(root):
    style = ttk.Style(root)
    # Use "light" if available; otherwise fall back gracefully.
    try:
        style.theme_use("light")
    except Exception:
        try:
            style.theme_use("light")
        except Exception:
            pass
    style.configure("TButton", font=FONT_BODY)
    style.configure("TLabel", font=FONT_BODY)
    style.configure("Rail.TButton", font=FONT_BODY, anchor="w", padding=(14, 10))