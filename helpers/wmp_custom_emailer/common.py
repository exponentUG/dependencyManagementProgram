# helpers/wmp_custom_emailer/common.py
from __future__ import annotations
import tkinter as tk
from tkinter import ttk, messagebox
import re

# ==== Shared styles & helpers ====
HTML_TABLE_STYLE = (
    "border-collapse:collapse;"
    "font-family:Arial;"
    "font-size:10pt;"
)
HTML_TH_STYLE = (
    "background:#2F5597;color:#fff;padding:6px 10px;"
    "border:1px solid #d0d0d0;text-align:left;"
    "font-family:Arial;font-size:10pt;"
)
HTML_TD_STYLE = "padding:6px 10px;border:1px solid #d0d0d0;text-align:left;"

SPACER = (
    "<table role='presentation' width='100%' cellspacing='0' cellpadding='0' "
    "style='border-collapse:collapse; mso-table-lspace:0pt; mso-table-rspace:0pt;'>"
    "<tr><td height='14' style='line-height:14px; font-size:14px;'>&nbsp;</td></tr>"
    "</table>"
)

def esc(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def br(s: str) -> str:
    parts = esc(s).splitlines()
    return "<br>".join(parts)

# ==== HTML table renderers ====
def render_orders_table_onecol(orders: list[str]) -> str:
    rows = [f'<tr><th style="{HTML_TH_STYLE}">Order</th></tr>']
    for o in orders:
        rows.append(f'<tr><td style="{HTML_TD_STYLE}">{esc(o)}</td></tr>')
    return f'<table style="{HTML_TABLE_STYLE}">' + "".join(rows) + "</table>"

def render_orders_table_twocol(pairs: list[tuple[str, str]]) -> str:
    rows = [(
        f'<tr>'
        f'<th style="{HTML_TH_STYLE}">Order</th>'
        f'<th style="{HTML_TH_STYLE}">Anticipated ERTC Out Date</th>'
        f'</tr>'
    )]
    for o, e in pairs:
        rows.append(
            f'<tr>'
            f'<td style="{HTML_TD_STYLE}">{esc(o)}</td>'
            f'<td style="{HTML_TD_STYLE}">{esc(e)}</td>'
            f'</tr>'
        )
    return f'<table style="{HTML_TABLE_STYLE}">' + "".join(rows) + "</table>"

# ==== Row widgets ====
class OrderList(ttk.Frame):
    """Single-column dynamic rows widget with bulk-paste."""
    SPLIT_RE = re.compile(r"[,\s;]+")

    def __init__(self, parent):
        super().__init__(parent)
        self.columnconfigure(0, weight=1)
        self._rows: list[tuple[ttk.Entry, ttk.Button]] = []

        hdr = ttk.Frame(self)
        hdr.grid(row=0, column=0, sticky="ew", pady=(0, 4))
        ttk.Label(hdr, text="Order", width=24).grid(row=0, column=0, sticky="w")

        self.rows_wrap = ttk.Frame(self)
        self.rows_wrap.grid(row=1, column=0, sticky="ew")
        self.rows_wrap.columnconfigure(0, weight=1)

        controls = ttk.Frame(self)
        controls.grid(row=2, column=0, sticky="w", pady=(6, 0))
        ttk.Button(controls, text="+ Add Row", command=self.add_row).grid(row=0, column=0, padx=(0, 6))
        ttk.Button(controls, text="Paste List", command=self._paste_list).grid(row=0, column=1)

        self.add_row()

    def _attach_paste_binds(self, entry: ttk.Entry):
        entry.bind("<<Paste>>", self._on_paste_event, add="+")
        entry.bind("<Control-v>", self._on_paste_event, add="+")
        entry.bind("<Control-V>", self._on_paste_event, add="+")

    def _normalize_tokens(self, text: str) -> list[str]:
        return [t.strip().strip("'\"") for t in self.SPLIT_RE.split(text.strip()) if t.strip()]

    def _on_paste_event(self, event) -> str | None:
        try:
            text = event.widget.clipboard_get()
        except tk.TclError:
            return "break"
        tokens = self._normalize_tokens(text)
        if len(tokens) <= 1:
            return None
        cur: ttk.Entry = event.widget
        cur.delete(0, "end"); cur.insert(0, tokens[0])
        for tok in tokens[1:]:
            self.add_row(order_val=tok)
        return "break"

    def _paste_list(self):
        try:
            text = self.clipboard_get()
        except tk.TclError:
            messagebox.showinfo("Paste", "Clipboard is empty.")
            return
        tokens = self._normalize_tokens(text)
        if not tokens:
            messagebox.showinfo("Paste", "No order numbers detected in clipboard.")
            return

        if self._rows:
            first_entry, _ = self._rows[0]
            if not (first_entry.get() or "").strip():
                first_entry.insert(0, tokens[0]); tokens = tokens[1:]
        for tok in tokens:
            self.add_row(order_val=tok)

    def add_row(self, order_val: str = ""):
        r = len(self._rows)
        rowf = ttk.Frame(self.rows_wrap)
        rowf.grid(row=r, column=0, sticky="ew", pady=2)
        rowf.columnconfigure(0, weight=1)

        e_order = ttk.Entry(rowf, width=32)
        e_order.grid(row=0, column=0, sticky="w")
        if order_val: e_order.insert(0, order_val)
        self._attach_paste_binds(e_order)

        def _remove():
            try:
                rowf.destroy()
                self._rows.remove((e_order, btn_rm))
                for i, (eo, br) in enumerate(self._rows):
                    eo.master.grid_configure(row=i)
            except Exception:
                pass

        btn_rm = ttk.Button(rowf, text="Remove", command=_remove)
        btn_rm.grid(row=0, column=1, padx=(8, 0))

        self._rows.append((e_order, btn_rm))

    def get_rows(self) -> list[str]:
        out = []
        for e_order, _ in self._rows:
            order = (e_order.get() or "").strip()
            if order:
                out.append(order)
        return out


class OrderListTwoCol(ttk.Frame):
    """
    Two-column dynamic rows widget with smart bulk-paste.
    Accepts 2-col Excel blocks (TAB/newline), CSV, or two-field lines.
    Also supports single-column paste into either side.
    """
    SPLIT_RE = re.compile(r"[,\s;]+")

    def __init__(self, parent):
        super().__init__(parent)
        self.columnconfigure(0, weight=1)
        self._rows: list[tuple[ttk.Entry, ttk.Entry, ttk.Button]] = []

        hdr = ttk.Frame(self)
        hdr.grid(row=0, column=0, sticky="ew", pady=(0, 4))
        ttk.Label(hdr, text="Order", width=24).grid(row=0, column=0, sticky="w")
        ttk.Label(hdr, text="Anticipated ERTC Out Date", width=28).grid(row=0, column=1, sticky="w")

        self.rows_wrap = ttk.Frame(self)
        self.rows_wrap.grid(row=1, column=0, sticky="ew")
        self.rows_wrap.columnconfigure(0, weight=1)

        controls = ttk.Frame(self)
        controls.grid(row=2, column=0, sticky="w", pady=(6, 0))
        ttk.Button(controls, text="+ Add Row", command=self.add_row).grid(row=0, column=0, padx=(0, 6))
        ttk.Button(controls, text="Paste 2-Column Table", command=self._paste_table).grid(row=0, column=1, padx=(0, 6))
        ttk.Button(controls, text="Paste List to Order", command=self._paste_list_left).grid(row=0, column=2, padx=(0, 6))
        ttk.Button(controls, text="Paste List to ERTC", command=self._paste_list_right).grid(row=0, column=3)

        self.add_row()

    # Parsing helpers
    def _parse_pairs(self, text: str) -> list[tuple[str, str]]:
        lines = [ln.strip() for ln in text.strip().splitlines() if ln.strip()]
        pairs: list[tuple[str, str]] = []
        for ln in lines:
            if "\t" in ln:
                parts = [p.strip().strip("'\"") for p in ln.split("\t")]
            elif "," in ln:
                parts = [p.strip().strip("'\"") for p in ln.split(",")]
            else:
                parts = re.split(r"\s+", ln, maxsplit=1)
                parts = [p.strip().strip("'\"") for p in parts]
            if len(parts) == 1:
                pairs.append((parts[0], ""))
            else:
                pairs.append((parts[0], parts[1]))
        return pairs

    def _try_table_paste(self, widget: ttk.Entry) -> str | None:
        try:
            text = widget.clipboard_get()
        except tk.TclError:
            return "break"
        pairs = self._parse_pairs(text)
        is_table = len(pairs) > 1 or (len(pairs) == 1 and pairs[0][1] != "")
        if not is_table:
            return None

        # find row index
        row_idx = None
        for i, (eo, ee, _btn) in enumerate(self._rows):
            if widget is eo or widget is ee:
                row_idx = i
                break
        if row_idx is None:
            row_idx = 0

        while len(self._rows) <= row_idx:
            self.add_row()

        e_order, e_ertc, _ = self._rows[row_idx]
        e_order.delete(0, "end"); e_order.insert(0, pairs[0][0])
        e_ertc.delete(0, "end"); e_ertc.insert(0, pairs[0][1])

        for o, e in pairs[1:]:
            self.add_row(order_val=o, ertc_val=e)

        return "break"

    def _attach_paste_binds(self, entry: ttk.Entry, side: str):
        def _on_paste(event):
            handled = self._try_table_paste(event.widget)
            if handled == "break":
                return "break"
            # fallback single-column
            try:
                text = event.widget.clipboard_get()
            except tk.TclError:
                return "break"
            tokens = [t.strip().strip("'\"") for t in self.SPLIT_RE.split(text.strip()) if t.strip()]
            if len(tokens) <= 1:
                return None
            cur: ttk.Entry = event.widget
            cur.delete(0, "end"); cur.insert(0, tokens[0])
            for tok in tokens[1:]:
                if side == "left":
                    self.add_row(order_val=tok)
                else:
                    self.add_row(ertc_val=tok)
            return "break"

        entry.bind("<<Paste>>", _on_paste, add="+")
        entry.bind("<Control-v>", _on_paste, add="+")
        entry.bind("<Control-V>", _on_paste, add="+")

    def _paste_table(self):
        try:
            text = self.clipboard_get()
        except tk.TclError:
            messagebox.showinfo("Paste", "Clipboard is empty.")
            return
        pairs = self._parse_pairs(text)
        if not pairs:
            messagebox.showinfo("Paste", "No values detected.")
            return

        if self._rows:
            e0_o, e0_e, _ = self._rows[0]
            if not (e0_o.get().strip() or e0_e.get().strip()):
                e0_o.insert(0, pairs[0][0])
                e0_e.insert(0, pairs[0][1])
                pairs = pairs[1:]

        for o, e in pairs:
            self.add_row(order_val=o, ertc_val=e)

    def _paste_list_left(self):
        try:
            text = self.clipboard_get()
        except tk.TclError:
            messagebox.showinfo("Paste", "Clipboard is empty."); return
        tokens = [t.strip().strip("'\"") for t in self.SPLIT_RE.split(text.strip()) if t.strip()]
        if not tokens:
            messagebox.showinfo("Paste", "No values detected."); return
        first = self._rows[0][0] if self._rows else None
        if first and not (first.get() or "").strip():
            first.insert(0, tokens[0]); tokens = tokens[1:]
        for tok in tokens:
            self.add_row(order_val=tok)

    def _paste_list_right(self):
        try:
            text = self.clipboard_get()
        except tk.TclError:
            messagebox.showinfo("Paste", "Clipboard is empty."); return
        tokens = [t.strip().strip("'\"") for t in self.SPLIT_RE.split(text.strip()) if t.strip()]
        if not tokens:
            messagebox.showinfo("Paste", "No values detected."); return
        first = self._rows[0][1] if self._rows else None
        if first and not (first.get() or "").strip():
            first.insert(0, tokens[0]); tokens = tokens[1:]
        for tok in tokens:
            self.add_row(ertc_val=tok)

    def add_row(self, order_val: str = "", ertc_val: str = ""):
        r = len(self._rows)
        rowf = ttk.Frame(self.rows_wrap)
        rowf.grid(row=r, column=0, sticky="ew", pady=2)
        rowf.columnconfigure(0, weight=1)

        e_order = ttk.Entry(rowf, width=28)
        e_order.grid(row=0, column=0, sticky="w")
        e_ertc = ttk.Entry(rowf, width=30)
        e_ertc.grid(row=0, column=1, sticky="w", padx=(8, 0))

        if order_val: e_order.insert(0, order_val)
        if ertc_val: e_ertc.insert(0, ertc_val)

        self._attach_paste_binds(e_order, side="left")
        self._attach_paste_binds(e_ertc, side="right")

        def _remove():
            try:
                rowf.destroy()
                self._rows.remove((e_order, e_ertc, btn_rm))
                for i, (eo, ee, br) in enumerate(self._rows):
                    eo.master.grid_configure(row=i)
            except Exception:
                pass

        btn_rm = ttk.Button(rowf, text="Remove", command=_remove)
        btn_rm.grid(row=0, column=2, padx=(8, 0))

        self._rows.append((e_order, e_ertc, btn_rm))

    def get_rows(self) -> list[tuple[str, str]]:
        out = []
        for e_order, e_ertc, _ in self._rows:
            o = (e_order.get() or "").strip()
            e = (e_ertc.get() or "").strip()
            if o or e:
                out.append((o, e))
        return out