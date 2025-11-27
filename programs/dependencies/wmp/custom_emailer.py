# programs/dependencies/wmp/custom_emailer.py
from __future__ import annotations
import tkinter as tk
from tkinter import ttk, messagebox
import threading, queue

from core.base import ToolView, FONT_H2

# COM (Outlook) imports guarded so the UI still loads if not present
try:
    import pythoncom  # to initialize COM in worker thread
except ImportError:
    pythoncom = None
try:
    import win32com.client as win32
except Exception:
    win32 = None

# Import helpers
from helpers.wmp_custom_emailer.common import (
    OrderList, OrderListTwoCol,
)
from helpers.wmp_custom_emailer import pc21 as PC21
from helpers.wmp_custom_emailer import jp_intent as JP
from helpers.wmp_custom_emailer import ds28 as DS28
from helpers.wmp_custom_emailer import ap10 as AP10  # NEW
from helpers.wmp_custom_emailer import ds73 as DS73  # NEW

TEMPLATE_PC21 = PC21.TEMPLATE_NAME
TEMPLATE_JP_INTENT = JP.TEMPLATE_NAME
TEMPLATE_DS28 = DS28.TEMPLATE_NAME
TEMPLATE_AP10 = AP10.TEMPLATE_NAME
TEMPLATE_DS73 = DS73.TEMPLATE_NAME  # NEW

TEMPLATE_NAMES = [TEMPLATE_PC21, TEMPLATE_JP_INTENT, TEMPLATE_DS28, TEMPLATE_AP10, TEMPLATE_DS73]  # UPDATED

class WmpCustomEmailer(ToolView):
    """
    Thin UI shell using helper modules for:
      - defaults
      - labels
      - HTML building
      - reusable row widgets

    Behavior:
      - PC21: TWO-COLUMN (Order | Anticipated ERTC Out Date) + Missing AOD section
      - Joint Pole: ONE-COLUMN (Orders only)
      - DS28 Closure: ONE-COLUMN (Orders only)
    """
    def __init__(self, parent, program_name, tool_name):
        super().__init__(parent, program_name, tool_name)

        self.columnconfigure(0, weight=1)
        row = 3

        # ==== Top bar ====
        top = ttk.Frame(self)
        top.grid(row=row, column=0, sticky="ew", padx=16, pady=(8, 6))
        top.columnconfigure(1, weight=1)

        ttk.Label(top, text="Template:", font=FONT_H2).grid(row=0, column=0, sticky="w", padx=(0, 10))
        self.template_var = tk.StringVar(value=TEMPLATE_NAMES[0])
        self.template_combo = ttk.Combobox(
            top, textvariable=self.template_var, values=TEMPLATE_NAMES, state="readonly", width=52
        )
        self.template_combo.grid(row=0, column=1, sticky="w")
        self.template_combo.bind("<<ComboboxSelected>>", lambda _e: self._apply_template())

        self.send_now_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(top, text="Send immediately (skip draft)", variable=self.send_now_var).grid(
            row=0, column=2, padx=(20, 10)
        )

        self.btn_send = ttk.Button(top, text="Send Email", command=self._send_email)
        self.btn_send.grid(row=0, column=3)

        # ==== Email fields ====
        row += 1
        fields = ttk.LabelFrame(self, text="Recipients & Subject")
        fields.grid(row=row, column=0, sticky="ew", padx=16, pady=(0, 8))
        for c in range(3):
            fields.columnconfigure(c, weight=1)

        ttk.Label(fields, text="To:").grid(row=0, column=0, sticky="w")
        self.to_var = tk.StringVar()
        ttk.Entry(fields, textvariable=self.to_var).grid(row=0, column=1, columnspan=2, sticky="ew", padx=(6, 0))

        ttk.Label(fields, text="CC:").grid(row=1, column=0, sticky="w", pady=(6, 0))
        self.cc_var = tk.StringVar()
        ttk.Entry(fields, textvariable=self.cc_var).grid(row=1, column=1, columnspan=2, sticky="ew", padx=(6, 0), pady=(6, 0))

        ttk.Label(fields, text="Subject:").grid(row=2, column=0, sticky="w", pady=(6, 0))
        self.subj_var = tk.StringVar()
        ttk.Entry(fields, textvariable=self.subj_var).grid(row=2, column=1, columnspan=2, sticky="ew", padx=(6, 0), pady=(6, 8))

        # ==== Section 1: Body (intro) ====
        row += 1
        self.intro_frame = ttk.LabelFrame(self, text="Body (intro)")
        self.intro_frame.grid(row=row, column=0, sticky="ew", padx=16, pady=(0, 8))
        self.intro_frame.columnconfigure(0, weight=1)
        self.body_intro = tk.Text(self.intro_frame, height=6, wrap="word")
        self.body_intro.grid(row=0, column=0, sticky="ew")

        # ==== Section 1: Orders (primary list) ====
        row += 1
        self.tbl1_frame = ttk.LabelFrame(self, text=PC21.LBL_PRIMARY)  # default (PC21)
        self.tbl1_frame.grid(row=row, column=0, sticky="ew", padx=16, pady=(0, 8))
        self.tbl1_frame.columnconfigure(0, weight=1)

        # Create BOTH primary widgets; toggle per template
        self.orders_primary_twocol = OrderListTwoCol(self.tbl1_frame)  # PC21
        self.orders_primary_twocol.grid(row=0, column=0, sticky="ew")

        self.orders_primary_onecol = OrderList(self.tbl1_frame)        # JP, DS28
        self.orders_primary_onecol.grid(row=0, column=0, sticky="ew")
        self.orders_primary_onecol.grid_remove()  # hidden by default

        # ==== Section 2: Body (missing AOD note) ====
        row += 1
        self.missing_body_frame = ttk.LabelFrame(self, text="Body (Missing Anticipated Out Dates)")
        self.missing_body_frame.grid(row=row, column=0, sticky="ew", padx=16, pady=(0, 8))
        self.missing_body_frame.columnconfigure(0, weight=1)
        self.body_missing = tk.Text(self.missing_body_frame, height=4, wrap="word")
        self.body_missing.grid(row=0, column=0, sticky="ew")

        # ==== Section 2: Orders (missing AODs) ====
        row += 1
        self.tbl2_frame = ttk.LabelFrame(self, text=PC21.LBL_SECONDARY)
        self.tbl2_frame.grid(row=row, column=0, sticky="ew", padx=16, pady=(0, 8))
        self.tbl2_frame.columnconfigure(0, weight=1)
        self.orders_secondary = OrderList(self.tbl2_frame)
        self.orders_secondary.grid(row=0, column=0, sticky="ew")

        # ==== Post-table text ====
        row += 1
        self.post_frame = ttk.LabelFrame(self, text="Post Table")
        self.post_frame.grid(row=row, column=0, sticky="ew", padx=16, pady=(0, 8))
        self.post_frame.columnconfigure(0, weight=1)
        self.post_text = tk.Text(self.post_frame, height=4, wrap="word")
        self.post_text.grid(row=0, column=0, sticky="ew")

        # ==== Status area ====
        row += 1
        status = ttk.Frame(self)
        status.grid(row=row, column=0, sticky="ew", padx=16, pady=(0, 12))
        status.columnconfigure(0, weight=1)
        self.progress = ttk.Progressbar(status, mode="indeterminate", length=220)
        self.progress.grid(row=0, column=0, sticky="w")
        self.status_var = tk.StringVar(value="")
        ttk.Label(status, textvariable=self.status_var).grid(row=0, column=1, sticky="w", padx=(10, 0))

        # worker plumbing
        self._q = queue.Queue()
        self._worker: threading.Thread | None = None

        # Load defaults for current template
        self._apply_template()

    # ---------------- Template Handling ----------------
    def _apply_template(self):
        name = self.template_var.get()
        if name == TEMPLATE_PC21:
            # Show section 2
            self._show_missing_sections(True)

            # Defaults
            self.to_var.set(PC21.DEFAULTS["to"])
            self.cc_var.set(PC21.DEFAULTS["cc"])
            self.subj_var.set(PC21.DEFAULTS["subject"])
            self._set_text(self.body_intro, PC21.DEFAULTS["body_intro"])
            self._set_text(self.body_missing, PC21.DEFAULTS["body_missing"])
            self._set_text(self.post_text, PC21.DEFAULTS["post_text"])

            # Labels
            self.tbl1_frame.configure(text=PC21.LBL_PRIMARY)
            self.tbl2_frame.configure(text=PC21.LBL_SECONDARY)

            # Primary table: TWO-COL visible, ONE-COL hidden
            self.orders_primary_onecol.grid_remove()
            self.orders_primary_twocol.grid()

        elif name == TEMPLATE_JP_INTENT:
            # Hide section 2
            self._show_missing_sections(False)

            # Defaults
            self.to_var.set(JP.DEFAULTS["to"])
            self.cc_var.set(JP.DEFAULTS["cc"])
            self.subj_var.set(JP.DEFAULTS["subject"])
            self._set_text(self.body_intro, JP.DEFAULTS["body_intro"])
            self._set_text(self.body_missing, "")  # hidden anyway
            self._set_text(self.post_text, JP.DEFAULTS["post_text"])

            # Labels
            self.tbl1_frame.configure(text=JP.LBL_PRIMARY)

            # Primary table: ONE-COL visible, TWO-COL hidden
            self.orders_primary_twocol.grid_remove()
            self.orders_primary_onecol.grid()

        elif name == TEMPLATE_DS28:
            # Hide section 2
            self._show_missing_sections(False)

            # Defaults
            self.to_var.set(DS28.DEFAULTS["to"])
            self.cc_var.set(DS28.DEFAULTS["cc"])
            self.subj_var.set(DS28.DEFAULTS["subject"])
            self._set_text(self.body_intro, DS28.DEFAULTS["body_intro"])
            self._set_text(self.body_missing, "")  # hidden
            self._set_text(self.post_text, DS28.DEFAULTS["post_text"])

            # Labels
            self.tbl1_frame.configure(text=DS28.LBL_PRIMARY)

            # Primary table: ONE-COL visible, TWO-COL hidden
            self.orders_primary_twocol.grid_remove()
            self.orders_primary_onecol.grid()

        elif name == TEMPLATE_AP10:
            # Hide section 2
            self._show_missing_sections(False)

            # Defaults
            self.to_var.set(AP10.DEFAULTS["to"])
            self.cc_var.set(AP10.DEFAULTS["cc"])
            self.subj_var.set(AP10.DEFAULTS["subject"])
            self._set_text(self.body_intro, AP10.DEFAULTS["body_intro"])
            self._set_text(self.body_missing, "")  # hidden
            self._set_text(self.post_text, AP10.DEFAULTS["post_text"])

            # Labels
            self.tbl1_frame.configure(text=AP10.LBL_PRIMARY)

            # Primary table: ONE-COL visible, TWO-COL hidden
            self.orders_primary_twocol.grid_remove()
            self.orders_primary_onecol.grid()

        elif name == TEMPLATE_DS73:
            # Hide section 2
            self._show_missing_sections(False)

            # Defaults
            self.to_var.set(DS73.DEFAULTS["to"])
            self.cc_var.set(DS73.DEFAULTS["cc"])
            self.subj_var.set(DS73.DEFAULTS["subject"])
            self._set_text(self.body_intro, DS73.DEFAULTS["body_intro"])
            self._set_text(self.body_missing, "")  # hidden
            self._set_text(self.post_text, DS73.DEFAULTS["post_text"])

            # Labels
            self.tbl1_frame.configure(text=DS73.LBL_PRIMARY)

            # Primary table: ONE-COL visible, TWO-COL hidden
            self.orders_primary_twocol.grid_remove()
            self.orders_primary_onecol.grid()

    def _show_missing_sections(self, show: bool):
        if show:
            if not self.missing_body_frame.winfo_ismapped():
                self.missing_body_frame.grid()
            if not self.tbl2_frame.winfo_ismapped():
                self.tbl2_frame.grid()
        else:
            self.missing_body_frame.grid_remove()
            self.tbl2_frame.grid_remove()

    def _set_text(self, widget: tk.Text, value: str):
        widget.delete("1.0", "end")
        widget.insert("1.0", value)

    # ---------------- Email Build/Send ----------------
    def _send_email(self):
        if self._worker and self._worker.is_alive():
            messagebox.showinfo("Email", "Email sending is already in progress.")
            return

        to = (self.to_var.get() or "").strip()
        cc = (self.cc_var.get() or "").strip()
        subject = (self.subj_var.get() or "").strip()
        intro = (self.body_intro.get("1.0", "end") or "").strip()
        missing = (self.body_missing.get("1.0", "end") or "").strip()
        post = (self.post_text.get("1.0", "end") or "").strip()

        tpl = self.template_var.get()

        # Determine which primary widget is visible and collect rows
        if self.orders_primary_twocol.winfo_ismapped():
            # PC21: two-column pairs
            orders_primary_pairs = self.orders_primary_twocol.get_rows()
            orders_primary_list = [o for (o, _e) in orders_primary_pairs if (o or "").strip()]
        else:
            # JP / DS28: one-column list
            orders_primary_pairs = []
            orders_primary_list = self.orders_primary_onecol.get_rows()

        orders_secondary = self.orders_secondary.get_rows() if self.missing_body_frame.winfo_ismapped() else []

        if not to:
            messagebox.showwarning("Recipients", "Please enter at least one recipient in To.")
            return
        if not subject:
            messagebox.showwarning("Subject", "Subject cannot be empty.")
            return

        if tpl == TEMPLATE_PC21:
            if not orders_primary_pairs and not orders_secondary:
                if not messagebox.askyesno("No Orders", "No orders entered in either table. Send anyway?", default="no"):
                    return
        else:
            if not orders_primary_list:
                if not messagebox.askyesno("No Orders", "No orders entered. Send anyway?", default="no"):
                    return

        # Disable UI bits while running
        self.btn_send.configure(state="disabled")
        self.progress.configure(mode="indeterminate")
        self.progress.start(20)
        self.status_var.set("Preparing email...")

        send_immediately = bool(self.send_now_var.get())

        self._worker = threading.Thread(
            target=self._worker_send,
            args=(to, cc, subject, intro, missing, post,
                  orders_primary_pairs, orders_primary_list, orders_secondary,
                  tpl, send_immediately),
            daemon=True
        )
        self._worker.start()
        self.after(100, self._pump_queue)

    def _worker_send(
        self, to: str, cc: str, subject: str,
        intro: str, missing: str, post: str,
        orders_primary_pairs: list[tuple[str, str]], orders_primary_list: list[str], orders_secondary: list[str],
        template_name: str, send_now: bool
    ):
        if pythoncom:
            try:
                pythoncom.CoInitialize()
            except Exception:
                pass
        try:
            if win32 is None:
                raise RuntimeError("Outlook COM library (pywin32) not available.")

            if template_name == TEMPLATE_PC21:
                html = PC21.build_html(intro, missing, post, orders_primary_pairs, orders_secondary)
            elif template_name == TEMPLATE_JP_INTENT:
                html = JP.build_html(intro, post, orders_primary_list)
            elif template_name == TEMPLATE_DS28:
                html = DS28.build_html(intro, post, orders_primary_list)
            elif template_name == TEMPLATE_AP10:
                html = AP10.build_html(intro, post, orders_primary_list)
            else:  # TEMPLATE_DS73
                html = DS73.build_html(intro, post, orders_primary_list)

            outlook = win32.Dispatch("Outlook.Application")
            mail = outlook.CreateItem(0)
            mail.To = to
            mail.CC = cc
            mail.Subject = subject
            mail.HTMLBody = html

            if send_now:
                mail.Send()
                self._q.put(("done", "Email sent."))
            else:
                mail.Display(True)
                self._q.put(("done", "Draft opened in Outlook."))
        except Exception as e:
            self._q.put(("error", str(e)))
        finally:
            if pythoncom:
                try:
                    pythoncom.CoUninitialize()
                except Exception:
                    pass

    def _pump_queue(self):
        try:
            while True:
                k, *rest = self._q.get_nowait()
                if k == "done":
                    msg = rest[0] if rest else "Done."
                    self.status_var.set(msg)
                    self.progress.stop()
                    self.btn_send.configure(state="normal")
                    return
                elif k == "error":
                    msg = rest[0] if rest else "Failed."
                    self.status_var.set(f"Error: {msg}")
                    self.progress.stop()
                    self.btn_send.configure(state="normal")
                    messagebox.showerror("Email failed", msg)
                    return
        except queue.Empty:
            pass

        if self._worker and self._worker.is_alive():
            self.after(120, self._pump_queue)
        else:
            self.progress.stop()
            self.btn_send.configure(state="normal")