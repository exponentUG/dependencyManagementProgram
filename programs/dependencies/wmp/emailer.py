# programs/dependencies/wmp/emailer.py
from __future__ import annotations

import os
import threading
import queue
from typing import List
from routers.emailRouter import router

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinter.constants import EW, NSEW

import pandas as pd

from core.base import ToolView, FONT_H2  # assumes these exist in your project

# Optional: COM init if you later use Outlook; we keep it harmless here.
try:
    import pythoncom  # type: ignore
except Exception:
    pythoncom = None

SHEET_NAME = "Tracker_49H"
HEADER_ROW_INDEX = 0  # Excel row 16 => pandas header=15

WMP_EMAIL_CATEGORIES = [
    "Permit | Need Click Date for Extension",
    "Permit | Confirm Permit is Approved/Permit Not Needed (Combined email to Brett)",
    "Permit | Request for Extension/Submitted Over 45 Days (Combined email to Brett)",
    "DS73 | Task Closure Request"
]

class WmpEmailer(ToolView):
    """
    Left side:
      - Workbook path (Entry + Browse)
      - "Email categories" label
      - Two checkboxes for categories
      - Create Emails / Cancel buttons + progress + status

    Right side:
      - Placeholder scrollable panel (we'll flesh this out later)

    Workflow for Create Emails:
      1) Read the Excel file: sheet = "Dependency Tracker", header row = 16.
      2) Build a single DataFrame 'df' (dtype=str).
      3) For each selected category, call self._route(df, category).
         (This is a hook you can wire to your own router implementation.)
    """

    def __init__(self, parent, program_name: str, tool_name: str):
        super().__init__(parent, program_name, tool_name)

        # Layout growth
        self.columnconfigure(0, weight=1)
        self.rowconfigure(3, weight=1)

        # === Split: LEFT (30%), RIGHT (70%) ===
        pw = ttk.PanedWindow(self, orient="horizontal")
        pw.grid(row=3, column=0, sticky="nsew", padx=16, pady=8)

        self.left = ttk.Frame(pw)
        self.right = ttk.Frame(pw)

        pw.add(self.left, weight=3)
        pw.add(self.right, weight=7)

        # Initial 30/70 sash position
        def _set_initial_split():
            pw.update_idletasks()
            total = pw.winfo_width() or self.winfo_width()
            if total > 1:
                pw.sashpos(0, int(total * 0.30))

        self.after(50, _set_initial_split)

        # ------------------------
        # LEFT: controls
        # ------------------------
        self.left.columnconfigure(1, weight=1)

        # Row 1: File path + Browse
        path_row = ttk.Frame(self.left)
        path_row.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 8))
        path_row.columnconfigure(1, weight=1)

        ttk.Label(path_row, text="Workbook path:").grid(row=0, column=0, sticky="w", padx=(0, 8))
        self.path_var = tk.StringVar()
        ttk.Entry(path_row, textvariable=self.path_var).grid(row=0, column=1, sticky="ew")
        ttk.Button(path_row, text="Browse…", command=self._browse).grid(row=0, column=2, padx=(8, 0))

        # Row 2: Categories label
        cats = ttk.Frame(self.left)
        cats.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(4, 8))
        ttk.Label(cats, text="Email categories:", font=FONT_H2).grid(row=0, column=0, sticky="w", pady=(0, 8))

        # Row 3: Category checkboxes (two for now)
        self.category_vars: dict[str, tk.BooleanVar] = {}
        for i, name in enumerate(WMP_EMAIL_CATEGORIES, start=1):
            var = tk.BooleanVar(value=(i == 1))  # preselect first
            ttk.Checkbutton(cats, text=name, variable=var).grid(row=i, column=0, sticky="w", pady=2)
            self.category_vars[name] = var

        # Row 4: Create / Cancel + Progress + Status
        actions = ttk.Frame(self.left)
        actions.grid(row=2, column=0, sticky="w", pady=(8, 0))

        self.btn_create = ttk.Button(actions, text="Create Emails", command=self._create_emails)
        self.btn_create.grid(row=0, column=0, sticky="w")

        self.btn_cancel = ttk.Button(actions, text="Cancel", command=self._cancel_email_job, state="disabled")
        self.btn_cancel.grid(row=0, column=1, padx=(8, 0))

        self.progress = ttk.Progressbar(actions, mode="determinate", length=260)
        self.progress.grid(row=1, column=0, columnspan=3, sticky="w", pady=(6, 0))

        self.status_var = tk.StringVar(value="")
        ttk.Label(actions, textvariable=self.status_var).grid(row=2, column=0, columnspan=3, sticky="w")

        # Worker plumbing
        self._worker: threading.Thread | None = None
        self._q: queue.Queue = queue.Queue()
        self._cancel_evt = threading.Event()

        # keep spacing tidy if left grows tall
        self.left.rowconfigure(99, weight=1)

        # ------------------------
        # RIGHT: placeholder (scrollable)
        # ------------------------
        self.right.columnconfigure(0, weight=1)
        self.right.rowconfigure(0, weight=1)

        self._canvas_wrap = ttk.Frame(self.right)
        self._canvas_wrap.grid(row=0, column=0, sticky="nsew")
        self._canvas_wrap.columnconfigure(0, weight=1)
        self._canvas_wrap.rowconfigure(0, weight=1)

        self._cv = tk.Canvas(self._canvas_wrap, highlightthickness=0)
        vsb = ttk.Scrollbar(self._canvas_wrap, orient="vertical", command=self._cv.yview)
        self._cv.configure(yscrollcommand=vsb.set)
        self._cv.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")

        self.right_host = ttk.Frame(self._cv)
        self._cv_window = self._cv.create_window((0, 0), window=self.right_host, anchor="nw")

        # keep scrollregion/width in sync
        self.right_host.bind(
            "<Configure>",
            lambda e: (
                self._cv.configure(scrollregion=self._cv.bbox("all")),
                self._cv.itemconfigure(self._cv_window, width=self._cv.winfo_width()),
            ),
        )
        self._cv.bind("<Configure>", lambda e: self._cv.itemconfigure(self._cv_window, width=e.width))

        # Placeholder content
        ttk.Label(self.right_host, text="(Preview & sanity panels coming soon)", padding=(8, 8)).grid(
            row=0, column=0, sticky="w"
        )

    # --------------------- UI helpers ---------------------

    def _browse(self):
        path = filedialog.askopenfilename(
            title="Select WMP Excel Workbook",
            filetypes=[("Excel files", "*.xlsx;*.xlsm;*.xls"), ("All files", "*.*")]
        )
        if path:
            self.path_var.set(path)

    def _selected_categories(self) -> List[str]:
        return [name for name, var in self.category_vars.items() if var.get()]

    # --------------------- Email creation flow ---------------------

    def _create_emails(self):
        # Prevent double-run
        if self._worker and self._worker.is_alive():
            messagebox.showinfo("Create Emails", "Email generation is already running.")
            return

        path = (self.path_var.get() or "").strip()
        cats = self._selected_categories()

        if not path:
            messagebox.showwarning("Workbook", "Please select a workbook first.")
            return
        if not os.path.isfile(path):
            messagebox.showerror("Workbook", f"File not found:\n{path}")
            return
        if not cats:
            messagebox.showinfo("Email categories", "Select at least one category.")
            return

        # UI state
        self.btn_create.configure(state="disabled")
        self.btn_cancel.configure(state="normal")
        self.progress.configure(mode="determinate", maximum=len(cats), value=0)
        self.status_var.set("Starting…")
        self._cancel_evt.clear()

        # Launch worker
        self._worker = threading.Thread(target=self._email_worker, args=(path, cats), daemon=True)
        self._worker.start()

        # Pump queue
        self.after(100, self._pump_queue)

    def _email_worker(self, path: str, categories: List[str]):
        # COM init (safe if Outlook is used later)
        if pythoncom:
            try:
                pythoncom.CoInitialize()
            except Exception:
                pass

        try:
            # # 1) Load DataFrame ONCE
            # try:
            #     df = pd.read_excel(path, sheet_name=SHEET_NAME, header=HEADER_ROW_INDEX, dtype=str)
            # except Exception as e:
            #     self._q.put(("error_fatal", f"Could not read '{SHEET_NAME}' from file:\n{path}\n\n{e}"))
            #     return

            # if df.empty:
            #     self._q.put(("error_fatal", f"Sheet '{SHEET_NAME}' appears to have no data below header row 16."))
            #     return

            # df = df.dropna(how="all")  # clean fully empty rows

            total = len(categories)
            for i, cat in enumerate(categories, start=1):
                if self._cancel_evt.is_set():
                    self._q.put(("cancelled",))
                    return

                self._q.put(("progress", i - 1, total, f"Processing: {cat}"))
                try:
                    router("wmp", cat, path)
                except NotImplementedError:
                    self._q.put(("error", f"Router not implemented. Add your routing logic in self._route(df, category)."))
                except Exception as e:
                    self._q.put(("error", f"{cat}: {e!s}"))

                self._q.put(("progress", i, total, f"Done: {cat}"))

            self._q.put(("done", total))
        finally:
            if pythoncom:
                try:
                    pythoncom.CoUninitialize()
                except Exception:
                    pass

    def _pump_queue(self):
        """Process worker → UI messages."""
        try:
            while True:
                msg = self._q.get_nowait()
                kind = msg[0]

                if kind == "progress":
                    _, i, total, text = msg
                    self.progress.configure(value=i)
                    self.status_var.set(text)

                elif kind == "error":
                    _, text = msg
                    messagebox.showerror("Emailer", text)

                elif kind == "error_fatal":
                    _, text = msg
                    messagebox.showerror("Emailer", text)
                    # reset UI
                    self.btn_create.configure(state="normal")
                    self.btn_cancel.configure(state="disabled")
                    self.progress.configure(value=0)
                    self.status_var.set("")
                    return

                elif kind == "cancelled":
                    self.status_var.set("Cancelled.")
                    self.btn_create.configure(state="normal")
                    self.btn_cancel.configure(state="disabled")
                    return

                elif kind == "done":
                    _, total = msg
                    self.progress.configure(value=total)
                    self.status_var.set(f"Done. Generated for {total} categories.")
                    self.btn_create.configure(state="normal")
                    self.btn_cancel.configure(state="disabled")
                    return

        except queue.Empty:
            # keep pumping
            self.after(100, self._pump_queue)

    def _cancel_email_job(self):
        if self._worker and self._worker.is_alive():
            self._cancel_evt.set()
