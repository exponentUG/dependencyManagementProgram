from __future__ import annotations
import os
import threading
import sqlite3
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import re as _re
import csv
import io
import pandas as pd

from helpers.wmp_tracker_builder.pull_sap_data import pull_sap_data
from helpers.wmp_tracker_builder.pull_epw_data import pull_epw_data
from helpers.wmp_tracker_builder.pull_land_data import pull_land_data
from helpers.wmp_tracker_builder.update_trackers import build_sap_tracker_initial
from helpers.wmp_tracker_builder.manual_inputs import save_pasted_pairs, save_from_tracker_excel

from services.db.poles_rfc_db import default_db_path

from core.base import ToolView, FONT_H1
from helpers.poles_tracker_builder.logic import (
    run_import_and_updates,
    export_order_list_to_excel,
    today_strings,
)

# table builder for Environment view
from helpers.wmp_tracker_builder.table_builders.environment_table import get_environment_table
from helpers.wmp_tracker_builder.table_builders.misctsk_table import get_misc_tsk_table
from helpers.wmp_tracker_builder.table_builders.joint_pole_table import get_joint_pole_table
from helpers.wmp_tracker_builder.table_builders.permit_table import get_permit_table
# near the other imports
from helpers.wmp_tracker_builder.table_builders.land_table import get_land_table
from helpers.wmp_tracker_builder.table_builders.faa_table import get_faa_table
from helpers.wmp_tracker_builder.table_builders.master_table import get_master_table

from helpers.sap_reports.wmp.task_management_report import get_task_management_report

TRACKER_MODES = [
    "Master",
    "Permit",
    "Land",
    "FAA",
    "Environment",
    "Joint Pole",
    "MiscTSK",
]

class Poles_Tracker_Builder_RFC(ToolView):
    def __init__(self, master=None, **kwargs):
        super().__init__(master, **kwargs)

        # ---- Row 3: Step title
        ttk.Label(self, text="Step 1: Upload latest copy of MPP and extract list of orders to track").grid(
            row=3, column=0, columnspan=6, sticky="w", pady=(8, 4), padx=16
        )

        # ---- Row 4: "MPP Database:" [Entry] [Browse] [Generate] (padding aligned with rows 6–8)
        fr4 = ttk.Frame(self)
        fr4.grid(row=4, column=0, columnspan=6, sticky="ew", padx=16, pady=4)
        fr4.columnconfigure(1, weight=1)

        ttk.Label(fr4, text="MPP Database:").grid(row=0, column=0, sticky="w", padx=(0, 8))
        self.path_var = tk.StringVar()
        self.path_entry = ttk.Entry(fr4, textvariable=self.path_var, width=70)
        self.path_entry.grid(row=0, column=1, sticky="ew", padx=(0, 8))

        self.browse_btn = ttk.Button(fr4, text="Browse", command=self._browse_file)
        self.browse_btn.grid(row=0, column=2, sticky="w", padx=(0, 8))

        self.generate_btn = ttk.Button(fr4, text="Generate Order List", command=self._on_generate, state="disabled")
        self.generate_btn.grid(row=0, column=3, sticky="w")

        self.path_var.trace_add("write", lambda *_: self._update_generate_state())

        # ---- Row 5: Step 2 label
        ttk.Label(self, text="Step 2: Update SAP Data, EPW Data, and Land Data").grid(
            row=5, column=0, columnspan=6, sticky="w", padx=16, pady=(16, 2)
        )

        # ---- Rows 6–8: pickers (full-width like row 4)
        self.var_sap = tk.StringVar();  self.var_sap.trace_add("write", lambda *_: self._update_step2_state())
        self.var_epw = tk.StringVar();  self.var_epw.trace_add("write", lambda *_: self._update_step2_state())
        self.var_land = tk.StringVar(); self.var_land.trace_add("write", lambda *_: self._update_step2_state())

        # --- Row 6 (SAP) custom so we can add Extract SAP Data button ---
        fr6 = ttk.Frame(self)
        fr6.grid(row=6, column=0, columnspan=6, sticky="ew", padx=16, pady=4)
        fr6.columnconfigure(1, weight=1)

        ttk.Label(fr6, text="SAP Data").grid(row=0, column=0, sticky="w", padx=(0, 8))
        ttk.Entry(fr6, textvariable=self.var_sap).grid(row=0, column=1, sticky="ew", padx=(0, 8))
        ttk.Button(fr6, text="Browse…", command=lambda: self._browse_excel(self.var_sap)).grid(
            row=0, column=2, sticky="w", padx=(0, 8)
        )
        ttk.Button(fr6, text="Extract SAP Data", command=self._on_extract_sap_data).grid(
            row=0, column=3, sticky="w"
        )

        # --- helper for rows 7–8 ---
        def make_row(r: int, label: str, var: tk.StringVar):
            fr = ttk.Frame(self)
            fr.grid(row=r, column=0, columnspan=6, sticky="ew", padx=16, pady=4)
            fr.columnconfigure(1, weight=1)
            ttk.Label(fr, text=label).grid(row=0, column=0, sticky="w", padx=(0, 8))
            ttk.Entry(fr, textvariable=var).grid(row=0, column=1, sticky="ew", padx=(0, 8))
            ttk.Button(fr, text="Browse…", command=lambda: self._browse_excel(var)).grid(
                row=0, column=2, sticky="w"
            )

        make_row(7, "EPW Data",  self.var_epw)
        make_row(8, "Land Data", self.var_land)

        # ---- Row 9: Extract Data (left aligned)
        fr_btn = ttk.Frame(self)
        fr_btn.grid(row=9, column=0, columnspan=6, sticky="w", padx=16, pady=(4, 6))
        self.btn_extract = ttk.Button(fr_btn, text="Extract Data", command=self._on_extract_step2, state="disabled")
        self.btn_extract.grid(row=0, column=0)

        # ---- Row 10: Tracker Tools heading
        ttk.Label(self, text="Tracker Tools", font=FONT_H1).grid(
            row=10, column=0, columnspan=6, sticky="w", padx=16, pady=(8, 4)
        )

        # ---- Row 11: Tools (left) + Mode dropdown + Refresh (right)
        fr_tools = ttk.Frame(self)
        fr_tools.grid(row=11, column=0, columnspan=6, sticky="ew", padx=16, pady=(0, 6))
        fr_tools.columnconfigure(0, weight=0)  # left buttons
        fr_tools.columnconfigure(1, weight=1)  # spacer
        fr_tools.columnconfigure(2, weight=0)  # right controls

        # left button group
        left_fr = ttk.Frame(fr_tools)
        left_fr.grid(row=0, column=0, sticky="w")
        self.btn_update_trackers = ttk.Button(left_fr, text="Update Trackers", command=self._on_update_trackers)
        self.btn_update_trackers.grid(row=0, column=0, padx=(0, 8))
        self.btn_manual_inputs = ttk.Button(left_fr, text="Manual Inputs", command=self._open_manual_inputs)
        self.btn_manual_inputs.grid(row=0, column=1, padx=(0, 8))
        self.btn_export_excel = ttk.Button(left_fr, text="Export to Excel", command=self._on_export_excel)
        self.btn_export_excel.grid(row=0, column=2, padx=(0, 8))

        # right view/refresh group
        right_fr = ttk.Frame(fr_tools)
        right_fr.grid(row=0, column=2, sticky="e")
        ttk.Label(right_fr, text="View:").grid(row=0, column=0, sticky="e", padx=(0, 8))
        self.mode_var = tk.StringVar(value="Environment")
        self.mode_dd = ttk.Combobox(
            right_fr, textvariable=self.mode_var, state="readonly",
            values=TRACKER_MODES, width=16
        )
        self.mode_dd.grid(row=0, column=1, sticky="e", padx=(0, 8))
        self.mode_dd.bind("<<ComboboxSelected>>", lambda _e: self._refresh_table())

        self.btn_refresh_table = ttk.Button(right_fr, text="Refresh", command=self._refresh_table)
        self.btn_refresh_table.grid(row=0, column=2, sticky="e")

        # ---- Row 12: Order Count (between tools and table)
        fr_count = ttk.Frame(self)
        fr_count.grid(row=12, column=0, columnspan=6, sticky="ew", padx=16, pady=(0, 6))
        self.count_var = tk.StringVar(value="Order Count: —")
        ttk.Label(fr_count, textvariable=self.count_var).grid(row=0, column=0, sticky="w")

        # ---- Row 13: Table (Treeview) with vertical + horizontal scrollbars
        lf = ttk.LabelFrame(self, text="Tracker View")
        lf.grid(row=13, column=0, columnspan=6, sticky="nsew", padx=16, pady=(0, 12))

        self.tree = ttk.Treeview(lf, columns=("message",), show="headings", height=16)
        self.tree.heading("message", text="Message")
        self.tree.column("message", width=960, anchor="w")

        vsb = ttk.Scrollbar(lf, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(lf, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        lf.rowconfigure(0, weight=1)
        lf.columnconfigure(0, weight=1)

        # Let the table area grow
        self.rowconfigure(13, weight=1)
        self.columnconfigure(0, weight=1)

        # initial load
        self._refresh_table()

        # Grid stretch (middle columns)
        self.columnconfigure(1, weight=1)
        self.columnconfigure(2, weight=1)
        self.columnconfigure(3, weight=1)

    # ---------- Performance helper ----------
    def _on_export_excel(self):
        """Export all tracker views to a single Excel workbook (7 sheets)."""
        db_path = default_db_path()
        if not os.path.isfile(db_path):
            messagebox.showerror("Missing DB", f"Database not found:\n{db_path}\n\nRun Extract/Generate first.")
            return

        # Build the sheet mapping (sheet name -> table getter)
        sheets = [
            ("Master",      get_master_table),
            ("Permit",      get_permit_table),
            ("Land",        get_land_table),
            ("FAA",         get_faa_table),
            ("Environment", get_environment_table),
            ("Joint Pole",  get_joint_pole_table),
            ("MiscTSK",     get_misc_tsk_table),
        ]

        # File name with today's date (use the same util you already use)
        mdy_slash, mdy_dash = today_strings()
        default_name = f"WMP Tracker - {mdy_dash}.xlsx"

        save_path = filedialog.asksaveasfilename(
            title=f'Exporting WMP Tracker - {mdy_slash}',
            defaultextension=".xlsx",
            initialfile=default_name,
            filetypes=[("Excel Workbook", "*.xlsx")],
        )
        if not save_path:
            return  # user canceled

        # Fetch each view and write to its sheet
        try:
            with pd.ExcelWriter(save_path, engine="xlsxwriter") as writer:
                for sheet_name, getter in sheets:
                    try:
                        cols, rows = getter(db_path)
                    except Exception as e:
                        # If a table builder fails, put the error in the sheet
                        cols, rows = ["Message"], [(f"Error loading {sheet_name}: {type(e).__name__}: {e}",)]

                    if not cols:
                        # Match UI behavior: when no data, include a friendly message
                        df = pd.DataFrame([{"Message": "No data yet. Run 'Extract Data' and 'Update Trackers' first."}])
                    else:
                        df = pd.DataFrame(rows, columns=cols)

                    # Write the sheet
                    df.to_excel(writer, sheet_name=sheet_name, index=False)

            messagebox.showinfo("Export Complete", f"Saved to:\n{save_path}")
        except Exception as e:
            messagebox.showerror("Export Failed", f"{type(e).__name__}: {e}")

    
    def _ensure_perf_indexes(self, db_path: str):
        """Create helpful indexes and set WAL/NORMAL pragmas. Safe to call repeatedly."""
        try:
            with sqlite3.connect(db_path) as conn:
                cur = conn.cursor()
                cur.executescript("""
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=NORMAL;

                CREATE INDEX IF NOT EXISTS idx_mpp_order        ON mpp_data("Order");
                CREATE INDEX IF NOT EXISTS idx_mpp_project_year ON mpp_data("Project Reporting Year");
                CREATE INDEX IF NOT EXISTS idx_mpp_mat          ON mpp_data("MAT");
                CREATE INDEX IF NOT EXISTS idx_mpp_program      ON mpp_data("Program");
                CREATE INDEX IF NOT EXISTS idx_mpp_subcat       ON mpp_data("Sub-Category");
                CREATE INDEX IF NOT EXISTS idx_mpp_div          ON mpp_data("Div");
                CREATE INDEX IF NOT EXISTS idx_mpp_region       ON mpp_data("Region");
                CREATE INDEX IF NOT EXISTS idx_mpp_click_start  ON mpp_data("CLICK Start Date");
                CREATE INDEX IF NOT EXISTS idx_mpp_click_end    ON mpp_data("CLICK End Date");
                CREATE INDEX IF NOT EXISTS idx_mpp_wpd          ON mpp_data("Work Plan Date");

                CREATE INDEX IF NOT EXISTS idx_saptracker_order ON sap_tracker("Order");
                CREATE INDEX IF NOT EXISTS idx_envtracker_order ON environment_tracker("Order");
                CREATE INDEX IF NOT EXISTS idx_opendep_order    ON open_dependencies("Order");
                CREATE INDEX IF NOT EXISTS idx_epw_ordernum     ON epw_data("Order Number");
                CREATE INDEX IF NOT EXISTS idx_manual_order     ON manual_tracker("Order");
                """)
                conn.commit()
        except Exception:
            # Never block the UI for indexing errors
            pass

    # ---------- Table helpers ----------
    def _set_table_columns(self, columns: list[str]):
        for col in self.tree["columns"]:
            try:
                self.tree.heading(col, text="")
            except Exception:
                pass
        self.tree["columns"] = columns
        for col in columns:
            self.tree.heading(col, text=col)
            base = 140 if col in ("Order", "Div", "Region", "DS11", "PC21") else 160
            if col in ("Environment Notes", "Environment Update", "Action"):
                base = 320
            if col in ("WPD", "CLICK Start Date", "CLICK End Date",
                       "Environment Anticipated Out Date"):
                base = 180
            self.tree.column(col, width=base, minwidth=80, anchor="w")

    def _clear_table_rows(self):
        for iid in self.tree.get_children(""):
            self.tree.delete(iid)

    def _populate_table(self, columns: list[str], rows: list[tuple]):
        self._set_table_columns(columns)
        self._clear_table_rows()
        if not rows:
            if columns and columns[0] != "Message":
                self._set_table_columns(["Message"])
            self.tree.insert("", "end", values=("No data to display.",))
            return
        for r in rows:
            r2 = tuple(list(r)[:len(columns)] + [""] * max(0, len(columns) - len(r)))
            self.tree.insert("", "end", values=r2)

    def _refresh_table(self):
        mode = self.mode_var.get().strip()
        db_path = default_db_path()

        if mode == "Environment":
            try:
                columns, rows = get_environment_table(db_path)
            except Exception as e:
                self._populate_table(["Message"], [(f"Error loading Environment view: {type(e).__name__}: {e}",)])
                self.count_var.set("Order Count: —")
                return

        elif mode == "MiscTSK":
            try:
                columns, rows = get_misc_tsk_table(db_path)
            except Exception as e:
                self._populate_table(["Message"], [(f"Error loading MiscTSK view: {type(e).__name__}: {e}",)])
                self.count_var.set("Order Count: —")
                return
        
        elif mode == "Joint Pole":
            try:
                columns, rows = get_joint_pole_table(db_path)
            except Exception as e:
                self._populate_table(["Message"], [(f"Error loading Joint Pole view: {type(e).__name__}: {e}",)])
                self.count_var.set("Order Count: —")
                return

        elif mode == "Permit":
            try:
                columns, rows = get_permit_table(db_path)
            except Exception as e:
                self._populate_table(["Message"], [(f"Error loading Permit view: {type(e).__name__}: {e}",)])
                self.count_var.set("Order Count: —")
                return
            
        elif mode == "Land":
            try:
                columns, rows = get_land_table(db_path)
            except Exception as e:
                self._populate_table(["Message"], [(f"Error loading Land view: {type(e).__name__}: {e}",)])
                self.count_var.set("Order Count: —")
                return

        elif mode == "FAA":
            try:
                columns, rows = get_faa_table(db_path)
            except Exception as e:
                self._populate_table(["Message"], [(f"Error loading FAA view: {type(e).__name__}: {e}",)])
                self.count_var.set("Order Count: —")
                return

        elif mode == "Master":
            try:
                columns, rows = get_master_table(db_path)
            except Exception as e:
                self._populate_table(["Message"], [(f"Error loading Master view: {type(e).__name__}: {e}",)])
                self.count_var.set("Order Count: —")
                return

        else:
            self._populate_table(["Message"], [("In development...",)])
            self.count_var.set("Order Count: —")
            return

        if not columns:
            self._populate_table(["Message"], [("No data yet. Run 'Extract Data' and 'Update Trackers' first.",)])
            self.count_var.set("Order Count: 0")
        else:
            self._populate_table(columns, rows)
            self.count_var.set(f"Order Count: {len(rows):,}")

    # ----- Step-2 helpers
    def _on_extract_sap_data(self):
        """
        Button handler for 'Extract SAP Data'.
        For now it calls get_task_management_report(), which:
          - asks SAP details
          - pulls Orders from order_tracking_list
          - prints both
        """
        try:
            get_task_management_report()
        except Exception as e:
            messagebox.showerror("Extract SAP Data Failed", f"{type(e).__name__}: {e}")

    
    def _browse_excel(self, var: tk.StringVar):
        path = filedialog.askopenfilename(
            title="Select Excel file",
            filetypes=[("Excel files", "*.xlsx *.xlsm *.xlsb *.xls"), ("All files", "*.*")]
        )
        if path:
            var.set(path)

    def _update_step2_state(self):
        all_three = all(os.path.isfile(x.strip()) for x in (self.var_sap.get(), self.var_epw.get(), self.var_land.get()))
        self.btn_extract.configure(state=("normal" if all_three else "disabled"))

    def _on_extract_step2(self):
        paths = {
            "SAP": self.var_sap.get().strip(),
            "EPW": self.var_epw.get().strip(),
            "LAND": self.var_land.get().strip(),
        }
        missing = [k for k, p in paths.items() if not p or not os.path.isfile(p)]
        if missing:
            messagebox.showerror("Missing Files", f"Please provide valid files for: {', '.join(missing)}")
            return

        db_path = default_db_path()
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        # --- NEW: Busy popup + background thread (same pattern as Update Trackers) ---
        busy = BusyPopup(self, title="Extracting Data")
        self.btn_extract.configure(state="disabled")
        self.configure(cursor="watch")
        self.update_idletasks()

        def worker():
            try:
                msgs = []
                tbl, n = pull_sap_data(db_path, paths["SAP"]);   msgs.append(f"- {tbl}: {n:,} rows")
                tbl, n = pull_epw_data(db_path, paths["EPW"]);   msgs.append(f"- {tbl}: {n:,} rows")
                tbl, n = pull_land_data(db_path, paths["LAND"]); msgs.append(f"- {tbl}: {n:,} rows")

                # Ensure indexes after loading source tables
                self._ensure_perf_indexes(db_path)

                def done_ok():
                    busy.finish()
                    self.btn_extract.configure(state="normal")
                    self.configure(cursor="")
                    messagebox.showinfo("Extraction Complete", f"Database: {db_path}\n\n" + "\n".join(msgs))

                self.after(0, done_ok)

            except Exception as e:
                err_text = f"{type(e).__name__}: {e}"

                def done_err(err_text=err_text):
                    busy.finish()
                    self.btn_extract.configure(state="normal")
                    self.configure(cursor="")
                    messagebox.showerror("Extraction Failed", err_text)

                self.after(0, done_err)

        threading.Thread(target=worker, daemon=True).start()

    # ----- Step-1 actions
    def _browse_file(self):
        fp = filedialog.askopenfilename(title="Select MPP CSV", filetypes=[("CSV files", "*.csv"), ("All files", "*.*")])
        if fp:
            self.path_var.set(fp)

    def _update_generate_state(self):
        path = self.path_var.get().strip()
        ok = bool(path) and os.path.isfile(path)
        self.generate_btn.configure(state=("normal" if ok else "disabled"))

    def _on_generate(self):
        path = self.path_var.get().strip()
        if not path:
            messagebox.showwarning("No file selected", "Please choose a CSV file first.")
            return

        # Ask for export path on the main thread *before* starting the worker
        mdy_slash, mdy_dash = today_strings()
        default_name = f"WMP Order List - {mdy_dash}.xlsx"
        save_path = filedialog.asksaveasfilename(
            title=f'Exporting WMP Order Tracking List - {mdy_slash}',
            defaultextension=".xlsx",
            initialfile=default_name,
            filetypes=[("Excel Workbook", "*.xlsx")],
        )
        if not save_path:
            # User cancelled; do nothing
            return

        # --- NEW: Busy popup + background thread (same pattern as Update Trackers) ---
        busy = BusyPopup(self, title="Processing MPP / Order List")
        self.generate_btn.configure(state="disabled")
        self.configure(cursor="watch")
        self.update_idletasks()

        def worker():
            try:
                # Heavy work off the UI thread
                rows, seeded, appended = run_import_and_updates(path)
                export_order_list_to_excel(save_path)

                def done_ok():
                    busy.finish()
                    self.generate_btn.configure(state="normal")
                    self.configure(cursor="")

                    if seeded > 0:
                        msg = (
                            f"mpp_data rows: {rows}\n"
                            f"order_tracking_list seeded with {seeded} orders.\n"
                            f"Exported to:\n{save_path}"
                        )
                    else:
                        msg = (
                            f"mpp_data rows: {rows}\n"
                            f"New orders appended: {appended}\n"
                            f"Exported to:\n{save_path}"
                        )
                    messagebox.showinfo("Success", msg)

                self.after(0, done_ok)

            except Exception as e:
                err_text = f"{type(e).__name__}: {e}"

                def done_err(err_text=err_text):
                    busy.finish()
                    self.generate_btn.configure(state="normal")
                    self.configure(cursor="")
                    messagebox.showerror("Error", f"Something went wrong:\n{err_text}")

                self.after(0, done_err)

        threading.Thread(target=worker, daemon=True).start()

    # ----- Update Trackers (with loading popup)
    def _on_update_trackers(self):
        db_path = default_db_path()
        if not os.path.isfile(db_path):
            messagebox.showerror("Missing DB", f"Database not found:\n{db_path}\n\nRun Extract/Generate first.")
            return

        busy = BusyPopup(self, title="Updating Trackers")
        self.btn_update_trackers.configure(state="disabled")
        self.configure(cursor="watch")
        self.update_idletasks()

        def worker():
            try:
                # make sure indexes exist before heavy rebuilds
                self._ensure_perf_indexes(db_path)

                affected, total_orders = build_sap_tracker_initial(db_path)

                def done_ok():
                    busy.finish()
                    self.btn_update_trackers.configure(state="normal")
                    self.configure(cursor="")
                    messagebox.showinfo(
                        "SAP Tracker Updated",
                        (
                            f"sap_tracker updated for {total_orders:,} orders.\n"
                            f"Affected rows (inserted/updated): {affected:,}\n\n"
                            "Columns built: Order, Primary Status, SP56, RP56, SP57, RP57, "
                            "DS42, PC20, DS76, PC24, DS11, PC21, AP10, AP25, DS28, DS73\n"
                            "open_dependencies refreshed as well."
                        )
                    )
                    # NOTE: Do not auto-refresh the large table here; use the Refresh button
                self.after(0, done_ok)

            except Exception as e:
                err_text = f"{type(e).__name__}: {e}"

                def done_err(err_text=err_text):
                    busy.finish()
                    self.btn_update_trackers.configure(state="normal")
                    self.configure(cursor="")
                    messagebox.showerror("Update Failed", err_text)

                self.after(0, done_err)

        threading.Thread(target=worker, daemon=True).start()

    # ----- Manual Inputs
    def _open_manual_inputs(self):
        ManualInputsPopup(self)


class ManualInputsPopup(tk.Toplevel):
    """
    Paste two-column table OR update from tracker Excel.

    Supported paste fields (case-insensitive):
      - "Environment Anticipated Out Date"
      - "Environment Notes"
      - "Sent to OU Date"
      - "Permit Notes"
      - "Land Notes"
      - "FAA Notes"
      - "Joint Pole Notes"
    """
    SUPPORTED_FIELDS = {
        "environment anticipated out date": "Environment Anticipated Out Date",
        "environment notes": "Environment Notes",
        "sent to ou date": "Sent to OU Date",
        "permit notes": "Permit Notes",
        "land notes": "Land Notes",
        "faa notes": "FAA Notes",
        "joint pole notes": "Joint Pole Notes",
    }

    def __init__(self, parent):
        super().__init__(parent)
        self.title("Manual Inputs")
        self.resizable(True, True)
        self.transient(parent)
        self.grab_set()

        # ---------------- NEW: Tracker file row ----------------
        self.tracker_path_var = tk.StringVar()

        row0 = ttk.Frame(self)
        row0.grid(row=0, column=0, columnspan=3, sticky="ew", padx=16, pady=(12, 6))
        row0.columnconfigure(1, weight=1)

        ttk.Label(row0, text="Tracker File:").grid(row=0, column=0, sticky="w")

        ent_tracker = ttk.Entry(row0, textvariable=self.tracker_path_var)
        ent_tracker.grid(row=0, column=1, sticky="ew", padx=(6, 6))

        btn_browse = ttk.Button(row0, text="Browse...", command=self._browse_tracker)
        btn_browse.grid(row=0, column=2, sticky="w")

        self.btn_update_excel = ttk.Button(
            row0, text="Update from Excel",
            command=self._on_update_from_excel,
            state="disabled"
        )
        self.btn_update_excel.grid(row=0, column=3, sticky="w", padx=(8, 0))

        self.tracker_path_var.trace_add("write", self._on_tracker_path_change)

        # ---------------- Existing paste UI ----------------
        ttk.Label(self, text="Paste a two-column table below (first row is headers):").grid(
            row=1, column=0, columnspan=3, sticky="w", padx=16, pady=(6, 8)
        )

        self.txt = tk.Text(self, width=90, height=12)
        self.txt.grid(row=2, column=0, columnspan=3, sticky="nsew", padx=16)
        self.rowconfigure(2, weight=1)
        self.columnconfigure(1, weight=1)

        btns = ttk.Frame(self)
        btns.grid(row=3, column=0, columnspan=3, sticky="e", padx=16, pady=(8, 8))

        self.btn_preview = ttk.Button(btns, text="Preview", command=self._on_preview)
        self.btn_preview.grid(row=0, column=0, padx=(0, 6))

        self.btn_save = ttk.Button(btns, text="Update Manually", command=self._on_save, state="disabled")
        self.btn_save.grid(row=0, column=1)

        lf = ttk.LabelFrame(self, text="Parsed Preview")
        lf.grid(row=4, column=0, columnspan=3, sticky="nsew", padx=16, pady=(0, 14))
        self.tree = ttk.Treeview(lf, columns=("order", "value"), show="headings", height=10)
        self.tree.heading("order", text="Order")
        self.tree.heading("value", text="Value")
        self.tree.column("order", width=160, anchor="w")
        self.tree.column("value", width=540, anchor="w")
        self.tree.pack(fill="both", expand=True)

        self._parsed_rows: list[tuple[int, str]] = []
        self._field_canonical: str | None = None

        self.protocol("WM_DELETE_WINDOW", self._close)

    # ---------------- NEW handlers ----------------
    def _browse_tracker(self):
        path = filedialog.askopenfilename(
            title="Select Tracker Excel File",
            filetypes=[("Excel Workbook", "*.xlsx"), ("All files", "*.*")]
        )
        if path:
            self.tracker_path_var.set(path)

    def _on_tracker_path_change(self, *_):
        path = self.tracker_path_var.get().strip()
        self.btn_update_excel.configure(state="normal" if path else "disabled")

    def _on_update_from_excel(self):
        path = self.tracker_path_var.get().strip()
        if not path or not os.path.exists(path):
            messagebox.showerror("Missing file", "Please browse and select a valid tracker Excel file.")
            return

        db_path = default_db_path()
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        try:
            with sqlite3.connect(db_path) as conn:
                n = save_from_tracker_excel(conn, path)
        except Exception as e:
            messagebox.showerror("Update Failed", f"{type(e).__name__}: {e}")
            return

        messagebox.showinfo("Updated", f"Updated {n} cell(s) from tracker Excel into manual_tracker.")

    # ---------------- Existing paste logic ----------------
    def _close(self):
        try:
            self.grab_release()
        except Exception:
            pass
        self.destroy()

    @staticmethod
    def _smart_split_fallback(line: str):
        if "\t" in line:
            return [c.strip() for c in line.split("\t")]
        if "," in line:
            return [c.strip() for c in line.split(",")]
        parts = [p for p in _re.split(r"\s{2,}", line.strip()) if p != ""]
        if len(parts) > 1:
            return parts
        return line.strip().split()

    def _parse_text(self) -> tuple[str, list[tuple[int, str]]]:
        raw = self.txt.get("1.0", "end-1c")
        if not raw.strip():
            raise ValueError("Nothing pasted. Please paste a two-column table.")

        use_tsv = ("\t" in raw)
        use_csv = (not use_tsv) and ("," in raw.splitlines()[0] if raw.splitlines() else False)

        rows: list[list[str]] = []
        if use_tsv or use_csv:
            delim = "\t" if use_tsv else ","
            reader = csv.reader(io.StringIO(raw), delimiter=delim)
            for r in reader:
                if any((c or "").strip() for c in r):
                    rows.append([(c or "").strip() for c in r])
        else:
            lines = [ln for ln in raw.splitlines() if ln.strip()]
            for ln in lines:
                rows.append(self._smart_split_fallback(ln))

        if len(rows) < 2:
            raise ValueError("Need at least a header row and one data row.")

        header = rows[0]
        if len(header) < 2:
            raise ValueError("Header must have at least two columns: 'Order' and a field name.")
        if header[0].strip().lower() != "order":
            raise ValueError("First header must be 'Order' (case-insensitive).")

        field_in = header[1].strip().lower()
        if field_in not in self.SUPPORTED_FIELDS:
            raise ValueError(
                f"Unsupported field '{header[1].strip()}'. "
                f"Supported: {', '.join(self.SUPPORTED_FIELDS.values())}"
            )
        field_canonical = self.SUPPORTED_FIELDS[field_in]

        parsed: list[tuple[int, str]] = []
        for i, r in enumerate(rows[1:], start=2):
            if not r or all(c.strip() == "" for c in r):
                continue
            ord_cell = (r[0] if len(r) >= 1 else "").strip()
            val_cell = (r[1] if len(r) >= 2 else "").strip()
            if not ord_cell:
                continue
            try:
                order_num = int(ord_cell)
            except Exception:
                raise ValueError(f"Row {i}: 'Order' must be an integer. Got: {ord_cell!r}")
            parsed.append((order_num, val_cell))

        if not parsed:
            raise ValueError("No valid data rows found under the headers.")
        return field_canonical, parsed

    def _on_preview(self):
        try:
            field_canonical, rows = self._parse_text()
        except ValueError as e:
            messagebox.showerror("Parse Error", str(e))
            self._clear_tree()
            self.btn_save.configure(state="disabled")
            self._parsed_rows = []
            self._field_canonical = None
            return

        self._clear_tree()
        for order_num, value in rows:
            self.tree.insert("", "end", values=(order_num, value))

        self._parsed_rows = rows
        self._field_canonical = field_canonical
        self.btn_save.configure(state="normal")

    def _clear_tree(self):
        for item in self.tree.get_children(""):
            self.tree.delete(item)

    def _on_save(self):
        if not self._parsed_rows or not self._field_canonical:
            messagebox.showerror("Nothing to Save", "Please preview a valid table before saving.")
            return

        db_path = default_db_path()
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        try:
            n = self._write_manual_rows(db_path, self._field_canonical, self._parsed_rows)
        except Exception as e:
            messagebox.showerror("Save Failed", f"{type(e).__name__}: {e}")
            return

        messagebox.showinfo("Saved", f"Saved {n} row(s) to 'manual_tracker'.")
        self._close()

    def _write_manual_rows(self, db_path: str, field_canonical: str, rows: list[tuple[int, str]]) -> int:
        with sqlite3.connect(db_path) as conn:
            return save_pasted_pairs(conn, field_canonical, rows)

class ProgressPopup(tk.Toplevel):
    def __init__(self, parent, title="Working"):
        super().__init__(parent)
        self.title(title)
        self.resizable(False, False)
        self.transient(parent)
        self.grab_set()

        self.label_var = tk.StringVar(value="Starting ...")
        ttk.Label(self, textvariable=self.label_var).grid(row=0, column=0, padx=(8, 16), pady=(16, 8), sticky="w")

        self.pb = ttk.Progressbar(self, mode="determinate", maximum=3, length=360)
        self.pb.grid(row=1, column=0, padx=16, pady=(0, 12))
        self.pb["value"] = 0

        self.steps_text = tk.Text(self, width=54, height=4, relief="flat", bg=self.cget("bg"))
        self.steps_text.insert("end", "1. Updating mpp_data\n2. Updating order_tracking_list\n3. Exporting WMP Order Tracking List - MM/DD/YYYY")
        self.steps_text.configure(state="disabled")
        self.steps_text.grid(row=2, column=0, padx=16, pady=(0, 12))

        self.protocol("WM_DELETE_WINDOW", self._disable_close)

    def _disable_close(self):
        pass

    def set_step(self, step_idx: int, message: str):
        self.label_var.set(message)
        self.pb["value"] = step_idx
        self.update_idletasks()

    def finish(self, final_message: str):
        self.label_var.set(final_message)
        self.pb["value"] = 3
        self.update_idletasks()
        self.grab_release()
        self.after(250, self.destroy())


class BusyPopup(tk.Toplevel):
    """Simple indeterminate spinner modal for long-running tasks."""
    def __init__(self, parent, title="Working"):
        super().__init__(parent)
        self.title(title)
        self.resizable(False, False)
        self.transient(parent)
        self.grab_set()

        ttk.Label(self, text="Please wait…").grid(row=0, column=0, padx=16, pady=(14, 6), sticky="w")
        self.pb = ttk.Progressbar(self, mode="indeterminate", length=320)
        self.pb.grid(row=1, column=0, padx=16, pady=(0, 14))
        self.pb.start(10)

        self.protocol("WM_DELETE_WINDOW", self._disable_close)

    def _disable_close(self):
        pass

    def finish(self):
        try:
            self.pb.stop()
        except Exception:
            pass
        self.grab_release()
        self.destroy()