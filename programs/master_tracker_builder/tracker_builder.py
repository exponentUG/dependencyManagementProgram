from __future__ import annotations

import os
import threading
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Tuple
import pandas as pd

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from helpers.wmp_tracker_builder.logic import today_strings


from core.base import ToolView, FONT_H1, FONT_H2  # FONT_H1 may be unused but is fine

from services.db import wmp_db, maintenance_db, poles_db, poles_rfc_db

from helpers.sap_reports.master_tracker_builder.task_management_master import (
    run_multi_tm_export,
)
from helpers.tracker_builder.pull_sap_data import pull_sap_data
from helpers.tracker_builder.pull_epw_data import pull_epw_data
from helpers.tracker_builder.pull_land_data import pull_land_data
from helpers.tracker_builder.update_trackers import build_sap_tracker_initial

# table builders (shared across programs)
from helpers.tracker_builder.table_builders.environment_table import get_environment_table
from helpers.tracker_builder.table_builders.misctsk_table import get_misc_tsk_table
from helpers.tracker_builder.table_builders.joint_pole_table import get_joint_pole_table
from helpers.tracker_builder.table_builders.permit_table import get_permit_table
from helpers.tracker_builder.table_builders.land_table import get_land_table
from helpers.tracker_builder.table_builders.faa_table import get_faa_table
from helpers.tracker_builder.table_builders.master_table import get_master_table

TRACKER_MODES = [
    "Master",
    "Permit",
    "Land",
    "FAA",
    "Environment",
    "Joint Pole",
    "MiscTSK",
]

DB_CHOICES = [
    "WMP",
    "Maintenance",
    "Poles",
    "Poles RFC",
]

from ledgers.tracker_conditions_ledger.maintenance import (
    ALLOWED_MAT as ALLOWED_MAT_MAINT,
    ALLOWED_SAP_STATUS as ALLOWED_SAP_STATUS_MAINT,
)
from ledgers.tracker_conditions_ledger.poles import (
    ALLOWED_MAT as ALLOWED_MAT_POLES,
    ALLOWED_SAP_STATUS as ALLOWED_SAP_STATUS_POLES,
)
from ledgers.tracker_conditions_ledger.poles_rfc import (
    ALLOWED_MAT as ALLOWED_MAT_POLES_RFC,
    ALLOWED_SAP_STATUS as ALLOWED_SAP_STATUS_POLES_RFC,
)
from ledgers.tracker_conditions_ledger.wmp import ALLOWED_MAT as ALLOWED_MAT_WMP

class BusyPopup(tk.Toplevel):
    """Simple indeterminate spinner modal for long-running tasks."""

    def __init__(self, parent, title: str = "Working"):
        super().__init__(parent)
        self.title(title)
        self.resizable(False, False)
        self.transient(parent)
        self.grab_set()

        ttk.Label(self, text="Please wait…").grid(
            row=0, column=0, padx=16, pady=(14, 6), sticky="w"
        )
        self.pb = ttk.Progressbar(self, mode="indeterminate", length=320)
        self.pb.grid(row=1, column=0, padx=16, pady=(0, 14))
        self.pb.start(10)

        self.protocol("WM_DELETE_WINDOW", self._disable_close)

    def _disable_close(self):
        # Prevent closing while a task is in progress
        pass

    def finish(self):
        try:
            self.pb.stop()
        except Exception:
            pass
        try:
            self.grab_release()
        except Exception:
            pass
        self.destroy()


class MASTER_TRACKER_BUILDER(ToolView):
    """
    Master Tracker Builder

    Step 1: Take a single MPP CSV and update mpp_data + order_tracking_list
            for ALL dependency trackers (WMP, Maintenance, Poles, Poles RFC)
            in one go.

    Step 2: Manage SAP / EPW / Land source files for all trackers in one place
            and run the extraction into each DB.

    Tracker Tools: Run build_sap_tracker_initial for all trackers
                   from a single "Update Trackers" button.
    """

    def __init__(self, master: tk.Misc | None = None, **kwargs: Any) -> None:
        super().__init__(master, **kwargs)

        # Step 1
        self.path_var = tk.StringVar()

        # Step 2 – folder + per-tracker SAP files + shared EPW / Land
        self.var_sap = tk.StringVar()  # destination folder for SAP exports

        self.var_sap_maint = tk.StringVar()
        self.var_sap_poles = tk.StringVar()
        self.var_sap_poles_rfc = tk.StringVar()
        self.var_sap_wmp = tk.StringVar()

        self.var_epw = tk.StringVar()
        self.var_land = tk.StringVar()

        # Attach change listeners so we can enable/disable "Extract Data"
        for v in (
            self.var_sap,
            self.var_sap_maint,
            self.var_sap_poles,
            self.var_sap_poles_rfc,
            self.var_sap_wmp,
            self.var_epw,
            self.var_land,
        ):
            v.trace_add("write", lambda *_: self._update_step2_state())

        self.btn_extract: ttk.Button | None = None
        self.btn_update_trackers: ttk.Button | None = None

        # Tracker Tools: database + tracker selection + table view
        self.db_var = tk.StringVar(value="WMP")          # Database dropdown
        self.tracker_var = tk.StringVar(value="Master")  # Tracker dropdown
        self.count_var = tk.StringVar(value="Row Count: —")

        self.tree: ttk.Treeview | None = None  # will be created in _build_ui


        self._build_ui()
        self._wire_signals()

    def _build_ui(self) -> None:
        self.columnconfigure(0, weight=1)

        # Common label width so all URL bars line up
        label_width = 24  # tweak this number until it looks right

        # ---- Row 3: Step 1 title ----
        ttk.Label(
            self,
            text="Step 1: Extract data from MPP Database",
            font=FONT_H2,
        ).grid(row=3, column=0, columnspan=6, sticky="w", pady=(8, 4), padx=16)

        # ---- Row 4: "MPP Database:" [Entry] [Browse] [Update MPP Database] ----
        fr4 = ttk.Frame(self)
        fr4.grid(row=4, column=0, columnspan=6, sticky="ew", padx=16, pady=4)
        fr4.columnconfigure(1, weight=1)

        ttk.Label(fr4, text="MPP Database:", width=label_width).grid(
            row=0, column=0, sticky="w", padx=(0, 8)
        )

        self.path_entry = ttk.Entry(fr4, textvariable=self.path_var, width=70)
        self.path_entry.grid(row=0, column=1, sticky="ew", padx=(0, 8))

        self.browse_btn = ttk.Button(fr4, text="Browse", command=self._browse_file)
        self.browse_btn.grid(row=0, column=2, sticky="w", padx=(0, 8))

        self.generate_btn = ttk.Button(
            fr4,
            text="Update MPP Database",
            command=self._on_generate,
            state="disabled",
        )
        self.generate_btn.grid(row=0, column=3, sticky="w")

        # ---- Row 5: Step 2 title ----
        ttk.Label(
            self,
            text="Step 2: Get data from SAP",
            font=FONT_H2,
        ).grid(row=5, column=0, columnspan=6, sticky="w", padx=16, pady=(16, 2))

        # ---- Row 6: SAP Data (all trackers) ----
        # Here the entry shows the *destination folder* for the SAP exports.
        fr6 = ttk.Frame(self)
        fr6.grid(row=6, column=0, columnspan=6, sticky="ew", padx=16, pady=4)
        fr6.columnconfigure(1, weight=1)

        ttk.Label(fr6, text="SAP Data (all trackers)", width=label_width).grid(
            row=0, column=0, sticky="w", padx=(0, 8)
        )
        ttk.Entry(fr6, textvariable=self.var_sap).grid(
            row=0, column=1, sticky="ew", padx=(0, 8)
        )
        ttk.Button(
            fr6,
            text="Browse…",
            command=self._browse_sap_folder,
        ).grid(row=0, column=2, sticky="w", padx=(0, 8))
        ttk.Button(
            fr6,
            text="Extract SAP Data",
            command=self._on_extract_sap_data,
        ).grid(row=0, column=3, sticky="w")

        # ---- Row 7: Step 3 title ----
        ttk.Label(
            self,
            text="Step 3: Extract data from SAP, Land, and EPW Database",
            font=FONT_H2,
        ).grid(row=7, column=0, columnspan=6, sticky="w", padx=16, pady=(16, 2))

        # ---- Rows 8–11: Per-tracker SAP file paths ----
        def _make_sap_row(row: int, label: str, var: tk.StringVar) -> None:
            fr = ttk.Frame(self)
            fr.grid(row=row, column=0, columnspan=6, sticky="ew", padx=16, pady=2)
            fr.columnconfigure(1, weight=1)
            ttk.Label(fr, text=label, width=label_width).grid(
                row=0, column=0, sticky="w", padx=(0, 8)
            )
            ttk.Entry(fr, textvariable=var).grid(
                row=0, column=1, sticky="ew", padx=(0, 8)
            )
            ttk.Button(
                fr,
                text="Browse…",
                command=lambda v=var: self._browse_excel(v),
            ).grid(row=0, column=2, sticky="w")

        _make_sap_row(8,  "Maintenance SAP Data:", self.var_sap_maint)
        _make_sap_row(9,  "Poles SAP Data:", self.var_sap_poles)
        _make_sap_row(10, "Poles RFC SAP Data:", self.var_sap_poles_rfc)
        _make_sap_row(11, "WMP SAP Data:", self.var_sap_wmp)

        # ---- Rows 12–13: Shared EPW / Land inputs ----
        def _make_row(row: int, label: str, var: tk.StringVar) -> None:
            fr = ttk.Frame(self)
            fr.grid(row=row, column=0, columnspan=6, sticky="ew", padx=16, pady=4)
            fr.columnconfigure(1, weight=1)
            ttk.Label(fr, text=label, width=label_width).grid(
                row=0, column=0, sticky="w", padx=(0, 8)
            )
            ttk.Entry(fr, textvariable=var).grid(
                row=0, column=1, sticky="ew", padx=(0, 8)
            )
            ttk.Button(
                fr,
                text="Browse…",
                command=lambda v=var: self._browse_excel(v),
            ).grid(row=0, column=2, sticky="w")

        _make_row(12, "EPW Data", self.var_epw)
        _make_row(13, "Land Data", self.var_land)

        # ---- Row 14: Extract Data button ----
        fr_btn = ttk.Frame(self)
        fr_btn.grid(
            row=14,
            column=0,
            columnspan=6,
            sticky="w",
            padx=16,
            pady=(4, 6),
        )
        self.btn_extract = ttk.Button(
            fr_btn,
            text="Extract Data",
            command=self._on_extract_step2,
            state="disabled",
        )
        self.btn_extract.grid(row=0, column=0)

        # ---- Row 15: Tracker Tools heading ----
        ttk.Label(self, text="Tracker Tools", font=FONT_H1).grid(
            row=15, column=0, columnspan=6, sticky="w", padx=16, pady=(12, 4)
        )

        # ---- Row 16: Tools row
        # left: Update Trackers
        # right: Database + Tracker dropdowns
        fr_tools = ttk.Frame(self)
        fr_tools.grid(
            row=16, column=0, columnspan=6, sticky="ew", padx=16, pady=(0, 8)
        )
        fr_tools.columnconfigure(0, weight=0)  # left buttons
        fr_tools.columnconfigure(1, weight=1)  # spacer
        fr_tools.columnconfigure(2, weight=0)  # right controls

        # Left group: Update Trackers + Export to Excel
        left_fr = ttk.Frame(fr_tools)
        left_fr.grid(row=0, column=0, sticky="w")

        self.btn_update_trackers = ttk.Button(
            left_fr,
            text="Update Trackers",
            command=self._on_update_trackers,
        )
        self.btn_update_trackers.grid(row=0, column=0, padx=(0, 8))

        self.btn_export_excel = ttk.Button(
            left_fr,
            text="Export to Excel",
            command=self._on_export_excel,
        )
        self.btn_export_excel.grid(row=0, column=1, padx=(0, 8))

        # Right group: Database + Tracker dropdowns
        right_fr = ttk.Frame(fr_tools)
        right_fr.grid(row=0, column=2, sticky="e")

        ttk.Label(right_fr, text="Database:").grid(
            row=0, column=0, sticky="e", padx=(0, 4)
        )
        self.db_dd = ttk.Combobox(
            right_fr,
            textvariable=self.db_var,
            state="readonly",
            values=DB_CHOICES,
            width=14,
        )
        self.db_dd.grid(row=0, column=1, sticky="e", padx=(0, 12))
        self.db_dd.bind("<<ComboboxSelected>>", lambda _e: self._refresh_table())

        ttk.Label(right_fr, text="Tracker:").grid(
            row=0, column=2, sticky="e", padx=(0, 4)
        )
        self.tracker_dd = ttk.Combobox(
            right_fr,
            textvariable=self.tracker_var,
            state="readonly",
            values=TRACKER_MODES,
            width=16,
        )
        self.tracker_dd.grid(row=0, column=3, sticky="e")
        self.tracker_dd.bind("<<ComboboxSelected>>", lambda _e: self._refresh_table())

        # ---- Row 17: Row Count (between tools and table)
        fr_count = ttk.Frame(self)
        fr_count.grid(
            row=17, column=0, columnspan=6, sticky="ew", padx=16, pady=(0, 6)
        )
        ttk.Label(fr_count, textvariable=self.count_var).grid(
            row=0, column=0, sticky="w"
        )

        # ---- Row 18: Table (Treeview) with scrollbars
        lf = ttk.LabelFrame(self, text="Tracker View")
        lf.grid(
            row=18, column=0, columnspan=6,
            sticky="nsew", padx=16, pady=(0, 12)
        )

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
        self.rowconfigure(18, weight=1)
        self.columnconfigure(0, weight=1)
        self.columnconfigure(1, weight=1)
        self.columnconfigure(2, weight=1)
        self.columnconfigure(3, weight=1)

        # Initial load for the table
        self._refresh_table()

    # ------------------------------------------------------------------
    # Table helpers (shared tracker view across all DBs)
    # ------------------------------------------------------------------
    def _on_export_excel(self) -> None:
        """
        Export all tracker views (7 sheets) for ALL four programs
        (WMP, Maintenance, Poles, Poles RFC) to separate Excel files
        in a single selected folder, with a busy popup.
        """
        # Ask for destination folder on the main thread
        folder = filedialog.askdirectory(
            title="Select destination folder for tracker Excel exports"
        )
        if not folder:
            return  # user cancelled

        # Show busy popup while we export in the background
        busy = BusyPopup(self, title="Exporting trackers to Excel")
        if getattr(self, "btn_export_excel", None) is not None:
            self.btn_export_excel.configure(state="disabled")
        self.configure(cursor="watch")
        self.update_idletasks()

        def worker() -> None:
            # Common sheet mapping: sheet name -> table builder
            sheets = [
                ("Master",      get_master_table),
                ("Permit",      get_permit_table),
                ("Land",        get_land_table),
                ("FAA",         get_faa_table),
                ("Environment", get_environment_table),
                ("Joint Pole",  get_joint_pole_table),
                ("MiscTSK",     get_misc_tsk_table),
            ]

            # Date suffix for filenames
            _mdy_slash, mdy_dash = today_strings()

            # (Label for messages, file prefix, db module)
            trackers = [
                ("WMP",         "WMP Tracker",         wmp_db),
                ("Maintenance", "Maintenance Tracker", maintenance_db),
                ("Poles",       "Poles Tracker",       poles_db),
                ("Poles RFC",   "Poles RFC Tracker",   poles_rfc_db),
            ]

            successes: list[str] = []
            errors: list[str] = []

            for label, prefix, db_mod in trackers:
                db_path = db_mod.default_db_path()
                if not os.path.isfile(db_path):
                    errors.append(
                        f"- {label}: Database not found:\n  {db_path}\n  Run Extract/Generate first."
                    )
                    continue

                save_path = os.path.join(folder, f"{prefix} - {mdy_dash}.xlsx")

                try:
                    with pd.ExcelWriter(save_path, engine="xlsxwriter") as writer:
                        for sheet_name, getter in sheets:
                            try:
                                cols, rows = getter(db_path)
                            except Exception as e:
                                # If a table builder fails, put the error in that sheet
                                cols, rows = ["Message"], [
                                    (f"Error loading {sheet_name}: {type(e).__name__}: {e}",)
                                ]

                            if not cols:
                                # Match UI behavior: when no data, include a friendly message
                                df = pd.DataFrame(
                                    [
                                        {
                                            "Message": (
                                                "No data yet. Run 'Extract Data' and "
                                                "'Update Trackers' first."
                                            )
                                        }
                                    ]
                                )
                            else:
                                df = pd.DataFrame(rows, columns=cols)

                            df.to_excel(writer, sheet_name=sheet_name, index=False)

                    successes.append(f"- {label}: {save_path}")
                except Exception as e:
                    errors.append(f"- {label}: {type(e).__name__}: {e}")

            def done() -> None:
                # Close busy popup and restore UI state
                busy.finish()
                if getattr(self, "btn_export_excel", None) is not None:
                    self.btn_export_excel.configure(state="normal")
                self.configure(cursor="")

                # Summarize result
                if not successes and errors:
                    messagebox.showerror(
                        "Export Failed",
                        "No trackers were exported.\n\n" + "\n".join(errors),
                    )
                elif successes and errors:
                    msg_lines: list[str] = []
                    msg_lines.append("Some trackers were exported, but others failed.")
                    msg_lines.append("")
                    msg_lines.append("Exported:")
                    msg_lines.extend(successes)
                    msg_lines.append("")
                    msg_lines.append("Issues:")
                    msg_lines.extend(errors)
                    messagebox.showwarning(
                        "Export Complete (partial)", "\n".join(msg_lines)
                    )
                else:
                    msg_lines = ["Exported all trackers successfully:", ""]
                    msg_lines.extend(successes)
                    messagebox.showinfo("Export Complete", "\n".join(msg_lines))

            # Back to main thread for UI updates
            self.after(0, done)

        threading.Thread(target=worker, daemon=True).start()

    def _get_db_path_for_selection(self) -> str | None:
        """Map dropdown database name -> concrete DB path."""
        name = (self.db_var.get() or "").strip()
        if name == "WMP":
            return wmp_db.default_db_path()
        if name == "Maintenance":
            return maintenance_db.default_db_path()
        if name == "Poles":
            return poles_db.default_db_path()
        if name == "Poles RFC":
            return poles_rfc_db.default_db_path()
        return None

    def _set_table_columns(self, columns: list[str]) -> None:
        if not self.tree:
            return

        # Clear headings
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
            if col in (
                "WPD",
                "CLICK Start Date",
                "CLICK End Date",
                "Environment Anticipated Out Date",
            ):
                base = 180
            self.tree.column(col, width=base, minwidth=80, anchor="w")

    def _clear_table_rows(self) -> None:
        if not self.tree:
            return
        for iid in self.tree.get_children(""):
            self.tree.delete(iid)

    def _populate_table(self, columns: list[str], rows: list[tuple]) -> None:
        if not self.tree:
            return

        self._set_table_columns(columns)
        self._clear_table_rows()

        if not rows:
            if columns and columns[0] != "Message":
                self._set_table_columns(["Message"])
            self.tree.insert("", "end", values=("No data to display.",))
            return

        for r in rows:
            r2 = tuple(list(r)[: len(columns)] + [""] * max(0, len(columns) - len(r)))
            self.tree.insert("", "end", values=r2)

    def _refresh_table(self) -> None:
        """Refresh table based on selected Database + Tracker."""
        if not self.tree:
            return

        mode = (self.tracker_var.get() or "").strip()
        db_path = self._get_db_path_for_selection()

        if not db_path or not os.path.isfile(db_path):
            self._populate_table(
                ["Message"],
                [(
                    "Database not found for this program. "
                    "Run 'Extract Data' and 'Update Trackers' first.",
                )],
            )
            self.count_var.set("Row Count: —")
            return

        # Choose appropriate table builder
        try:
            if mode == "Environment":
                columns, rows = get_environment_table(db_path)
            elif mode == "MiscTSK":
                columns, rows = get_misc_tsk_table(db_path)
            elif mode == "Joint Pole":
                columns, rows = get_joint_pole_table(db_path)
            elif mode == "Permit":
                columns, rows = get_permit_table(db_path)
            elif mode == "Land":
                columns, rows = get_land_table(db_path)
            elif mode == "FAA":
                columns, rows = get_faa_table(db_path)
            elif mode == "Master":
                columns, rows = get_master_table(db_path)
            else:
                self._populate_table(["Message"], [("In development...",)])
                self.count_var.set("Row Count: —")
                return
        except Exception as e:
            self._populate_table(
                ["Message"],
                [(f"Error loading {mode} view: {type(e).__name__}: {e}",)],
            )
            self.count_var.set("Row Count: —")
            return

        if not columns:
            self._populate_table(
                ["Message"],
                [("No data yet. Run 'Extract Data' and 'Update Trackers' first.",)],
            )
            self.count_var.set("Row Count: 0")
        else:
            self._populate_table(columns, rows)
            self.count_var.set(f"Row Count: {len(rows):,}")
    
    def _wire_signals(self) -> None:
        self.path_var.trace_add("write", lambda *_: self._update_generate_state())

    # ------------------------------------------------------------------
    # Step 1 helpers (MPP update for all DBs)
    # ------------------------------------------------------------------
    def _browse_file(self) -> None:
        fp = filedialog.askopenfilename(
            title="Select MPP CSV",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
        if fp:
            self.path_var.set(fp)

    def _update_generate_state(self) -> None:
        # Don't re-enable while a run is in progress
        path = self.path_var.get().strip()
        ok = bool(path) and os.path.isfile(path)
        self.generate_btn.configure(state=("normal" if ok else "disabled"))

    def _on_generate(self) -> None:
        path = self.path_var.get().strip()
        if not path:
            messagebox.showwarning(
                "No file selected",
                "Please choose a CSV file first.",
            )
            return

        if not os.path.isfile(path):
            messagebox.showerror("Invalid file", f"File not found:\n{path}")
            return

        # Mark UI as busy
        self.generate_btn.configure(state="disabled")
        self.browse_btn.configure(state="disabled")
        self.configure(cursor="watch")
        self.update_idletasks()

        def worker() -> None:
            trackers = [
                ("WMP", wmp_db),
                ("Maintenance", maintenance_db),
                ("Poles", poles_db),
                ("Poles RFC", poles_rfc_db),
            ]

            results: List[Dict[str, Any]] = []
            errors: List[Dict[str, Any]] = []

            for label, db_mod in trackers:
                popup_holder: Dict[str, BusyPopup] = {}
                ready = threading.Event()

                # Create a BusyPopup on the main thread
                def create_popup(lbl: str = label) -> None:
                    popup_holder["popup"] = BusyPopup(
                        self,
                        title=f"Updating MPP Database for {lbl}",
                    )
                    ready.set()

                self.after(0, create_popup)
                # Wait until the popup is created before doing heavy work
                ready.wait()
                popup = popup_holder["popup"]

                try:
                    # Heavy work in the background thread
                    df = db_mod.load_and_filter_csv(path)
                    rows = db_mod.replace_mpp_data(df)
                    existing_before, inserted = (
                        db_mod.update_order_tracking_list_from_mpp()
                    )

                    results.append(
                        {
                            "label": label,
                            "rows": rows,
                            "existing_before": existing_before,
                            "inserted": inserted,
                        }
                    )
                except Exception as e:
                    errors.append({"label": label, "error": e})
                finally:
                    # Close the popup on the main thread
                    def close_popup(p: BusyPopup = popup) -> None:
                        try:
                            p.finish()
                        except Exception:
                            pass

                    self.after(0, close_popup)

            # Final UI cleanup and messaging on the main thread
            def done() -> None:
                self.generate_btn.configure(state="normal")
                self.browse_btn.configure(state="normal")
                self.configure(cursor="")

                if errors:
                    # At least one tracker failed
                    lines: List[str] = ["Some MPP updates failed:", ""]
                    for item in errors:
                        lbl = item["label"]
                        err = item["error"]
                        lines.append(f"- {lbl}: {type(err).__name__}: {err}")

                    if results:
                        lines.append("")
                        lines.append("Successful updates:")
                        for r in results:
                            lines.append(
                                f"  • {r['label']}: mpp_data rows = {r['rows']}, "
                                f"new orders added = {r['inserted']}"
                            )

                    messagebox.showerror(
                        "MPP Update Complete (with errors)", "\n".join(lines)
                    )
                else:
                    # All good
                    lines = ["MPP data updated successfully for all trackers:", ""]
                    for r in results:
                        lines.append(
                            f"• {r['label']}: mpp_data rows = {r['rows']}, "
                            f"new orders added = {r['inserted']}"
                        )
                    messagebox.showinfo("MPP Update Complete", "\n".join(lines))

            self.after(0, done)

        threading.Thread(target=worker, daemon=True).start()

    # ------------------------------------------------------------------
    # Step 2 helpers
    # ------------------------------------------------------------------
    def _browse_sap_folder(self) -> None:
        folder = filedialog.askdirectory(
            title="Select destination folder for SAP Data"
        )
        if folder:
            self.var_sap.set(folder)

    def _browse_excel(self, var: tk.StringVar) -> None:
        path = filedialog.askopenfilename(
            title="Select Excel file",
            filetypes=[
                ("Excel files", "*.xlsx *.xlsm *.xlsb *.xls"),
                ("All files", "*.*"),
            ],
        )
        if path:
            var.set(path)

    def _on_extract_sap_data(self) -> None:
        """
        Button handler for 'Extract SAP Data (all trackers)'.

        Calls run_multi_tm_export(), which:
          - prompts for SAP username/password and destination folder (prefilled
            from var_sap if present)
          - logs into SAP once
          - runs the Task Management report for each DB
          - exports four Excel files (one per tracker)
        Then we auto-populate the four per-tracker SAP path entries.
        """
        try:
            initial_dest = self.var_sap.get().strip() or None
            result = run_multi_tm_export(self, initial_dest=initial_dest)
        except Exception as e:
            messagebox.showerror(
                "Extract SAP Data Failed", f"{type(e).__name__}: {e}"
            )
            return

        if not result:
            # User cancelled in the dialog
            return

        dest_folder = result.get("destination") or ""
        if dest_folder:
            self.var_sap.set(dest_folder)

        # File names follow the convention in task_management_master.py
        today_str = datetime.today().strftime("%m%d%Y")
        if dest_folder:
            def _join(name: str) -> str:
                return os.path.join(dest_folder, name)

            self.var_sap_maint.set(
                _join(f"Maintenance SAP Data - {today_str}.xlsx")
            )
            self.var_sap_poles.set(
                _join(f"Poles SAP Data - {today_str}.xlsx")
            )
            self.var_sap_poles_rfc.set(
                _join(f"Poles RFC SAP Data - {today_str}.xlsx")
            )
            self.var_sap_wmp.set(
                _join(f"WMP SAP Data - {today_str}.xlsx")
            )

        # Re-evaluate whether "Extract Data" can be enabled
        self._update_step2_state()

    def _update_step2_state(self, *_args) -> None:
        """
        Enable 'Extract Data' only when we have valid files for:
          - 4x SAP (Maintenance, Poles, Poles RFC, WMP)
          - 1x EPW
          - 1x Land
        EPW / Land are shared across all trackers.
        """
        if self.btn_extract is None:
            return  # UI not fully built yet

        required_paths = [
            self.var_sap_maint.get().strip(),
            self.var_sap_poles.get().strip(),
            self.var_sap_poles_rfc.get().strip(),
            self.var_sap_wmp.get().strip(),
            self.var_epw.get().strip(),
            self.var_land.get().strip(),
        ]
        all_ok = all(p and os.path.isfile(p) for p in required_paths)
        self.btn_extract.configure(state=("normal" if all_ok else "disabled"))

    def _on_extract_step2(self) -> None:
        """
        Run the EPW / Land / SAP extraction for *all four* trackers in one go.

        - EPW + Land paths are *shared* across all trackers.
        - SAP path is taken from the respective per-tracker row.
        - For Maintenance / Poles / Poles RFC we also:
            * drop Priority = 'B'
            * filter EPW rows to a subset of SAP statuses
        """
        paths = {
            "Maintenance SAP": self.var_sap_maint.get().strip(),
            "Poles SAP": self.var_sap_poles.get().strip(),
            "Poles RFC SAP": self.var_sap_poles_rfc.get().strip(),
            "WMP SAP": self.var_sap_wmp.get().strip(),
            "EPW": self.var_epw.get().strip(),
            "LAND": self.var_land.get().strip(),
        }

        missing = [k for k, p in paths.items() if not p or not os.path.isfile(p)]
        if missing:
            messagebox.showerror(
                "Missing Files",
                "Please provide valid files for: " + ", ".join(missing),
            )
            return

        busy = BusyPopup(self, title="Extracting Data for All Trackers")
        self.btn_extract.configure(state="disabled")
        self.configure(cursor="watch")
        self.update_idletasks()

        def worker() -> None:
            try:
                msgs: List[str] = []

                # label,   db_module,    sap_path,                allowed_mat,
                # remove_btag, remove_sap_status, sap_status_to_keep
                trackers = [
                    (
                        "Maintenance",
                        maintenance_db,
                        paths["Maintenance SAP"],
                        ALLOWED_MAT_MAINT,
                        True,   # REMOVE_BTAG
                        True,   # REMOVE_SAP_STATUS
                        ALLOWED_SAP_STATUS_MAINT,
                    ),
                    (
                        "Poles",
                        poles_db,
                        paths["Poles SAP"],
                        ALLOWED_MAT_POLES,
                        True,
                        True,
                        ALLOWED_SAP_STATUS_POLES,
                    ),
                    (
                        "Poles RFC",
                        poles_rfc_db,
                        paths["Poles RFC SAP"],
                        ALLOWED_MAT_POLES_RFC,
                        True,
                        True,
                        ALLOWED_SAP_STATUS_POLES_RFC,
                    ),
                    (
                        "WMP",
                        wmp_db,
                        paths["WMP SAP"],
                        ALLOWED_MAT_WMP,
                        False,  # keep original behavior (no extra filtering)
                        False,
                        None,
                    ),
                ]

                epw_path = paths["EPW"]
                land_path = paths["LAND"]

                for (
                    label,
                    db_mod,
                    sap_path,
                    allowed_mat,
                    remove_btag,
                    remove_sap_status,
                    sap_status_to_keep,
                ) in trackers:
                    db_path = db_mod.default_db_path()
                    os.makedirs(os.path.dirname(db_path), exist_ok=True)

                    # SAP
                    t1, n1 = pull_sap_data(db_path, sap_path)
                    msgs.append(f"{label}: {t1} (SAP) = {n1:,} rows")

                    # EPW – now using the extended signature
                    t2, n2 = pull_epw_data(
                        db_path,
                        epw_path,
                        allowed_mat,
                        REMOVE_BTAG=remove_btag,
                        REMOVE_SAP_STATUS=remove_sap_status,
                        SAP_STATUS_TO_KEEP=sap_status_to_keep,
                    )
                    msgs.append(f"{label}: {t2} (EPW) = {n2:,} rows")

                    # Land (unchanged)
                    # Land – now using extended signature (same flags as EPW)
                    t3, n3 = pull_land_data(
                        db_path,
                        land_path,
                        allowed_mat,
                        REMOVE_BTAG=remove_btag,
                        REMOVE_SAP_STATUS=remove_sap_status,
                        SAP_STATUS_TO_KEEP=sap_status_to_keep,
                    )
                    msgs.append(f"{label}: {t3} (Land) = {n3:,} rows")

                def done_ok() -> None:
                    busy.finish()
                    self.btn_extract.configure(state="normal")
                    self.configure(cursor="")
                    messagebox.showinfo(
                        "Extraction Complete",
                        "Database extraction complete for all trackers.\n\n"
                        + "\n".join(msgs),
                    )

                self.after(0, done_ok)

            except Exception as e:
                err_text = f"{type(e).__name__}: {e}"

                def done_err() -> None:
                    busy.finish()
                    self.btn_extract.configure(state="normal")
                    self.configure(cursor="")
                    messagebox.showerror("Extraction Failed", err_text)

                self.after(0, done_err)

        threading.Thread(target=worker, daemon=True).start()

    # ------------------------------------------------------------------
    # Tracker Tools: Update Trackers for all DBs
    # ------------------------------------------------------------------
    def _ensure_perf_indexes(self, db_path: str) -> None:
        """
        Create helpful indexes and set WAL/NORMAL pragmas.
        Safe to call repeatedly for any of the dependency DBs.
        """
        try:
            with sqlite3.connect(db_path) as conn:
                cur = conn.cursor()
                cur.executescript(
                    """
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
                    """
                )
                conn.commit()
        except Exception:
            # Never block the UI for indexing errors
            pass

    def _on_update_trackers(self) -> None:
        """
        Run build_sap_tracker_initial for all four trackers (WMP, Maintenance,
        Poles, Poles RFC) from a single button.

        Mirrors the per-program _on_update_trackers pattern but aggregated.
        """
        trackers = [
            ("WMP", wmp_db),
            ("Maintenance", maintenance_db),
            ("Poles", poles_db),
            ("Poles RFC", poles_rfc_db),
        ]

        busy = BusyPopup(self, title="Updating Trackers (All Programs)")
        if self.btn_update_trackers is not None:
            self.btn_update_trackers.configure(state="disabled")
        self.configure(cursor="watch")
        self.update_idletasks()

        def worker() -> None:
            results: List[Tuple[str, int, int]] = []  # (label, affected, total_orders)
            errors: List[Tuple[str, str]] = []        # (label, error_text)

            for label, db_mod in trackers:
                db_path = db_mod.default_db_path()

                if not os.path.isfile(db_path):
                    errors.append(
                        (label, f"Database not found:\n{db_path}\nRun Extract/Generate first.")
                    )
                    continue

                try:
                    # make sure indexes exist before heavy rebuilds
                    self._ensure_perf_indexes(db_path)

                    affected, total_orders = build_sap_tracker_initial(db_path)
                    results.append((label, affected, total_orders))
                except Exception as e:
                    errors.append((label, f"{type(e).__name__}: {e}"))

            def done() -> None:
                busy.finish()
                if self.btn_update_trackers is not None:
                    self.btn_update_trackers.configure(state="normal")
                self.configure(cursor="")

                if not results and errors:
                    # Everything failed
                    msg_lines = ["Tracker updates failed for all programs:", ""]
                    for label, err in errors:
                        msg_lines.append(f"- {label}: {err}")
                    messagebox.showerror("Update Failed", "\n".join(msg_lines))
                    return

                # Build success/partial message
                msg_lines: List[str] = []

                if results:
                    msg_lines.append("SAP Trackers updated successfully:")
                    msg_lines.append("")
                    for label, affected, total in results:
                        msg_lines.append(
                            f"• {label}: sap_tracker updated for {total:,} orders; "
                            f"Affected rows (inserted/updated): {affected:,}"
                        )
                    msg_lines.append("")
                    msg_lines.append(
                        "Columns built per tracker include: Order, Primary Status, SP56, RP56, "
                        "SP57, RP57, DS42, PC20, DS76, PC24, DS11, PC21, AP10, AP25, DS28, DS73; "
                        "open_dependencies, permit, land, FAA, environment, joint pole, and misc task trackers are refreshed as well."
                    )

                if errors:
                    msg_lines.append("")
                    msg_lines.append("Some trackers could not be updated:")
                    for label, err in errors:
                        msg_lines.append(f"- {label}: {err}")
                    messagebox.showwarning(
                        "Update Complete (partial)", "\n".join(msg_lines)
                    )
                else:
                    messagebox.showinfo("Update Complete", "\n".join(msg_lines))

            self.after(0, done)

        threading.Thread(target=worker, daemon=True).start()
