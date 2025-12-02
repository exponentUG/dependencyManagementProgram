from __future__ import annotations

import os
import threading
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Tuple

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

from core.base import ToolView, FONT_H1  # FONT_H1 may be unused but is fine

from services.db import wmp_db, maintenance_db, poles_db, poles_rfc_db

from helpers.sap_reports.master_tracker_builder.task_management_master import (
    run_multi_tm_export,
)
from helpers.tracker_builder.pull_sap_data import pull_sap_data
from helpers.tracker_builder.pull_epw_data import pull_epw_data
from helpers.tracker_builder.pull_land_data import pull_land_data
from helpers.tracker_builder.update_trackers import build_sap_tracker_initial

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

        self._build_ui()
        self._wire_signals()

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------
    def _build_ui(self) -> None:
        self.columnconfigure(0, weight=1)

        # ---- Row 3: Step 1 title ----
        ttk.Label(
            self,
            text=(
                "Step 1: Upload latest copy of MPP and update all tracker databases"
            ),
        ).grid(row=3, column=0, columnspan=6, sticky="w", pady=(8, 4), padx=16)

        # ---- Row 4: "MPP Database:" [Entry] [Browse] [Update MPP Database] ----
        fr4 = ttk.Frame(self)
        fr4.grid(row=4, column=0, columnspan=6, sticky="ew", padx=16, pady=4)
        fr4.columnconfigure(1, weight=1)

        ttk.Label(fr4, text="MPP Database:").grid(
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
            text="Step 2: Update SAP, EPW, and Land data for all trackers",
        ).grid(row=5, column=0, columnspan=6, sticky="w", padx=16, pady=(16, 2))

        # ---- Row 6: SAP Data (all trackers) ----
        # Here the entry shows the *destination folder* for the SAP exports.
        fr6 = ttk.Frame(self)
        fr6.grid(row=6, column=0, columnspan=6, sticky="ew", padx=16, pady=4)
        fr6.columnconfigure(1, weight=1)

        ttk.Label(fr6, text="SAP Data (all trackers)").grid(
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

        # ---- Rows 7–10: Per-tracker SAP file paths ----
        def _make_sap_row(row: int, label: str, var: tk.StringVar) -> None:
            fr = ttk.Frame(self)
            fr.grid(row=row, column=0, columnspan=6, sticky="ew", padx=16, pady=2)
            fr.columnconfigure(1, weight=1)
            ttk.Label(fr, text=label).grid(
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

        _make_sap_row(7, "Maintenance SAP Data:", self.var_sap_maint)
        _make_sap_row(8, "Poles SAP Data:", self.var_sap_poles)
        _make_sap_row(9, "Poles RFC SAP Data:", self.var_sap_poles_rfc)
        _make_sap_row(10, "WMP SAP Data:", self.var_sap_wmp)

        # ---- Rows 11–12: Shared EPW / Land inputs ----
        def _make_row(row: int, label: str, var: tk.StringVar) -> None:
            fr = ttk.Frame(self)
            fr.grid(row=row, column=0, columnspan=6, sticky="ew", padx=16, pady=4)
            fr.columnconfigure(1, weight=1)
            ttk.Label(fr, text=label).grid(
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

        _make_row(11, "EPW Data", self.var_epw)
        _make_row(12, "Land Data", self.var_land)

        # ---- Row 13: Extract Data button ----
        fr_btn = ttk.Frame(self)
        fr_btn.grid(
            row=13,
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

        # ---- Row 14: Tracker Tools heading ----
        ttk.Label(self, text="Tracker Tools", font=FONT_H1).grid(
            row=14, column=0, columnspan=6, sticky="w", padx=16, pady=(12, 4)
        )

        # ---- Row 15: Tools row (left-aligned Update Trackers) ----
        fr_tools = ttk.Frame(self)
        fr_tools.grid(
            row=15, column=0, columnspan=6, sticky="w", padx=16, pady=(0, 8)
        )

        self.btn_update_trackers = ttk.Button(
            fr_tools,
            text="Update Trackers",
            command=self._on_update_trackers,
        )
        self.btn_update_trackers.grid(row=0, column=0, padx=(0, 8))

        # Let middle columns expand
        self.columnconfigure(1, weight=1)
        self.columnconfigure(2, weight=1)
        self.columnconfigure(3, weight=1)

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
