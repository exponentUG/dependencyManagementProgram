# programs/master_tracker_builder/tracker_builder.py
from __future__ import annotations

import os
import threading
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from typing import Any, Dict, List

from core.base import ToolView

# DB modules for each dependency tracker
# Make sure these modules exist with the expected API:
#   - load_and_filter_csv(csv_path: str) -> pd.DataFrame
#   - replace_mpp_data(df: pd.DataFrame) -> int
#   - update_order_tracking_list_from_mpp() -> tuple[int, int]
from services.db import wmp_db, maintenance_db, poles_db, poles_rfc_db

# SAP multi-export helper (new)
from helpers.sap_reports.master_tracker_builder.task_management_master import (
    run_multi_tm_export,
)


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
    for ALL dependency trackers (WMP, Maintenance, Poles, Poles RFC) in one go.

    Step 2 (Row 6): Extract SAP Data for all trackers via SAP GUI automation.
    """

    def __init__(self, master: tk.Misc | None = None, **kwargs: Any) -> None:
        super().__init__(master, **kwargs)

        self.path_var = tk.StringVar()
        self.var_sap = tk.StringVar()  # used to display / prefill destination folder
        self._is_running = False

        self._build_ui()
        self._wire_signals()

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------
    def _build_ui(self) -> None:
        self.columnconfigure(0, weight=1)

        # ---- Row 3: Step 1 title (mirrors the other tracker_builder UIs) ----
        ttk.Label(
            self,
            text="Step 1: Upload latest copy of MPP and update all tracker databases",
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

        # Button text changed from "Generate Order List" -> "Update MPP Database"
        self.generate_btn = ttk.Button(
            fr4,
            text="Update MPP Database",
            command=self._on_generate,
            state="disabled",
        )
        self.generate_btn.grid(row=0, column=3, sticky="w")

        # ---- Row 6: SAP Data (all trackers) ----
        # Mirrors the WMP tracker row-6 layout, but here:
        #   - Entry shows the destination folder (if known)
        #   - Browse lets user pick a default folder
        #   - "Extract SAP Data" runs SAP automation for all 4 DBs
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

        # Let middle columns expand
        self.columnconfigure(1, weight=1)
        self.columnconfigure(2, weight=1)
        self.columnconfigure(3, weight=1)

    def _wire_signals(self) -> None:
        self.path_var.trace_add("write", lambda *_: self._update_generate_state())

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _browse_file(self) -> None:
        fp = filedialog.askopenfilename(
            title="Select MPP CSV",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
        if fp:
            self.path_var.set(fp)

    def _browse_sap_folder(self) -> None:
        folder = filedialog.askdirectory(title="Select SAP Export Folder")
        if folder:
            self.var_sap.set(folder)

    def _update_generate_state(self) -> None:
        if self._is_running:
            # Don't re-enable while a run is in progress
            return
        path = self.path_var.get().strip()
        ok = bool(path) and os.path.isfile(path)
        self.generate_btn.configure(state=("normal" if ok else "disabled"))

    # ------------------------------------------------------------------
    # Core action: update all MPP DBs (Step 1)
    # ------------------------------------------------------------------
    def _on_generate(self) -> None:
        path = self.path_var.get().strip()
        if not path:
            messagebox.showwarning(
                "No file selected",
                "Please choose a CSV file first.",
            )
            return

        if not os.path.isfile(path):
            messagebox.showerror(
                "Invalid file",
                f"File not found:\n{path}",
            )
            return

        # Mark UI as busy
        self._is_running = True
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
                    existing_before, inserted = db_mod.update_order_tracking_list_from_mpp()

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
                self._is_running = False
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
    # Step 2: Extract SAP Data for all trackers
    # ------------------------------------------------------------------
    def _on_extract_sap_data(self) -> None:
        """
        Button handler for 'Extract SAP Data' (Master).

        - Shows SAPDetailsDialog (username, password, destination folder).
        - Opens SAP once, logs in once.
        - Loops over Maintenance, Poles, Poles RFC, WMP:
            * pulls each DB's order_tracking_list
            * runs ZIWRE_TM_REPORT
            * exports XXL to a default file name based on today's date
            * sends VKey(15) between runs.
        """
        dest_hint = self.var_sap.get().strip() or None

        try:
            result = run_multi_tm_export(self, initial_dest=dest_hint)
        except Exception as e:
            messagebox.showerror(
                "Extract SAP Data Failed",
                f"{type(e).__name__}: {e}",
            )
            return

        if not result:
            # User likely cancelled the SAP details dialog
            return

        destination = result.get("destination", "")
        files = result.get("files", [])

        if destination:
            self.var_sap.set(destination)

        if files:
            msg_lines = ["Exported the following files:", ""]
            for f in files:
                msg_lines.append(f"• {os.path.join(destination, f)}")
            messagebox.showinfo("SAP Export Complete", "\n".join(msg_lines))
        else:
            messagebox.showinfo(
                "SAP Export Complete",
                "No files were exported (no orders found in the trackers).",
            )
