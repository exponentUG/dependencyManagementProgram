# helpers/sap_reports/master_tracker_builder/task_management_master.py
from __future__ import annotations

import os
import time
import subprocess
from datetime import datetime
from typing import Any, Dict, List, Tuple, Optional

import tkinter as tk
from tkinter import ttk, filedialog

import win32com.client
import pyperclip

from services.db import wmp_db, maintenance_db, poles_db, poles_rfc_db


TASKS = [
    "SP56",
    "RP56",
    "SP57",
    "RP57",
    "DS42",
    "PC20",
    "DS76",
    "PC24",
    "DS11",
    "PC21",
    "AP10",
    "AP25",
    "DS28",
    "DS73",
]


class SAPDetailsDialog(tk.Toplevel):
    """
    Modal dialog to collect:
      - SAP Username
      - SAP Password (masked)
      - Destination Folder (with Browse)
    (File name is derived automatically per tracker.)
    """

    def __init__(self, parent: tk.Misc, initial_dest: Optional[str] = None):
        super().__init__(parent)
        self.title("SAP Details")
        self.resizable(False, False)

        self.result: Optional[Tuple[str, str, str]] = None  # (username, password, dest_folder)

        # --- Variables ---
        self.username_var = tk.StringVar()
        self.password_var = tk.StringVar()
        self.dest_folder_var = tk.StringVar(value=initial_dest or "")

        # --- Layout ---
        main = ttk.Frame(self, padding=10)
        main.grid(row=0, column=0, sticky="nsew")

        ttk.Label(main, text="SAP Username:").grid(
            row=0, column=0, sticky="w", padx=(0, 5), pady=3
        )
        ttk.Entry(main, textvariable=self.username_var, width=30).grid(
            row=0, column=1, columnspan=2, sticky="ew", pady=3
        )

        ttk.Label(main, text="SAP Password:").grid(
            row=1, column=0, sticky="w", padx=(0, 5), pady=3
        )
        ttk.Entry(main, textvariable=self.password_var, width=30, show="*").grid(
            row=1, column=1, columnspan=2, sticky="ew", pady=3
        )

        ttk.Label(main, text="Destination Folder:").grid(
            row=2, column=0, sticky="w", padx=(0, 5), pady=3
        )
        dest_entry = ttk.Entry(main, textvariable=self.dest_folder_var, width=30)
        dest_entry.grid(row=2, column=1, sticky="ew", pady=3)
        ttk.Button(main, text="Browse...", command=self._browse_folder).grid(
            row=2, column=2, sticky="w", padx=(5, 0), pady=3
        )

        btn_frame = ttk.Frame(main)
        btn_frame.grid(row=3, column=0, columnspan=3, sticky="e", pady=(10, 0))
        ttk.Button(btn_frame, text="OK", command=self._on_ok).grid(
            row=0, column=0, padx=(0, 5)
        )
        ttk.Button(btn_frame, text="Cancel", command=self._on_cancel).grid(
            row=0, column=1
        )

        main.columnconfigure(1, weight=1)

        self.transient(parent)
        self.grab_set()
        self.protocol("WM_DELETE_WINDOW", self._on_cancel)

        # Center over parent
        self.update_idletasks()
        if parent is not None and parent.winfo_ismapped():
            px = parent.winfo_rootx()
            py = parent.winfo_rooty()
            pw = parent.winfo_width()
            ph = parent.winfo_height()
            ww = self.winfo_width()
            wh = self.winfo_height()
            x = px + (pw - ww) // 2
            y = py + (ph - wh) // 2
            self.geometry(f"+{x}+{y}")

    def _browse_folder(self):
        folder = filedialog.askdirectory(title="Select Destination Folder")
        if folder:
            self.dest_folder_var.set(folder)

    def _on_ok(self):
        username = self.username_var.get().strip()
        password = self.password_var.get().strip()
        dest_folder = self.dest_folder_var.get().strip()

        if not username or not password or not dest_folder:
            # Simple validation: all three required
            return

        self.result = (username, password, dest_folder)
        self.destroy()

    def _on_cancel(self):
        self.result = None
        self.destroy()


def _run_tm_for_orders(session, orders: List[int], destination_folder: str, file_name: str):
    """
    Run the ZIWRE_TM_REPORT for a given order list and export to Excel (XXL)
    in destination_folder / file_name.

    Assumes the user is already logged into SAP and we are at the main screen.
    """
    # Go to transaction
    session.findById("wnd[0]/tbar[0]/okcd").text = "ziwre_tm_report"
    session.findById("wnd[0]").sendVKey(0)

    # Open order multi-select
    session.findById("wnd[0]/usr/btn%_S_AUFNR_%_APP_%-VALU_PUSH").press()

    # Wait for popup
    while session.Children.Count < 2:
        time.sleep(5)

    # Paste orders into selection
    order_list = "\r\n".join(map(str, orders)) + "\r\n"
    pyperclip.copy(order_list)

    session.findById("wnd[1]").sendVKey(24)
    session.findById("wnd[1]/tbar[0]/btn[8]").press()

    # Set date and tasks
    session.findById("wnd[0]/usr/ctxtP_DATUV").text = "01/01/1900"
    session.findById("wnd[0]/usr/ctxtP_DATUV").setFocus
    session.findById("wnd[0]/usr/ctxtP_DATUV").caretPosition = 10
    session.findById("wnd[0]/usr/btn%_S_MNCOD_%_APP_%-VALU_PUSH").press()

    task_list = "\r\n".join(map(str, TASKS)) + "\r\n"
    pyperclip.copy(task_list)

    session.findById("wnd[0]/usr/btn%_S_MNCOD_%_APP_%-VALU_PUSH").press()

    while session.Children.Count < 2:
        time.sleep(5)

    session.findById("wnd[1]").sendVKey(24)
    session.findById("wnd[1]/tbar[0]/btn[8]").press()

    # Clear Task Status and run
    session.findById("wnd[0]/usr/ctxtS_TSTAT-LOW").text = ""
    session.findById("wnd[0]/usr/ctxtS_TSTAT-LOW").setFocus
    session.findById("wnd[0]/usr/ctxtS_TSTAT-LOW").caretPosition = 0
    session.findById("wnd[0]").sendVKey(8)

    # Load variant
    session.findById("wnd[0]/usr/cntlALV_CONTAINER/shellcont/shell").pressToolbarContextButton("&MB_VARIANT")
    session.findById("wnd[0]/usr/cntlALV_CONTAINER/shellcont/shell").selectContextMenuItem("&LOAD")

    while session.Children.Count < 2:
        time.sleep(5)

    shell = session.findById(
        "wnd[1]/usr/ssubD0500_SUBSCREEN:SAPLSLVC_DIALOG:0501/"
        "cntlG51_CONTAINER/shellcont/shell"
    )
    shell.setCurrentCell(3, "TEXT")
    shell.firstVisibleRow = 2
    shell.selectedRows = "3"
    shell.clickCurrentCell()

    # Export to Excel
    session.findById("wnd[0]/usr/cntlALV_CONTAINER/shellcont/shell").pressToolbarContextButton("&MB_EXPORT")
    session.findById("wnd[0]/usr/cntlALV_CONTAINER/shellcont/shell").selectContextMenuItem("&XXL")

    session.findById("wnd[1]/usr/ctxtDY_PATH").text = destination_folder
    session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = file_name
    session.findById("wnd[1]/usr/ctxtDY_FILENAME").caretPosition = len(file_name)
    session.findById("wnd[1]/tbar[0]/btn[0]").press()

    # NEW: ensure we return to the starting point before the next loop
    session.findById("wnd[0]").sendVKey(15)


def run_multi_tm_export(parent: tk.Misc, initial_dest: Optional[str] = None) -> Dict[str, Any]:
    """
    Main entry point for MASTER_TRACKER_BUILDER 'Extract SAP Data'.

    Flow:
      - Ask for SAP username/password and destination folder (no filename).
      - Open SAP GUI and log in ONCE.
      - For each DB (Maintenance, Poles, Poles RFC, WMP):
          * Pull order list from that DB's order_tracking_list
          * Build a default file name based on today's date
          * Run ZIWRE_TM_REPORT and export XXL Excel to that file
          * After each export, send VKey(15) to reset

    Returns:
      {
        "destination": <folder>,
        "files": [ "Maintenance SAP Data - MM/DD/YYYY.xlsx", ... ]
      }
      or {} if user cancels.
    """
    # --- Ask for SAP details ---
    dlg = SAPDetailsDialog(parent, initial_dest=initial_dest)
    parent.wait_window(dlg)
    if dlg.result is None:
        return {}

    username, password, destination_folder = dlg.result

    # Ensure destination exists
    os.makedirs(destination_folder, exist_ok=True)

    # --- Start SAP and log in ONCE ---
    subprocess.Popen(r"C:\Program Files (x86)\SAP\FrontEnd\SAPgui\saplogon.exe")
    time.sleep(5)

    SapGuiAuto = win32com.client.GetObject("SAPGUI")
    application = SapGuiAuto.GetScriptingEngine
    connection = application.OpenConnection("Prod Work management [PR1]", True)
    session = connection.Children(0)

    session.findById("wnd[0]/usr/txtRSYST-BNAME").text = username
    session.findById("wnd[0]/usr/pwdRSYST-BCODE").text = password
    session.findById("wnd[0]").sendVKey(0)

    session.findById("wnd[0]").maximize()

    # --- Prepare trackers & filenames ---
    today_str = datetime.today().strftime("%m%d%Y")  # MMDDYYYY

    trackers: List[Tuple[str, Any]] = [
        ("Maintenance", maintenance_db),
        ("Poles", poles_db),
        ("Poles RFC", poles_rfc_db),
        ("WMP", wmp_db),
    ]

    file_name_map = {
        "Maintenance": f"Maintenance SAP Data - {today_str}.xlsx",
        "Poles": f"Poles SAP Data - {today_str}.xlsx",
        "Poles RFC": f"Poles RFC SAP Data - {today_str}.xlsx",
        "WMP": f"WMP SAP Data - {today_str}.xlsx",
    }

    created_files: List[str] = []

    for label, db_mod in trackers:
        db_path = db_mod.default_db_path()
        if not os.path.isfile(db_path):
            orders: List[int] = []
        else:
            # Make sure we refresh from each DB's own order_tracking_list
            orders = db_mod.fetch_order_tracking_list(db_path)

        if not orders:
            # If a DB has no orders, just skip quietly
            continue

        file_name = file_name_map.get(label, f"{label} SAP Data - {today_str}.xlsx")

        _run_tm_for_orders(
            session=session,
            orders=orders,
            destination_folder=destination_folder,
            file_name=file_name,
        )
        created_files.append(file_name)

    return {
        "destination": destination_folder,
        "files": created_files,
    }
