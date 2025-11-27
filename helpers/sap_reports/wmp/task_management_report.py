# helpers/sap_reports/wmp/task_management_report.py
from __future__ import annotations

import os
import tkinter as tk
from tkinter import ttk, filedialog

from services.db.wmp_db import fetch_order_tracking_list, default_db_path

import win32com.client
import subprocess
import time
import pyperclip

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
    "DS73"
]

class SAPDetailsDialog(tk.Toplevel):
    """
    Modal dialog to collect:
      - SAP Username
      - SAP Password (masked)
      - Destination Folder (with Browse)
      - File Name
    """
    def __init__(self, parent):
        super().__init__(parent)
        self.title("SAP Details")
        self.resizable(False, False)

        self.result = None  # (username, password, destination_folder, file_name)

        # --- Variables ---
        self.username_var = tk.StringVar()
        self.password_var = tk.StringVar()
        self.dest_folder_var = tk.StringVar()
        self.file_name_var = tk.StringVar()

        # --- Layout ---
        main = ttk.Frame(self, padding=10)
        main.grid(row=0, column=0, sticky="nsew")

        ttk.Label(main, text="SAP Username:").grid(row=0, column=0, sticky="w", padx=(0, 5), pady=3)
        ttk.Entry(main, textvariable=self.username_var, width=30).grid(row=0, column=1, columnspan=2, sticky="ew", pady=3)

        ttk.Label(main, text="SAP Password:").grid(row=1, column=0, sticky="w", padx=(0, 5), pady=3)
        ttk.Entry(main, textvariable=self.password_var, width=30, show="*").grid(
            row=1, column=1, columnspan=2, sticky="ew", pady=3
        )

        ttk.Label(main, text="Destination Folder:").grid(row=2, column=0, sticky="w", padx=(0, 5), pady=3)
        dest_entry = ttk.Entry(main, textvariable=self.dest_folder_var, width=30)
        dest_entry.grid(row=2, column=1, sticky="ew", pady=3)
        ttk.Button(main, text="Browse...", command=self._browse_folder).grid(row=2, column=2, sticky="w", padx=(5, 0), pady=3)

        ttk.Label(main, text="File Name:").grid(row=3, column=0, sticky="w", padx=(0, 5), pady=3)
        ttk.Entry(main, textvariable=self.file_name_var, width=30).grid(row=3, column=1, columnspan=2, sticky="ew", pady=3)

        btn_frame = ttk.Frame(main)
        btn_frame.grid(row=4, column=0, columnspan=3, sticky="e", pady=(10, 0))
        ttk.Button(btn_frame, text="OK", command=self._on_ok).grid(row=0, column=0, padx=(0, 5))
        ttk.Button(btn_frame, text="Cancel", command=self._on_cancel).grid(row=0, column=1)

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
        file_name = self.file_name_var.get().strip()

        if not username or not password or not dest_folder or not file_name:
            return

        if not file_name.lower().endswith(".xlsx"):
            file_name += ".xlsx"

        self.result = (username, password, dest_folder, file_name)
        self.destroy()

    def _on_cancel(self):
        self.result = None
        self.destroy()


def _ask_sap_details() -> tuple[str, str, str, str] | None:
    """Show SAPDetailsDialog and return (username, password, dest_folder, file_name) or None."""
    root = tk._default_root
    created_root = False
    if root is None:
        root = tk.Tk()
        root.withdraw()
        created_root = True

    dlg = SAPDetailsDialog(root)
    root.wait_window(dlg)

    if created_root:
        root.destroy()

    return dlg.result


def get_task_management_report() -> tuple[list[int], tuple[str, str, str, str]] | None:
    """
    Entry point for Tracker Builder button.
    For now:
      - ask SAP creds / folder / filename
      - pull Order list from order_tracking_list in WMP DB
      - print both
    Returns (orders, details) for future use, or None if cancelled.
    """
    details = _ask_sap_details()
    if details is None:
        return None

    username, password, destination_folder, file_name = details

    db_path = default_db_path()
    if not os.path.isfile(db_path):
        orders: list[int] = []
    else:
        orders = fetch_order_tracking_list(db_path)

    # # --- requested behavior: just print for now ---
    # print("SAP Details:")
    # print(f"  Username: {username}")
    # print(f"  Password: {'*' * len(password)}")
    # print(f"  Destination Folder: {destination_folder}")
    # print(f"  File Name: {file_name}")
    # print("")
    # print(f"Orders from order_tracking_list ({len(orders)}):")
    # print(orders)

    # return orders, details

    subprocess.Popen(r"C:\Program Files (x86)\SAP\FrontEnd\SAPgui\saplogon.exe")
    time.sleep(5)
    SapGuiAuto = win32com.client.GetObject("SAPGUI")
    application = SapGuiAuto.GetScriptingEngine
    connection = application.OpenConnection("Prod Work management [PR1]", True)
    session = connection.Children(0)

    session.findById("wnd[0]/usr/txtRSYST-BNAME").text = username
    session.findById("wnd[0]/usr/pwdRSYST-BCODE").text = password
    session.findById("wnd[0]").sendVKey(0)

    session.findById("wnd[0]").maximize
    session.findById("wnd[0]/tbar[0]/okcd").text = "ziwre_tm_report"
    session.findById("wnd[0]").sendVKey(0)
    session.findById("wnd[0]/usr/btn%_S_AUFNR_%_APP_%-VALU_PUSH").press()

    while session.Children.Count < 2:
        time.sleep(1)

    order_list = "\r\n".join(map(str, orders)) + "\r\n"
    pyperclip.copy(order_list)

    session.findById("wnd[1]").sendVKey(24)
    session.findById("wnd[1]/tbar[0]/btn[8]").press()

    session.findById("wnd[0]/usr/ctxtP_DATUV").text = "01/01/1900"
    session.findById("wnd[0]/usr/ctxtP_DATUV").setFocus
    session.findById("wnd[0]/usr/ctxtP_DATUV").caretPosition = 10
    session.findById("wnd[0]/usr/btn%_S_MNCOD_%_APP_%-VALU_PUSH").press()

    task_list = "\r\n".join(map(str,TASKS)) + "\r\n"
    pyperclip.copy(task_list)

    session.findById("wnd[0]/usr/btn%_S_MNCOD_%_APP_%-VALU_PUSH").press()

    while session.Children.Count < 2:
        time.sleep(1)

    session.findById("wnd[1]").sendVKey(24)
    session.findById("wnd[1]/tbar[0]/btn[8]").press()

    session.findById("wnd[0]/usr/ctxtS_TSTAT-LOW").text = ""
    session.findById("wnd[0]/usr/ctxtS_TSTAT-LOW").setFocus
    session.findById("wnd[0]/usr/ctxtS_TSTAT-LOW").caretPosition = 0
    session.findById("wnd[0]").sendVKey(8)

    session.findById("wnd[0]/usr/cntlALV_CONTAINER/shellcont/shell").pressToolbarContextButton("&MB_VARIANT")
    session.findById("wnd[0]/usr/cntlALV_CONTAINER/shellcont/shell").selectContextMenuItem("&LOAD")

    while session.Children.Count < 2:
        time.sleep(1)

    session.findById("wnd[1]/usr/ssubD0500_SUBSCREEN:SAPLSLVC_DIALOG:0501/cntlG51_CONTAINER/shellcont/shell").setCurrentCell(3,"TEXT")
    session.findById("wnd[1]/usr/ssubD0500_SUBSCREEN:SAPLSLVC_DIALOG:0501/cntlG51_CONTAINER/shellcont/shell").firstVisibleRow = 2
    session.findById("wnd[1]/usr/ssubD0500_SUBSCREEN:SAPLSLVC_DIALOG:0501/cntlG51_CONTAINER/shellcont/shell").selectedRows = "3"
    session.findById("wnd[1]/usr/ssubD0500_SUBSCREEN:SAPLSLVC_DIALOG:0501/cntlG51_CONTAINER/shellcont/shell").clickCurrentCell()
    session.findById("wnd[0]/usr/cntlALV_CONTAINER/shellcont/shell").pressToolbarContextButton("&MB_EXPORT")
    session.findById("wnd[0]/usr/cntlALV_CONTAINER/shellcont/shell").selectContextMenuItem("&XXL")
    session.findById("wnd[1]/usr/ctxtDY_PATH").text = destination_folder
    session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = file_name
    session.findById("wnd[1]/usr/ctxtDY_FILENAME").caretPosition = 8
    session.findById("wnd[1]/tbar[0]/btn[0]").press()
