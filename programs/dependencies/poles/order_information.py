# programs/dependencies/wmp/order_information.py
from __future__ import annotations
import tkinter as tk
from tkinter import ttk, messagebox

from core.base import ToolView  # Frame-like base

from helpers.poles_tracker_builder.logic import (
    fetch_mpp_first_for_order,
    fetch_sap_summary_for_order,
    fetch_epw_first_for_order,
    fetch_land_first_for_order,
    fetch_open_dependencies_for_order,  # <-- NEW
)

class Poles_Order_Information(ToolView):
    """
    Order lookup UI with four result panels (MPP, SAP, EPW, Land) in a 2×2 grid,
    plus a right-side vertical summary table that spans the full height of the grid.
    """
    def __init__(self, master=None, **kwargs):
        super().__init__(master, **kwargs)

        # =========================
        # ROW 3: Heading + Search
        # =========================
        heading_font = ("Segoe UI", 10, "bold")
        ttk.Label(self, text="Order Information", font=heading_font).grid(
            row=3, column=0, columnspan=5, sticky="w", padx=16, pady=(8, 6)  # span 5 to cover the new right column
        )

        ttk.Label(self, text="Order:").grid(row=3, column=1, sticky="e", padx=(0, 6))
        self.order_query_var = tk.StringVar()
        self.order_entry = ttk.Entry(self, textvariable=self.order_query_var, width=24)

        self.order_entry.grid(row=3, column=2, sticky="we", padx=(0, 6))
        self.order_search_btn = ttk.Button(self, text="Search", command=self._on_order_search, state="disabled")
        self.order_search_btn.grid(row=3, column=3, sticky="w", padx=16)

        self.order_entry.bind("<Return>", lambda e: self._on_order_search())
        self.order_entry.bind("<KP_Enter>", lambda e: self._on_order_search())  # numpad Enter
        self.order_query_var.trace_add("write", lambda *_: self._update_search_state())

        # Let the entry column expand
        self.columnconfigure(2, weight=1)
        # Let the results row expand
        self.rowconfigure(4, weight=1)

        # =========================
        # ROW 4: RESULTS CONTAINER
        # =========================
        # This spans ALL 5 columns (2x2 grid + right sidebar)
        self.results = ttk.Frame(self)
        self.results.grid(row=4, column=0, columnspan=5, sticky="nsew", padx=16, pady=(4, 12))

        # Inside: 3 columns -> [col0, col1] are the 2x2 grid; [col2] is the right sidebar
        self.results.columnconfigure(0, weight=1)
        self.results.columnconfigure(1, weight=1)
        self.results.columnconfigure(2, weight=0)  # sidebar fixed width look
        self.results.rowconfigure(0, weight=1)
        self.results.rowconfigure(1, weight=1)

        # 2x2 grid panels
        self.mpp_frame  = ttk.Frame(self.results)
        self.sap_frame  = ttk.Frame(self.results)
        self.epw_frame  = ttk.Frame(self.results)
        self.land_frame = ttk.Frame(self.results)

        self.mpp_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 8),  pady=(0, 8))
        self.sap_frame.grid(row=0, column=1, sticky="nsew", padx=(8, 0),  pady=(0, 8))
        self.epw_frame.grid(row=1, column=0, sticky="nsew", padx=(0, 8),  pady=(8, 0))
        self.land_frame.grid(row=1, column=1, sticky="nsew", padx=(8, 0),  pady=(8, 0))

        # Right-side vertical table (spans both rows)
        self.summary_frame = ttk.Frame(self.results)
        self.summary_frame.grid(row=0, column=2, rowspan=2, sticky="nsew", padx=(12, 0), pady=(0, 0))

        # Build panel UIs
        self._build_mpp_panel(self.mpp_frame)
        self._build_sap_panel(self.sap_frame)
        self._build_epw_panel(self.epw_frame)
        self._build_land_panel(self.land_frame)
        self._build_summary_panel(self.summary_frame)  # <-- NEW

    # -------------------------
    # Search / wiring
    # -------------------------
    def _update_search_state(self):
        q = self.order_query_var.get().strip()
        self.order_search_btn.configure(state=("normal" if q else "disabled"))

    def _on_order_search(self):
        q = self.order_query_var.get().strip()
        if not q:
            return

        # MPP
        mpp = fetch_mpp_first_for_order(q)
        if mpp:
            self._populate_mpp_table(mpp)
        else:
            self._clear_tree(self.mpp_tree)

        # SAP
        sap_df = fetch_sap_summary_for_order(q)
        if sap_df is not None and not sap_df.empty:
            self._populate_sap_table(sap_df)
        else:
            self._clear_tree(self.sap_tree)

        # EPW
        epw = fetch_epw_first_for_order(q)
        if epw:
            self._populate_field_value_tree(self.epw_tree, epw)
        else:
            self._clear_tree(self.epw_tree)

        # LAND
        land = fetch_land_first_for_order(q)
        if land:
            self._populate_field_value_tree(self.land_tree, land)
        else:
            self._clear_tree(self.land_tree)

        # SUMMARY (Right sidebar): Open Dependencies
        od = fetch_open_dependencies_for_order(q) or {}
        self._populate_summary_table(od)

        if not (mpp or (sap_df is not None and not sap_df.empty) or epw or land or od):
            messagebox.showinfo("Not Found", f"No rows found for Order '{q}' in MPP, SAP, EPW, Land, or Open Dependencies.")

    # -------------------------
    # MPP panel
    # -------------------------
    def _build_mpp_panel(self, parent: ttk.Frame):
        lf = ttk.LabelFrame(parent, text="MPP Data")
        lf.pack(fill="both", expand=True)

        cols = ("field", "value")
        self.mpp_tree = ttk.Treeview(lf, columns=cols, show="headings")
        self.mpp_tree.heading("field", text="Field")
        self.mpp_tree.heading("value", text="Value")
        self.mpp_tree.column("field", width=220, anchor="w")
        self.mpp_tree.column("value", width=380, anchor="w")
        self.mpp_tree.pack(side="left", fill="both", expand=True)

        vsb = ttk.Scrollbar(lf, orient="vertical", command=self.mpp_tree.yview)
        self.mpp_tree.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")

    def _populate_mpp_table(self, record: dict):
        self._clear_tree(self.mpp_tree)
        for k in sorted(record.keys(), key=lambda x: str(x).lower()):
            v = record.get(k, "")
            self.mpp_tree.insert("", "end", values=(k, "" if v is None else str(v)))

    # -------------------------
    # SAP panel
    # -------------------------
    def _build_sap_panel(self, parent: ttk.Frame):
        lf = ttk.LabelFrame(parent, text="SAP Data")
        lf.pack(fill="both", expand=True)

        cols = ("Code", "ActualStart", "Completed On", "TaskUsrStatus", "Completed By")
        self.sap_tree = ttk.Treeview(lf, columns=cols, show="headings")
        for col, w in [
            ("Code", 140),
            ("ActualStart", 120),
            ("Completed On", 120),
            ("TaskUsrStatus", 170),
            ("Completed By", 170),
        ]:
            self.sap_tree.heading(col, text=col)
            self.sap_tree.column(col, width=w, anchor="w")
        self.sap_tree.pack(side="left", fill="both", expand=True)

        vsb = ttk.Scrollbar(lf, orient="vertical", command=self.sap_tree.yview)
        self.sap_tree.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")

    def _populate_sap_table(self, df):
        self._clear_tree(self.sap_tree)
        for _, r in df.iterrows():
            self.sap_tree.insert(
                "",
                "end",
                values=(
                    r.get("Code", ""),
                    r.get("ActualStart", ""),
                    r.get("Completed On", ""),
                    r.get("TaskUsrStatus", ""),
                    r.get("Completed By", ""),
                ),
            )

    # -------------------------
    # EPW panel (Field | Value)
    # -------------------------
    def _build_epw_panel(self, parent: ttk.Frame):
        lf = ttk.LabelFrame(parent, text="EPW Data")
        lf.pack(fill="both", expand=True)

        cols = ("field", "value")
        self.epw_tree = ttk.Treeview(lf, columns=cols, show="headings")
        self.epw_tree.heading("field", text="Field")
        self.epw_tree.heading("value", text="Value")
        self.epw_tree.column("field", width=220, anchor="w")
        self.epw_tree.column("value", width=380, anchor="w")
        self.epw_tree.pack(side="left", fill="both", expand=True)

        vsb = ttk.Scrollbar(lf, orient="vertical", command=self.epw_tree.yview)
        self.epw_tree.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")

    # -------------------------
    # LAND panel (Field | Value)
    # -------------------------
    def _build_land_panel(self, parent: ttk.Frame):
        lf = ttk.LabelFrame(parent, text="Land Data")
        lf.pack(fill="both", expand=True)

        cols = ("field", "value")
        self.land_tree = ttk.Treeview(lf, columns=cols, show="headings")
        self.land_tree.heading("field", text="Field")
        self.land_tree.heading("value", text="Value")
        self.land_tree.column("field", width=220, anchor="w")
        self.land_tree.column("value", width=380, anchor="w")
        self.land_tree.pack(side="left", fill="both", expand=True)

        vsb = ttk.Scrollbar(lf, orient="vertical", command=self.land_tree.yview)
        self.land_tree.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")

    # -------------------------
    # SUMMARY sidebar (Field | Value)
    # -------------------------
    def _build_summary_panel(self, parent: ttk.Frame):
        lf = ttk.LabelFrame(parent, text="Order Summary")
        lf.pack(fill="both", expand=True)

        cols = ("field", "value")
        self.summary_tree = ttk.Treeview(lf, columns=cols, show="headings", height=24)
        self.summary_tree.heading("field", text="Field")
        self.summary_tree.heading("value", text="Value")
        # A bit narrower to look like a sidebar
        self.summary_tree.column("field", width=180, anchor="w")
        self.summary_tree.column("value", width=260, anchor="w")
        self.summary_tree.pack(side="left", fill="both", expand=True)

        vsb = ttk.Scrollbar(lf, orient="vertical", command=self.summary_tree.yview)
        self.summary_tree.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")

    def _populate_summary_table(self, od_record: dict):
        # Only one row for now: "Open Dependencies"
        self._clear_tree(self.summary_tree)
        val = ""
        if isinstance(od_record, dict):
            # Expecting a dict with key "Open Dependencies"
            val = od_record.get("Open Dependencies", "") or ""
        self.summary_tree.insert("", "end", values=("Open Dependencies", val))

    # -------------------------
    # Shared helpers
    # -------------------------
    @staticmethod
    def _clear_tree(tree: ttk.Treeview):
        for item in tree.get_children(""):
            tree.delete(item)

    @staticmethod
    def _populate_field_value_tree(tree: ttk.Treeview, record: dict):
        for item in tree.get_children(""):
            tree.delete(item)
        for k in sorted(record.keys(), key=lambda x: str(x).lower()):
            v = record.get(k, "")
            tree.insert("", "end", values=(k, "" if v is None else str(v)))