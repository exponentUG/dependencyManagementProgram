# programs/master_tracker_builder/order_information.py
from __future__ import annotations

import tkinter as tk
from tkinter import ttk, messagebox

from core.base import ToolView  # Frame-like base

# --- Per-tracker fetch helpers ------------------------------------
# Adjust import paths if any of these modules have different names
from helpers.wmp_tracker_builder.logic import (
    fetch_mpp_first_for_order as wmp_fetch_mpp,
    fetch_sap_summary_for_order as wmp_fetch_sap,
    fetch_epw_first_for_order as wmp_fetch_epw,
    fetch_land_first_for_order as wmp_fetch_land,
    fetch_open_dependencies_for_order as wmp_fetch_open_dep,
)

from helpers.maintenance_tracker_builder.logic import (
    fetch_mpp_first_for_order as maint_fetch_mpp,
    fetch_sap_summary_for_order as maint_fetch_sap,
    fetch_epw_first_for_order as maint_fetch_epw,
    fetch_land_first_for_order as maint_fetch_land,
    fetch_open_dependencies_for_order as maint_fetch_open_dep,
)

from helpers.maintenance_rfc_tracker_builder.logic import (
    fetch_mpp_first_for_order as maint_rfc_fetch_mpp,
    fetch_sap_summary_for_order as maint_rfc_fetch_sap,
    fetch_epw_first_for_order as maint_rfc_fetch_epw,
    fetch_land_first_for_order as maint_rfc_fetch_land,
    fetch_open_dependencies_for_order as maint_rfc_fetch_open_dep,
)

from helpers.poles_tracker_builder.logic import (
    fetch_mpp_first_for_order as poles_fetch_mpp,
    fetch_sap_summary_for_order as poles_fetch_sap,
    fetch_epw_first_for_order as poles_fetch_epw,
    fetch_land_first_for_order as poles_fetch_land,
    fetch_open_dependencies_for_order as poles_fetch_open_dep,
)

from helpers.poles_rfc_tracker_builder.logic import (
    fetch_mpp_first_for_order as poles_rfc_fetch_mpp,
    fetch_sap_summary_for_order as poles_rfc_fetch_sap,
    fetch_epw_first_for_order as poles_rfc_fetch_epw,
    fetch_land_first_for_order as poles_rfc_fetch_land,
    fetch_open_dependencies_for_order as poles_rfc_fetch_open_dep,
)

class Master_Order_Information(ToolView):
    """
    Master Order Information viewer.

    - User enters an Order number once.
    - We scan ALL trackers:
        * WMP
        * Maintenance
        * Maintenance RFC
        * Poles
        * Poles RFC
    - We pick the first tracker that has any data as the PRIMARY tracker and
      display its MPP / SAP / EPW / Land in the 2x2 grid.
    - The right-hand sidebar shows:
        * Primary tracker
        * Any other trackers where this order was also found
        * Open Dependencies (from the primary tracker)
    """

    # Priority order for choosing the "primary" tracker when an order exists
    # in multiple databases.
    TRACKERS = [
        (
            "WMP",
            {
                "mpp": wmp_fetch_mpp,
                "sap": wmp_fetch_sap,
                "epw": wmp_fetch_epw,
                "land": wmp_fetch_land,
                "open_dep": wmp_fetch_open_dep,
            },
        ),
        (
            "Maintenance",
            {
                "mpp": maint_fetch_mpp,
                "sap": maint_fetch_sap,
                "epw": maint_fetch_epw,
                "land": maint_fetch_land,
                "open_dep": maint_fetch_open_dep,
            },
        ),
        (
            "Maintenance RFC",
            {
                "mpp": maint_rfc_fetch_mpp,
                "sap": maint_rfc_fetch_sap,
                "epw": maint_rfc_fetch_epw,
                "land": maint_rfc_fetch_land,
                "open_dep": maint_rfc_fetch_open_dep,
            },
        ),
        (
            "Poles",
            {
                "mpp": poles_fetch_mpp,
                "sap": poles_fetch_sap,
                "epw": poles_fetch_epw,
                "land": poles_fetch_land,
                "open_dep": poles_fetch_open_dep,
            },
        ),
        (
            "Poles RFC",
            {
                "mpp": poles_rfc_fetch_mpp,
                "sap": poles_rfc_fetch_sap,
                "epw": poles_rfc_fetch_epw,
                "land": poles_rfc_fetch_land,
                "open_dep": poles_rfc_fetch_open_dep,
            },
        ),
    ]

    def __init__(self, master=None, **kwargs):
        super().__init__(master, **kwargs)

        # =========================
        # ROW 3: Heading + Search
        # =========================
        heading_font = ("Segoe UI", 10, "bold")
        self.heading_var = tk.StringVar(value="Order Information (no tracker selected)")
        ttk.Label(self, textvariable=self.heading_var, font=heading_font).grid(
            row=3, column=0, columnspan=5, sticky="w", padx=16, pady=(8, 6)
        )

        ttk.Label(self, text="Order:").grid(row=3, column=1, sticky="e", padx=(0, 6))
        self.order_query_var = tk.StringVar()
        self.order_entry = ttk.Entry(self, textvariable=self.order_query_var, width=24)

        self.order_entry.grid(row=3, column=2, sticky="we", padx=(0, 6))
        self.order_search_btn = ttk.Button(
            self, text="Search", command=self._on_order_search, state="disabled"
        )
        self.order_search_btn.grid(row=3, column=3, sticky="w", padx=16)

        self.order_entry.bind("<Return>", lambda e: self._on_order_search())
        self.order_entry.bind("<KP_Enter>", lambda e: self._on_order_search())
        self.order_query_var.trace_add("write", lambda *_: self._update_search_state())

        # Let the entry column expand
        self.columnconfigure(2, weight=1)
        # Let the results row expand
        self.rowconfigure(4, weight=1)

        # =========================
        # ROW 4: RESULTS CONTAINER
        # =========================
        self.results = ttk.Frame(self)
        self.results.grid(
            row=4, column=0, columnspan=5, sticky="nsew", padx=16, pady=(4, 12)
        )

        # Inside: 3 columns -> [col0, col1] grid; [col2] right sidebar
        self.results.columnconfigure(0, weight=1)
        self.results.columnconfigure(1, weight=1)
        self.results.columnconfigure(2, weight=0)
        self.results.rowconfigure(0, weight=1)
        self.results.rowconfigure(1, weight=1)

        # 2x2 grid panels
        self.mpp_frame = ttk.Frame(self.results)
        self.sap_frame = ttk.Frame(self.results)
        self.epw_frame = ttk.Frame(self.results)
        self.land_frame = ttk.Frame(self.results)

        self.mpp_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 8), pady=(0, 8))
        self.sap_frame.grid(row=0, column=1, sticky="nsew", padx=(8, 0), pady=(0, 8))
        self.epw_frame.grid(row=1, column=0, sticky="nsew", padx=(0, 8), pady=(8, 0))
        self.land_frame.grid(row=1, column=1, sticky="nsew", padx=(8, 0), pady=(8, 0))

        # Right-side vertical table (spans both rows)
        self.summary_frame = ttk.Frame(self.results)
        self.summary_frame.grid(
            row=0, column=2, rowspan=2, sticky="nsew", padx=(12, 0), pady=(0, 0)
        )

        # Build panel UIs
        self._build_mpp_panel(self.mpp_frame)
        self._build_sap_panel(self.sap_frame)
        self._build_epw_panel(self.epw_frame)
        self._build_land_panel(self.land_frame)
        self._build_summary_panel(self.summary_frame)

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

        # Clear old results first
        self._clear_tree(self.mpp_tree)
        self._clear_tree(self.sap_tree)
        self._clear_tree(self.epw_tree)
        self._clear_tree(self.land_tree)
        self._clear_tree(self.summary_tree)
        self.heading_var.set("Order Information (searching…)")

        hits = []

        # Scan all trackers
        for label, funcs in self.TRACKERS:
            try:
                mpp = funcs["mpp"](q)
                sap_df = funcs["sap"](q)
                epw = funcs["epw"](q)
                land = funcs["land"](q)
                od = funcs["open_dep"](q)
            except Exception:
                # If any tracker blows up, just skip it (we don't want to
                # break master view because one DB is cranky).
                continue

            has_sap = sap_df is not None and hasattr(sap_df, "empty") and not sap_df.empty
            if mpp or has_sap or epw or land or (od or {}):
                hits.append(
                    {
                        "label": label,
                        "mpp": mpp,
                        "sap_df": sap_df,
                        "epw": epw,
                        "land": land,
                        "od": od,
                    }
                )

        if not hits:
            self.heading_var.set("Order Information (no tracker selected)")
            messagebox.showinfo(
                "Not Found",
                f"No rows found for Order '{q}' in any tracker "
                "(WMP, Maintenance, Maintenance RFC, Poles, Poles RFC).",
            )
            return

        # Pick the first hit as the PRIMARY tracker per priority order
        primary = hits[0]
        primary_label = primary["label"]

        self.heading_var.set(f"Order Information – {primary_label}")

        # Populate MPP
        if primary["mpp"]:
            self._populate_mpp_table(primary["mpp"])
        else:
            self._clear_tree(self.mpp_tree)

        # Populate SAP
        sap_df = primary["sap_df"]
        if sap_df is not None and hasattr(sap_df, "empty") and not sap_df.empty:
            self._populate_sap_table(sap_df)
        else:
            self._clear_tree(self.sap_tree)

        # Populate EPW
        if primary["epw"]:
            self._populate_field_value_tree(self.epw_tree, primary["epw"])
        else:
            self._clear_tree(self.epw_tree)

        # Populate LAND
        if primary["land"]:
            self._populate_field_value_tree(self.land_tree, primary["land"])
        else:
            self._clear_tree(self.land_tree)

        # Populate SUMMARY sidebar: which trackers had hits + Open Dependencies
        self._populate_summary_table(primary_label, primary["od"], hits)

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
        self.summary_tree.column("field", width=180, anchor="w")
        self.summary_tree.column("value", width=260, anchor="w")
        self.summary_tree.pack(side="left", fill="both", expand=True)

        vsb = ttk.Scrollbar(lf, orient="vertical", command=self.summary_tree.yview)
        self.summary_tree.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")

    def _populate_summary_table(self, primary_label: str, od_record: dict, hits: list[dict]):
        self._clear_tree(self.summary_tree)

        # Primary tracker
        self.summary_tree.insert("", "end", values=("Primary Tracker", primary_label))

        # Other trackers where this order was found
        other_labels = [h["label"] for h in hits[1:]]
        if other_labels:
            self.summary_tree.insert(
                "", "end", values=("Also found in", ", ".join(other_labels))
            )

        # Spacer row
        self.summary_tree.insert("", "end", values=("", ""))

        # Open Dependencies from the primary tracker
        val = ""
        if isinstance(od_record, dict):
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
