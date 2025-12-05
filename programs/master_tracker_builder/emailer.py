from __future__ import annotations

import os
import sqlite3
from typing import Any, List

import pandas as pd
import tkinter as tk
from tkinter import ttk, messagebox

from core.base import ToolView, FONT_H1, FONT_H2
from services.db import (
    wmp_db,
    maintenance_db,
    poles_db,
    poles_rfc_db,
    maintenance_rfc_db,
)

from helpers.emailHelpers.email import df_to_excelish_html

# Path to static lists DB (for pm_list)
STATIC_LISTS_DB_PATH = os.path.join("data", "static_lists.sqlite3")

# Columns for the joint pole email/table
JP_COLUMNS: List[str] = [
    "Order",
    "Notification Status",
    "SAP Status",
    "DS42",
    "PC20",
    "Primary Intent Status",
    "Status Date",
    "Due By",
    "Action",
]

# Columns for the basic permit email/table
PERMIT_COLUMNS: List[str] = [
    "Order",
    "Notification Status",
    "SAP Status",
    "SP56 Status",
    "RP56 Status",
    "E Permit Status",
    "Submit Days",
    "Permit Expiration Date",
    "Work Plan Date",
    "CLICK Start Date",
    "CLICK End Date",
    "LEAPS Cycle Time",
    "Action",
]

# Columns for the "Permit expired. Need CLICK Date for extension." email/table
PERMIT_CLICK_COLUMNS: List[str] = [
    "Order",
    "Notification Status",
    "MAT",
    "Program Manager",
    "LAN ID",
    "SAP Status",
    "SP56 Status",
    "RP56 Status",
    "E Permit Status",
    "Submit Days",
    "Permit Expiration Date",
    "Work Plan Date",
    "CLICK Start Date",
    "CLICK End Date",
    "LEAPS Cycle Time",
    "Action",
]

# Email category labels
EMAIL_CATEGORIES = [
    "Joint Pole: Request to complete PC20",
    "Joint Pole: Intent in draft. Request for updated.",
    "Permit: Request for Task Completion",
    "Permit: Permit expired. Please request for extension.",
    "Permit: Permit expired. Need CLICK Date for extension.",
]


class Master_Emailer(ToolView):
    """
    Master Emailer

    Categories supported:
      1) "Joint Pole: Request to complete PC20"
         - joint_pole_tracker rows where Action is either:
             * 'Released to construction. Please complete PC20.'
             * 'OU days exceeded. Please complete PC20.'

      2) "Joint Pole: Intent in draft. Request for updated."
         - joint_pole_tracker rows where Action =
             'Intent in draft. Please review and provide update.'

      3) "Permit: Request for Task Completion"
         - permit_tracker rows where Action is either:
             * 'Please confirm permit is approved and complete SAP task.'
             * 'Permit not needed. Please close SP/RP56.'

      4) "Permit: Permit expired. Please request for extension."
         - permit_tracker rows where Action =
             'Permit expired. Please request for extension.'

      5) "Permit: Permit expired. Need CLICK Date for extension."
         - permit_tracker rows where Action =
             'Permit expired. Please provide CLICK Date for extension.'
         - Enriched with MAT and Program Manager / LAN ID (from pm_list).
    """

    def __init__(self, master: tk.Misc | None = None, **kwargs: Any) -> None:
        super().__init__(master, **kwargs)

        # --- state ---
        self.category_var = tk.StringVar()
        self.to_var = tk.StringVar()
        self.cc_var = tk.StringVar()
        self.subject_var = tk.StringVar()

        self._df_current: pd.DataFrame | None = None
        self._current_columns: List[str] = JP_COLUMNS.copy()

        # widgets
        self.body_text: tk.Text | None = None
        self.tree: ttk.Treeview | None = None
        self.btn_send: ttk.Button | None = None

        self._build_ui()
        self._wire_signals()

        # Default to first category
        if EMAIL_CATEGORIES:
            self.category_var.set(EMAIL_CATEGORIES[0])
            self._apply_template_for_category()
            self._refresh_table_for_category()

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------
    def _build_ui(self) -> None:
        self.columnconfigure(0, weight=1)

        label_width = 16

        #
        # Row 3: Email Category
        #
        fr3 = ttk.Frame(self)
        fr3.grid(row=3, column=0, sticky="ew", padx=16, pady=(8, 4))
        fr3.columnconfigure(1, weight=1)

        ttk.Label(fr3, text="Email Category:", width=label_width).grid(
            row=0, column=0, sticky="w", padx=(0, 8)
        )
        category_dd = ttk.Combobox(
            fr3,
            textvariable=self.category_var,
            state="readonly",
            values=EMAIL_CATEGORIES,
            width=40,
        )
        category_dd.grid(row=0, column=1, sticky="w")
        self.category_dd = category_dd  # keep reference

        #
        # Row 4: To
        #
        fr4 = ttk.Frame(self)
        fr4.grid(row=4, column=0, sticky="ew", padx=16, pady=2)
        fr4.columnconfigure(1, weight=1)

        ttk.Label(fr4, text="To:", width=label_width).grid(
            row=0, column=0, sticky="w", padx=(0, 8)
        )
        ttk.Entry(fr4, textvariable=self.to_var).grid(
            row=0, column=1, sticky="ew"
        )

        #
        # Row 5: CC
        #
        fr5 = ttk.Frame(self)
        fr5.grid(row=5, column=0, sticky="ew", padx=16, pady=2)
        fr5.columnconfigure(1, weight=1)

        ttk.Label(fr5, text="CC:", width=label_width).grid(
            row=0, column=0, sticky="w", padx=(0, 8)
        )
        ttk.Entry(fr5, textvariable=self.cc_var).grid(
            row=0, column=1, sticky="ew"
        )

        #
        # Row 6: Subject
        #
        fr6 = ttk.Frame(self)
        fr6.grid(row=6, column=0, sticky="ew", padx=16, pady=2)
        fr6.columnconfigure(1, weight=1)

        ttk.Label(fr6, text="Subject:", width=label_width).grid(
            row=0, column=0, sticky="w", padx=(0, 8)
        )
        ttk.Entry(fr6, textvariable=self.subject_var).grid(
            row=0, column=1, sticky="ew"
        )

        #
        # Row 7: Body label
        #
        ttk.Label(
            self,
            text="Body:",
            font=FONT_H2,
        ).grid(row=7, column=0, sticky="w", padx=16, pady=(8, 2))

        #
        # Row 8: Body Text (multi-line)
        #
        fr8 = ttk.Frame(self)
        fr8.grid(row=8, column=0, sticky="nsew", padx=16, pady=(0, 8))
        fr8.columnconfigure(0, weight=1)
        fr8.rowconfigure(0, weight=1)

        body = tk.Text(fr8, height=5, wrap="word")
        body.grid(row=0, column=0, sticky="nsew")
        sb_body = ttk.Scrollbar(fr8, orient="vertical", command=body.yview)
        sb_body.grid(row=0, column=1, sticky="ns")
        body.configure(yscrollcommand=sb_body.set)
        self.body_text = body

        #
        # Row 9: Treeview table
        #
        lf = ttk.LabelFrame(self, text="Orders")
        lf.grid(row=9, column=0, sticky="nsew", padx=16, pady=(0, 8))
        lf.columnconfigure(0, weight=1)
        lf.rowconfigure(0, weight=1)

        tree = ttk.Treeview(lf, columns=self._current_columns, show="headings", height=10)
        for col in self._current_columns:
            tree.heading(col, text=col)
            base = 120
            if col == "Order":
                base = 100
            if col in ("Primary Intent Status", "Action"):
                base = 260
            tree.column(col, width=base, anchor="w", stretch=True)

        vsb = ttk.Scrollbar(lf, orient="vertical", command=tree.yview)
        hsb = ttk.Scrollbar(lf, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")

        self.tree = tree

        # Allow tree area to expand
        self.rowconfigure(9, weight=1)

        #
        # Row 10: Send Email button
        #
        fr10 = ttk.Frame(self)
        fr10.grid(row=10, column=0, sticky="e", padx=16, pady=(4, 12))

        self.btn_send = ttk.Button(
            fr10,
            text="Send Email",
            command=self._on_send_email,
            state="disabled",
        )
        self.btn_send.grid(row=0, column=0, sticky="e")

    def _wire_signals(self) -> None:
        self.category_dd.bind("<<ComboboxSelected>>", self._on_category_changed)

        # Update send button whenever To / Subject change
        self.to_var.trace_add("write", lambda *_: self._update_send_state())
        self.subject_var.trace_add("write", lambda *_: self._update_send_state())

    # ------------------------------------------------------------------
    # Category handling
    # ------------------------------------------------------------------
    def _on_category_changed(self, _event: Any) -> None:
        self._apply_template_for_category()
        self._refresh_table_for_category()

    def _apply_template_for_category(self) -> None:
        """
        Auto-fill To / CC / Subject / Body when the category changes.
        """
        cat = (self.category_var.get() or "").strip()

        if cat == "Joint Pole: Request to complete PC20":
            # Defaults you specified
            self.to_var.set("pjcw@pge.com")
            self.cc_var.set("p6b6@pge.com; pusd@pge.com; s2f6@pge.com")
            self.subject_var.set("Request for PC20 Task Closure")

            if self.body_text is not None:
                self.body_text.delete("1.0", "end")
                self.body_text.insert(
                    "1.0",
                    "Hi Pam,\n\n"
                    "I hope you are doing well. Can you please review the order(s) "
                    "below and complete the PC20 task for them in SAP.\n\n",
                )

        elif cat == "Joint Pole: Intent in draft. Request for updated.":
            self.to_var.set("svbm@pge.com")
            self.cc_var.set("p6b6@pge.com; pusd@pge.com; s2f6@pge.com")
            self.subject_var.set("Request for Update on Joint Pole Dependencies")

            if self.body_text is not None:
                self.body_text.delete("1.0", "end")
                self.body_text.insert(
                    "1.0",
                    "Hi Stuti,\n\n"
                    "I hope you are doing well. The following order(s) has/have the "
                    'Primiary Intent Status marked as "DRAFT". Can you please review '
                    "them and provide updates?\n\n",
                )

        elif cat == "Permit: Request for Task Completion":
            self.to_var.set("svbm@pge.com")
            self.cc_var.set("p6b6@pge.com; pusd@pge.com; s2f6@pge.com")
            self.subject_var.set("Request for SP56/RP56 Task Closure")

            if self.body_text is not None:
                self.body_text.delete("1.0", "end")
                self.body_text.insert(
                    "1.0",
                    "Hi Brett,\n\n"
                    "I hope you are doing well. The following order(s) has/have been "
                    "flagged as permit not needed, or permit approved, but the SP56/RP56 "
                    "tasks are still open. Can you please review them and complete the "
                    "tasks? Please let us know if anything else is pending for this/these "
                    "order(s).\n\n",
                )

        elif cat == "Permit: Permit expired. Please request for extension.":
            self.to_var.set("svbm@pge.com")
            self.cc_var.set("p6b6@pge.com; pusd@pge.com; s2f6@pge.com")
            self.subject_var.set("Request for Extension for Expired Permit")

            if self.body_text is not None:
                self.body_text.delete("1.0", "end")
                self.body_text.insert(
                    "1.0",
                    "Hi Brett,\n\n"
                    "I hope you are doing well. The following order(s) has/have expired "
                    "permits, and have CLICK Start Date(s) coming up in the next 90 days. "
                    "Can you please request for an extension for this/them?\n\n",
                )

        elif cat == "Permit: Permit expired. Need CLICK Date for extension.":
            # We'll decide To/CC/Subject/Body after we lock down the recipients logic
            self.to_var.set("")
            self.cc_var.set("")
            self.subject_var.set("")
            if self.body_text is not None:
                self.body_text.delete("1.0", "end")

        else:
            # Fallback for any unknown future category
            self.to_var.set("")
            self.cc_var.set("")
            self.subject_var.set("")
            if self.body_text is not None:
                self.body_text.delete("1.0", "end")

        self._update_send_state()

    # ------------------------------------------------------------------
    # Data loading / aggregation
    # ------------------------------------------------------------------
    def _refresh_table_for_category(self) -> None:
        cat = (self.category_var.get() or "").strip()

        if cat == "Joint Pole: Request to complete PC20":
            self._current_columns = JP_COLUMNS.copy()
            df = self._load_joint_pole_df_for_actions(
                [
                    "Released to construction. Please complete PC20.",
                    "OU days exceeded. Please complete PC20.",
                ]
            )
        elif cat == "Joint Pole: Intent in draft. Request for updated.":
            self._current_columns = JP_COLUMNS.copy()
            df = self._load_joint_pole_df_for_actions(
                ["Intent in draft. Please review and provide update."]
            )
        elif cat == "Permit: Request for Task Completion":
            self._current_columns = PERMIT_COLUMNS.copy()
            df = self._load_permit_df_for_actions(
                [
                    "Please confirm permit is approved and complete SAP task.",
                    "Permit not needed. Please close SP/RP56.",
                ]
            )
        elif cat == "Permit: Permit expired. Please request for extension.":
            self._current_columns = PERMIT_COLUMNS.copy()
            df = self._load_permit_df_for_actions(
                ["Permit expired. Please request for extension."]
            )
        elif cat == "Permit: Permit expired. Need CLICK Date for extension.":
            self._current_columns = PERMIT_CLICK_COLUMNS.copy()
            df = self._load_permit_expired_need_click_df()
        else:
            self._current_columns = JP_COLUMNS.copy()
            df = pd.DataFrame(columns=self._current_columns)

        self._df_current = df
        self._populate_tree_from_df(df)
        self._update_send_state()

    def _load_joint_pole_df_for_actions(self, action_filters: list[str]) -> pd.DataFrame:
        """
        Scan all 5 program DBs (where available) and pull rows from joint_pole_tracker
        where Action is in the provided list of action_filters.
        Returns a single combined DataFrame with columns JP_COLUMNS.
        """
        if not action_filters:
            return pd.DataFrame(columns=JP_COLUMNS)

        db_modules = [
            wmp_db,
            maintenance_db,
            maintenance_rfc_db,
            poles_db,
            poles_rfc_db,
        ]

        frames: list[pd.DataFrame] = []

        for db_mod in db_modules:
            db_path = db_mod.default_db_path()
            if not db_path or not os.path.isfile(db_path):
                continue

            try:
                with sqlite3.connect(db_path) as conn:
                    cur = conn.cursor()
                    # Check table exists
                    cur.execute(
                        """
                        SELECT name FROM sqlite_master
                        WHERE type='table' AND name='joint_pole_tracker'
                        """
                    )
                    if cur.fetchone() is None:
                        continue

                    placeholders = ",".join("?" for _ in action_filters)
                    query = f"""
                        SELECT
                            "Order",
                            "Notification Status",
                            "SAP Status",
                            "DS42",
                            "PC20",
                            "Primary Intent Status",
                            "Status Date",
                            "Due By",
                            "Action"
                        FROM joint_pole_tracker
                        WHERE "Action" IN ({placeholders})
                    """
                    df = pd.read_sql_query(query, conn, params=action_filters)
                    if not df.empty:
                        # enforce column order and add any missing columns as blank
                        for col in JP_COLUMNS:
                            if col not in df.columns:
                                df[col] = ""
                        df = df[JP_COLUMNS].copy()
                        frames.append(df)
            except Exception as e:
                print(f"[Master_Emailer] Error reading {db_path}: {type(e).__name__}: {e}")

        if not frames:
            return pd.DataFrame(columns=JP_COLUMNS)

        combined = pd.concat(frames, ignore_index=True)

        # Sort by Order for nicer display if numeric-ish
        try:
            combined["Order_sort"] = pd.to_numeric(combined["Order"], errors="coerce")
            combined = combined.sort_values(["Order_sort", "Order"]).drop(
                columns=["Order_sort"]
            )
        except Exception:
            pass

        return combined

    def _load_permit_df_for_actions(self, action_filters: list[str]) -> pd.DataFrame:
        """
        Scan all 5 program DBs (where available) and pull rows from permit_tracker
        where Action is in the provided list of action_filters.
        Returns a single combined DataFrame with columns PERMIT_COLUMNS.
        """
        if not action_filters:
            return pd.DataFrame(columns=PERMIT_COLUMNS)

        db_modules = [
            wmp_db,
            maintenance_db,
            maintenance_rfc_db,
            poles_db,
            poles_rfc_db,
        ]

        frames: list[pd.DataFrame] = []

        for db_mod in db_modules:
            db_path = db_mod.default_db_path()
            if not db_path or not os.path.isfile(db_path):
                continue

            try:
                with sqlite3.connect(db_path) as conn:
                    cur = conn.cursor()
                    # Check table exists
                    cur.execute(
                        """
                        SELECT name FROM sqlite_master
                        WHERE type='table' AND name='permit_tracker'
                        """
                    )
                    if cur.fetchone() is None:
                        continue

                    placeholders = ",".join("?" for _ in action_filters)
                    query = f"""
                        SELECT
                            "Order",
                            "Notification Status",
                            "SAP Status",
                            "SP56 Status",
                            "RP56 Status",
                            "E Permit Status",
                            "Submit Days",
                            "Permit Expiration Date",
                            "Work Plan Date",
                            "CLICK Start Date",
                            "CLICK End Date",
                            "LEAPS Cycle Time",
                            "Action"
                        FROM permit_tracker
                        WHERE "Action" IN ({placeholders})
                    """
                    df = pd.read_sql_query(query, conn, params=action_filters)
                    if not df.empty:
                        # enforce column order and add any missing columns as blank
                        for col in PERMIT_COLUMNS:
                            if col not in df.columns:
                                df[col] = ""
                        df = df[PERMIT_COLUMNS].copy()
                        frames.append(df)
            except Exception as e:
                print(f"[Master_Emailer] Error reading {db_path}: {type(e).__name__}: {e}")

        if not frames:
            return pd.DataFrame(columns=PERMIT_COLUMNS)

        combined = pd.concat(frames, ignore_index=True)

        # Sort by Order for nicer display if numeric-ish
        try:
            combined["Order_sort"] = pd.to_numeric(combined["Order"], errors="coerce")
            combined = combined.sort_values(["Order_sort", "Order"]).drop(
                columns=["Order_sort"]
            )
        except Exception:
            pass

        return combined

    def _load_permit_expired_need_click_df(self) -> pd.DataFrame:
        """
        For 'Permit: Permit expired. Need CLICK Date for extension.':

        1) From each program DB, pull rows from permit_tracker where
           Action = 'Permit expired. Please provide CLICK Date for extension.'.

        2) For each Order, look up MAT from mpp_data (first row per Order).

        3) After combining across all DBs, look up Program Manager / LAN ID
           from data/static_lists.sqlite3 :: pm_list via MAT.

        4) Return a combined DataFrame with columns PERMIT_CLICK_COLUMNS,
           sorted/grouped by Program Manager, MAT, then Order.
        """
        ACTION_TEXT = "Permit expired. Please provide CLICK Date for extension."

        db_modules = [
            wmp_db,
            maintenance_db,
            maintenance_rfc_db,
            poles_db,
            poles_rfc_db,
        ]

        frames: list[pd.DataFrame] = []

        for db_mod in db_modules:
            db_path = db_mod.default_db_path()
            if not db_path or not os.path.isfile(db_path):
                continue

            try:
                with sqlite3.connect(db_path) as conn:
                    cur = conn.cursor()

                    # Check permit_tracker exists
                    cur.execute(
                        """
                        SELECT name FROM sqlite_master
                        WHERE type='table' AND name='permit_tracker'
                        """
                    )
                    if cur.fetchone() is None:
                        continue

                    # Check mpp_data exists
                    cur.execute(
                        """
                        SELECT name FROM sqlite_master
                        WHERE type='table' AND name='mpp_data'
                        """
                    )
                    has_mpp = (cur.fetchone() is not None)

                    if has_mpp:
                        # Use first mpp_data row per Order via rowid
                        query = f"""
                            WITH m_first AS (
                                SELECT "Order", "MAT"
                                FROM mpp_data
                                WHERE rowid IN (
                                    SELECT MIN(rowid)
                                    FROM mpp_data
                                    GROUP BY "Order"
                                )
                            )
                            SELECT
                                p."Order",
                                p."Notification Status",
                                m_first."MAT",
                                p."SAP Status",
                                p."SP56 Status",
                                p."RP56 Status",
                                p."E Permit Status",
                                p."Submit Days",
                                p."Permit Expiration Date",
                                p."Work Plan Date",
                                p."CLICK Start Date",
                                p."CLICK End Date",
                                p."LEAPS Cycle Time",
                                p."Action"
                            FROM permit_tracker p
                            LEFT JOIN m_first
                              ON m_first."Order" = p."Order"
                            WHERE p."Action" = ?
                        """
                        df = pd.read_sql_query(query, conn, params=[ACTION_TEXT])
                    else:
                        # No mpp_data -> we still pull permit rows, MAT will be blank
                        query = f"""
                            SELECT
                                p."Order",
                                p."Notification Status",
                                NULL AS "MAT",
                                p."SAP Status",
                                p."SP56 Status",
                                p."RP56 Status",
                                p."E Permit Status",
                                p."Submit Days",
                                p."Permit Expiration Date",
                                p."Work Plan Date",
                                p."CLICK Start Date",
                                p."CLICK End Date",
                                p."LEAPS Cycle Time",
                                p."Action"
                            FROM permit_tracker p
                            WHERE p."Action" = ?
                        """
                        df = pd.read_sql_query(query, conn, params=[ACTION_TEXT])

                    if not df.empty:
                        # Ensure MAT column exists
                        if "MAT" not in df.columns:
                            df["MAT"] = ""
                        frames.append(df)
            except Exception as e:
                print(f"[Master_Emailer] Error reading {db_path}: {type(e).__name__}: {e}")

        if not frames:
            return pd.DataFrame(columns=PERMIT_CLICK_COLUMNS)

        combined = pd.concat(frames, ignore_index=True)

        # ------------------------------------------------------------------
        # Enrich with Program Manager / LAN ID from pm_list in static_lists
        # ------------------------------------------------------------------
        pm_df: pd.DataFrame | None = None
        if os.path.isfile(STATIC_LISTS_DB_PATH):
            try:
                with sqlite3.connect(STATIC_LISTS_DB_PATH) as conn:
                    pm_df = pd.read_sql_query(
                        'SELECT "MAT", "Program Manager", "LAN ID" FROM pm_list',
                        conn,
                    )
            except Exception as e:
                print(f"[Master_Emailer] Error reading pm_list: {type(e).__name__}: {e}")
                pm_df = None

        if pm_df is not None and not pm_df.empty:
            # De-duplicate MAT rows; keep first
            pm_df = pm_df.drop_duplicates(subset=["MAT"], keep="first")
            combined = combined.merge(pm_df, on="MAT", how="left")
        else:
            combined["Program Manager"] = ""
            combined["LAN ID"] = ""

        # ------------------------------------------------------------------
        # Enforce final column order and fill missing columns as blank
        # ------------------------------------------------------------------
        for col in PERMIT_CLICK_COLUMNS:
            if col not in combined.columns:
                combined[col] = ""

        combined = combined[PERMIT_CLICK_COLUMNS].copy()

        # Group visually by Program Manager, then MAT, then Order
        try:
            combined["Order_sort"] = pd.to_numeric(combined["Order"], errors="coerce")
            combined = combined.sort_values(
                ["Program Manager", "MAT", "Order_sort", "Order"]
            ).drop(columns=["Order_sort"])
        except Exception:
            pass

        return combined

    # ------------------------------------------------------------------
    # Treeview population
    # ------------------------------------------------------------------
    def _populate_tree_from_df(self, df: pd.DataFrame) -> None:
        if self.tree is None:
            return

        columns = self._current_columns or []

        tree = self.tree

        # Clear existing rows
        for iid in tree.get_children(""):
            tree.delete(iid)

        # Ensure columns are set correctly
        tree["columns"] = columns
        for col in columns:
            tree.heading(col, text=col)
            base = 120
            if col == "Order":
                base = 100
            if col in ("Primary Intent Status", "Action"):
                base = 260
            if col in ("Program Manager", "LAN ID", "MAT"):
                base = 160
            tree.column(col, width=base, anchor="w", stretch=True)

        if df.empty:
            return

        # Insert rows
        for _, row in df.iterrows():
            values = [row.get(col, "") for col in columns]
            tree.insert("", "end", values=values)

    # ------------------------------------------------------------------
    # Send Email
    # ------------------------------------------------------------------
    def _update_send_state(self) -> None:
        if self.btn_send is None:
            return

        df_ok = self._df_current is not None and not self._df_current.empty
        to_ok = bool(self.to_var.get().strip())
        subj_ok = bool(self.subject_var.get().strip())

        self.btn_send.configure(
            state=("normal" if df_ok and to_ok and subj_ok else "disabled")
        )

    def _on_send_email(self) -> None:
        """
        Use Outlook automation (pywin32) to open an email draft with an HTML table.
        """
        df = self._df_current
        if df is None or df.empty:
            messagebox.showinfo(
                "No Orders",
                "There are no matching orders to include in the email.",
            )
            return

        to_addr = self.to_var.get().strip()
        subject = self.subject_var.get().strip()
        cc_addr = self.cc_var.get().strip()
        if not to_addr:
            messagebox.showerror(
                "Missing 'To'",
                "Please provide a recipient in the 'To:' field before sending.",
            )
            return
        if not subject:
            messagebox.showerror(
                "Missing Subject",
                "Please provide a subject before sending.",
            )
            return

        # Body text from the text widget
        if self.body_text is not None:
            body_text = self.body_text.get("1.0", "end").strip()
        else:
            body_text = ""

        columns = self._current_columns or []
        if not columns:
            # fallback if something weird happens
            columns = list(df.columns)

        # Generate HTML table
        html_table = df_to_excelish_html(df, columns)

        try:
            import win32com.client as win32
        except ImportError:
            messagebox.showerror(
                "Outlook Automation Error",
                "pywin32 is required for Outlook automation.\n\n"
                "Install it with:\n  pip install pywin32",
            )
            return

        try:
            app = win32.Dispatch("Outlook.Application")
            mail = app.CreateItem(0)  # olMailItem

            mail.Subject = subject
            mail.To = to_addr
            if cc_addr:
                mail.CC = cc_addr

            # Convert body_text newlines into <br> for HTML
            safe_body = (
                body_text.replace("\r\n", "\n")
                .replace("\r", "\n")
                .replace("\n", "<br>")
            )

            mail.HTMLBody = (
                f"<p>{safe_body}</p>"
                + html_table
                + "<p>Thank you.</p>"
            )

            # Show as draft for QC
            mail.Display(True)

        except Exception as e:
            messagebox.showerror(
                "Email Error",
                f"Failed to create Outlook email draft:\n\n{type(e).__name__}: {e}",
            )
