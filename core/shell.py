import tkinter as tk
from tkinter import ttk
from .base import APP_TITLE, FONT_H2, ProgramSpec, ToolSpec, ACTIVE_COLOR, LEFT_RAIL_WIDTH

class ProgramShell(ttk.Frame):
    """Top bar + (either subprogram picker OR left tool rail) + right content area."""
    def __init__(self, parent, app, program: ProgramSpec):
        super().__init__(parent)
        self.app = app
        self.program = program
        self._tool_widgets: dict[str, dict] = {}

        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)

        # Top bar
        top = ttk.Frame(self)
        top.grid(row=0, column=0, sticky="ew")
        top.columnconfigure(0, weight=0)
        top.columnconfigure(1, weight=1)

        # Back goes up one tier
        ttk.Button(top, text="←", width=3, command=self.app.go_back)\
           .grid(row=0, column=0, padx=(8, 0), pady=6, sticky="w")

        # ---------- CHANGED: use app.get_breadcrumb() ----------
        ttk.Label(top, text=self.app.get_breadcrumb(), font=FONT_H2)\
           .grid(row=0, column=1, padx=8, pady=6, sticky="w")

        ttk.Separator(self, orient="horizontal").grid(row=0, column=0, sticky="ew", pady=(40, 0))

        # If this program has children, show sub-program picker and return
        if not program.is_leaf():
            self._render_subprogram_picker()
            return

        # Otherwise render the original tool-rail layout
        body = ttk.Frame(self)
        body.grid(row=1, column=0, sticky="nsew")
        body.columnconfigure(0, weight=0)
        body.columnconfigure(1, weight=1)
        body.rowconfigure(0, weight=1)

        # LEFT rail (tools)
        rail = tk.Frame(body, bd=0, highlightthickness=0)
        rail.grid(row=0, column=0, sticky="nsw")
        rail.configure(width=LEFT_RAIL_WIDTH)
        rail.grid_propagate(False)

        ttk.Label(rail, text="Tools", font=FONT_H2).pack(anchor="w", padx=10, pady=(12, 6))
        style = ttk.Style(self)
        style.configure("Rail.TButton", anchor="w", padding=(14, 10))

        for tool in program.tools:
            row = tk.Frame(rail)
            row.pack(fill="x", padx=6, pady=6)

            indicator = tk.Frame(row, width=4, height=36, bg=rail.cget("bg"))
            indicator.pack(side="left", fill="y")

            btn = ttk.Button(row, text=tool.name, style="Rail.TButton",
                             command=lambda ts=tool: self._show_tool(ts))
            btn.pack(side="left", fill="x", expand=True, ipadx=6, ipady=6)

            self._tool_widgets[tool.id] = {"indicator": indicator, "button": btn, "spec": tool}

        # RIGHT content host
        self.content = ttk.Frame(body)
        self.content.grid(row=0, column=1, sticky="nsew")
        self.content.columnconfigure(0, weight=1)
        self.content.rowconfigure(0, weight=1)

        self.current_tool_id: str | None = None
        if program.tools:
            self._show_tool(program.tools[0])

    # --- Sub-program picker (list of buttons) ---
    def _render_subprogram_picker(self):
        grid = ttk.Frame(self, padding=16)
        grid.grid(row=1, column=0, sticky="nw")
        ttk.Label(grid, text="Select a sub-program:", font=FONT_H2).grid(row=0, column=0, sticky="w", pady=(0, 10))

        # NOTE: When entering a child, push current program onto the nav stack.
        for r, child in enumerate(self.program.children, start=1):
            ttk.Button(
                grid, text=child.name, width=25, command=lambda p=child: self.app.show_program(p, push=True)
            ).grid(row=r, column=0, sticky="w", ipadx=24, ipady=10, pady=8)

    def _activate_button(self, tool_id: str):
        for tid, widgets in self._tool_widgets.items():
            ind: tk.Frame = widgets["indicator"]
            ind.configure(bg=(ACTIVE_COLOR if tid == tool_id else ind.master.cget("bg")))

    def _show_tool(self, tool_spec: ToolSpec):
        for child in self.content.winfo_children():
            child.destroy()
        view = tool_spec.view_cls(self.content, program_name=self.program.name, tool_name=tool_spec.name)
        view.grid(row=0, column=0, sticky="nsew")
        self.current_tool_id = tool_spec.id
        self._activate_button(tool_spec.id)
