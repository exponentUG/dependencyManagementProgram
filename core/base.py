from dataclasses import dataclass, field
from typing import Type, List, Optional
from tkinter import ttk

APP_TITLE = "Exponent ToolKit"
FONT_H1 = ("Segoe UI", 16, "bold")
FONT_H2 = ("Segoe UI", 12, "bold")
FONT_BODY = ("Segoe UI", 10)

LEFT_RAIL_WIDTH = 260
ACTIVE_COLOR = "#2D6CDF"

class ToolView(ttk.Frame):
    def __init__(self, parent, program_name: str, tool_name: str):
        super().__init__(parent)
        ttk.Label(self, text=program_name, font=FONT_H2).grid(row=0, column=0, sticky="w", padx=16, pady=(16, 4))
        ttk.Label(self, text=tool_name, font=FONT_H1).grid(row=1, column=0, sticky="w", padx=16, pady=(0, 8))

class PlaceholderTool(ToolView):
    pass

@dataclass
class ToolSpec:
    id: str
    name: str
    view_cls: Type[ToolView] = PlaceholderTool

@dataclass
class ProgramSpec:
    id: str
    name: str
    tools: List[ToolSpec] = field(default_factory=list)
    children: List["ProgramSpec"] = field(default_factory=list)  # NEW

    def is_leaf(self) -> bool:
        """True if this program directly contains tools (no children)."""
        return len(self.children) == 0
