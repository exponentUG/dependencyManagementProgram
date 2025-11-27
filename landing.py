# landing.py
import tkinter as tk
from tkinter import ttk
from core.base import APP_TITLE, FONT_H1, FONT_H2, FONT_BODY

# <-- update this to your path (no quotes around backslashes if you use /)
LOGO_PATH = "ledgers/exponentLogo/exponentLogo.png"

class LandingView(ttk.Frame):
    def __init__(self, parent, app, programs):
        super().__init__(parent)
        self.app = app
        self.programs = programs

        self.columnconfigure(0, weight=1)
        self.rowconfigure(2, weight=1)

        # Title style: teal
        style = ttk.Style(self)
        style.configure("LandingTitle.TLabel", foreground="#008080", font=FONT_H1)

        header = ttk.Label(self, text=APP_TITLE, style="LandingTitle.TLabel")
        header.grid(row=0, column=0, sticky="w", padx=12, pady=8)

        wrap = ttk.Frame(self, padding=16)
        wrap.grid(row=1, column=0, sticky="w")
        ttk.Label(wrap, text="Welcome!", font=FONT_H2).grid(row=0, column=0, sticky="w", pady=(0, 8))
        ttk.Label(
            wrap,
            text="Please choose the program you would like to work with today:",
            font=FONT_BODY
        ).grid(row=1, column=0, sticky="w", pady=(0, 16))

        btns = ttk.Frame(self, padding=(16, 8))
        btns.grid(row=2, column=0, sticky="nw")
        for r, prog in enumerate(self.programs.values()):
            ttk.Button(btns, text=prog.name, command=lambda p=prog: self.app.show_program(p))\
               .grid(row=r, column=0, sticky="w", ipadx=24, ipady=10, pady=12)
