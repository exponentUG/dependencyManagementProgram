from tkinter import Tk, ttk
from core.theme import apply_theme
from landing import LandingView
from core.shell import ProgramShell
from registry import PROGRAMS
from core.base import APP_TITLE   # <-- NEW

class App:
    def __init__(self):
        self.root = Tk()
        self.root.title("Exponent ToolKit")

        # Make default size full screen (portable approach)
        try:
            # Windows (and many Linux builds)
            self.root.state("zoomed")
        except Tk.TclError:
            pass
        # Fallback: explicitly size to screen
        w = self.root.winfo_screenwidth()
        h = self.root.winfo_screenheight()
        self.root.geometry(f"{w}x{h}+0+0")

        self.root.minsize(950, 600)
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)

        apply_theme(self.root)

        self.container = ttk.Frame(self.root)
        self.container.grid(row=0, column=0, sticky="nsew")
        self.container.columnconfigure(0, weight=1)
        self.container.rowconfigure(0, weight=1)

        # Navigation state (from earlier changes)
        self.active_view = None
        self.nav_stack = []
        self.current_program = None

        self.show_landing()

    def _swap(self, view_cls, **kwargs):
        if self.active_view is not None:
            self.active_view.destroy()
        self.active_view = view_cls(self.container, app=self, **kwargs)
        self.active_view.grid(row=0, column=0, sticky="nsew")

    def show_landing(self):
        self.nav_stack.clear()
        self.current_program = None
        self._swap(LandingView, programs=PROGRAMS)

    def show_program(self, program_spec, push=True):
        if push and self.current_program is not None:
            self.nav_stack.append(self.current_program)
        self.current_program = program_spec
        self._swap(ProgramShell, program=program_spec)

    def go_back(self):
        if self.nav_stack:
            prev = self.nav_stack.pop()
            self.current_program = prev
            self._swap(ProgramShell, program=prev)
        else:
            self.show_landing()

    # ---------- NEW: breadcrumb text ----------
    def get_breadcrumb(self) -> str:
        """Return text like:
           'Exponent ToolKit' (landing)
           'Exponent ToolKit | Tier1'
           'Exponent ToolKit | Tier1 | Tier2'
           'Exponent ToolKit | Tier1 | Tier2 | Tier3'
        """
        parts = [APP_TITLE]
        if self.current_program:
            chain = [p.name for p in self.nav_stack] + [self.current_program.name]
            parts.append(" | ".join(chain))
        return " | ".join(parts)

    def run(self):
        self.root.mainloop()

if __name__ == "__main__":
    App().run()