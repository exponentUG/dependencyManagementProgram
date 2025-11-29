from core.base import ProgramSpec, ToolSpec
from .tracker_builder import Maintenance_Tracker_Builder

POLES_PROGRAM = ProgramSpec(
    id="poles",
    name="Poles Program",
    tools=[
        ToolSpec("tracker_builder", "Tracker Builder", Maintenance_Tracker_Builder),
    ],
)
