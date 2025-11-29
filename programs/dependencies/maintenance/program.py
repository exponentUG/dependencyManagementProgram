from core.base import ProgramSpec, ToolSpec
from .tracker_builder import Maintenance_Tracker_Builder

MAINTENANCE_PROGRAM = ProgramSpec(
    id="maintenance",
    name="Maintenance Program",
    tools=[
        ToolSpec("tracker_builder", "Tracker Builder", Maintenance_Tracker_Builder),
    ],
)
