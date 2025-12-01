from core.base import ProgramSpec, ToolSpec, PlaceholderTool
from programs.dependencies.wmp.program import WMP_PROGRAM
from programs.dependencies.poles.program import POLES_PROGRAM
from programs.dependencies.maintenance.program import MAINTENANCE_PROGRAM
from programs.master_tracker_builder.program import MASTER_TRACKER_BUILDER

DEPENDENCIES_PROGRAM = ProgramSpec(
    id="dependencies",
    name="Dependencies",
    children = [
        WMP_PROGRAM,
        POLES_PROGRAM,
        MAINTENANCE_PROGRAM
    ]
)

PROGRAMS = {
    DEPENDENCIES_PROGRAM.id: DEPENDENCIES_PROGRAM,
    MASTER_TRACKER_BUILDER.id: MASTER_TRACKER_BUILDER
}
