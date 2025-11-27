from core.base import ProgramSpec, ToolSpec, PlaceholderTool
from programs.dependencies.wmp.program import WMP_PROGRAM

DEPENDENCIES_PROGRAM = ProgramSpec(
    id="dependencies",
    name="Dependencies",
    children = [
        WMP_PROGRAM
    ]
)


PROGRAMS = {
    DEPENDENCIES_PROGRAM.id: DEPENDENCIES_PROGRAM,
}
