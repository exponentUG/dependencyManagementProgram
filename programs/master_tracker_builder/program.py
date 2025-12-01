from core.base import ProgramSpec, ToolSpec
from .tracker_builder import MASTER_TRACKER_BUILDER

MASTER_TRACKER_BUILDER = ProgramSpec(
    id="master_tracker_builder",
    name="Master Tracker Builder",
    tools=[
        ToolSpec("master_tracker_builder", "Master Tracker Builder", MASTER_TRACKER_BUILDER),
    ],
)
