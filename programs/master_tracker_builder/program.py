from core.base import ProgramSpec, ToolSpec
from .tracker_builder import MASTER_TRACKER_BUILDER
from .order_information import Master_Order_Information
from .emailer import Master_Emailer

MASTER_TRACKER_BUILDER = ProgramSpec(
    id="master_tracker_builder",
    name="Master Tracker Builder",
    tools=[
        ToolSpec("master_tracker_builder", "Master Tracker Builder", MASTER_TRACKER_BUILDER),
        ToolSpec("master_order_information", "Master Order Information", Master_Order_Information),
        ToolSpec("master_emailer", "Master Emailer", Master_Emailer),
    ],
)
