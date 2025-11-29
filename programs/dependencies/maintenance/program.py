from core.base import ProgramSpec, ToolSpec
from .tracker_builder import Maintenance_Tracker_Builder
from .order_information import Maintenance_Order_Information_RFC

MAINTENANCE_PROGRAM = ProgramSpec(
    id="maintenance",
    name="Maintenance Program",
    tools=[
        ToolSpec("tracker_builder", "Tracker Builder", Maintenance_Tracker_Builder),
        ToolSpec("order information", "Order Information", Maintenance_Order_Information_RFC)
    ],
)
