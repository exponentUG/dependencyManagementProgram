from core.base import ProgramSpec, ToolSpec
from .tracker_builder import Maintenance_Tracker_Builder
from .order_information import Maintenance_Order_Information
from .tracker_builder_rfc import Maintenance_Tracker_Builder_RFC
from .order_information_rfc import Maintenance_Order_Information_RFC

MAINTENANCE_PROGRAM = ProgramSpec(
    id="maintenance",
    name="Maintenance Program",
    tools=[
        ToolSpec("tracker_builder_rfc", "Tracker Builder (RFC)", Maintenance_Tracker_Builder_RFC),
        ToolSpec("order_information_rfc", "Order Information (RFC)", Maintenance_Order_Information_RFC),
        ToolSpec("tracker_builder", "Tracker Builder", Maintenance_Tracker_Builder),
        ToolSpec("order_information", "Order Information", Maintenance_Order_Information)
    ],
)
