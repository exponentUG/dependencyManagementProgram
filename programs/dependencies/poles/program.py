from core.base import ProgramSpec, ToolSpec
from .tracker_builder_rfc import Poles_Tracker_Builder_RFC
from .order_information_rfc import Poles_Order_Information_RFC
from .tracker_builder import Poles_Tracker_Builder
from .order_information import Poles_Order_Information

POLES_PROGRAM = ProgramSpec(
    id="poles",
    name="Poles Program",
    tools=[
        ToolSpec("tracker_builder_rfc", "Tracker Builder (RFC)", Poles_Tracker_Builder_RFC),
        ToolSpec("order_information_rfc", "Order Information (RFC)", Poles_Order_Information_RFC),
        ToolSpec("tracker_builder", "Tracker Builder", Poles_Tracker_Builder),
        ToolSpec("order_information", "Order Information", Poles_Order_Information)
    ],
)
