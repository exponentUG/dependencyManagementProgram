from core.base import ProgramSpec, ToolSpec
from .tracker_builder_rfc import Poles_Tracker_Builder_RFC
from .order_information_rfc import Poles_Order_Information_RFC

POLES_PROGRAM = ProgramSpec(
    id="poles",
    name="Poles Program",
    tools=[
        ToolSpec("tracker_builder", "Tracker Builder (RFC)", Poles_Tracker_Builder_RFC),
        ToolSpec("order information rfc", "Order Information (RFC)", Poles_Order_Information_RFC)
    ],
)
