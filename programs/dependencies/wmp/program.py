from core.base import ProgramSpec, ToolSpec
from .emailer import WmpEmailer
from .custom_emailer import WmpCustomEmailer
from .tracker_builder import WMP_Tracker_Builder
from .order_information import WMP_Order_Information

WMP_PROGRAM = ProgramSpec(
    id="wmp",
    name="WMP Program",
    tools=[
        ToolSpec("tracker_builder", "Tracker Builder", WMP_Tracker_Builder),
        ToolSpec("emailer", "Emailer", WmpEmailer),
        ToolSpec("custom_emailer", "Custom Emailer", WmpCustomEmailer),
        ToolSpec("order_information", "Order Information", WMP_Order_Information)
    ],
)
