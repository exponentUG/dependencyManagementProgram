from services.emailingServices.wmp.permit.combinedConfirmPermitAndPermitNotNeeded import wmp_permit_combinedConfirmPermitAndPermitNotNeeded
from services.emailingServices.wmp.permit.combinedRequestForExtensionAndSubmittedOver import wmp_permit_combinedRequestForExtensionAndSubmittedOver
from services.emailingServices.wmp.miscTSK.ds73 import wmp_miscTSK_ds73


def router(program: str, category: str, path, land_categories = []):
    if program == "wmp":
        if category == "Permit | Need Click Date for Extension":
            print("Permit | Need Click Date for Extension")
        elif category == "Permit | Confirm Permit is Approved/Permit Not Needed (Combined email to Brett)":
            wmp_permit_combinedConfirmPermitAndPermitNotNeeded(path)
        elif category == "Permit | Request for Extension/Submitted Over 45 Days (Combined email to Brett)":
            wmp_permit_combinedRequestForExtensionAndSubmittedOver(path)
        elif category == "DS73 | Task Closure Request":
            wmp_miscTSK_ds73()
    
    else:
        print("we do not have code for this program right now")
