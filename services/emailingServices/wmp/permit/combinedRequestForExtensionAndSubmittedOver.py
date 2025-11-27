from helpers.emailHelpers.loadSheet import load_sheet
from helpers.emailHelpers.email import df_to_excelish_html
from ledgers.emailLedgers.wmp.column_ledger import permitColumns_combinedRequestForExtensionAndSubmittedOver
from ledgers.emailLedgers.wmp.email_ledger import wmp_permit_combinedRequestForExtensionAndSubmittedOver_el
import pandas as pd

COLUMNS = permitColumns_combinedRequestForExtensionAndSubmittedOver
RECIPIENTS_MAP = wmp_permit_combinedRequestForExtensionAndSubmittedOver_el

def wmp_permit_combinedRequestForExtensionAndSubmittedOver(path: str):
    #Load Sheet
    df = load_sheet(path, "Permit")

    #Keep only required actions
    action_norm = df["Action"].astype(str).str.strip().str.casefold()
    df_request_for_extension = df[action_norm == "request for extension"].copy()
    df_submitted_over_forty_five_days = df[action_norm == "submitted over 45 days. provide update"].copy()

    # tidy dates for display
    for col in ("Work Plan Date", "CLICK Start Date", "CLICK End Date", "Permit Application Date", "Encroachment Permit Expiration Date", "LEAPS Expected EOD", "Permit Expiration Date"):
        if col in df_request_for_extension.columns:
            df_request_for_extension[col] = pd.to_datetime(df_request_for_extension[col], errors="coerce").dt.strftime("%m/%d/%Y")
        if col in df_submitted_over_forty_five_days.columns:
            df_submitted_over_forty_five_days[col] = pd.to_datetime(df_submitted_over_forty_five_days[col], errors="coerce").dt.strftime("%m/%d/%Y")

    #combine the two
    df_final = pd.concat([df_request_for_extension, df_submitted_over_forty_five_days], ignore_index=True)

    #open one outlook draft with the full table
    try:
        import win32com.client as win32
    except ImportError as e:
        raise ImportError("pywin32 is required for Outlook automation. Install: pip install pywin32") from e

    html_table = df_to_excelish_html(df_final, COLUMNS)
    app = win32.Dispatch("Outlook.Application")
    mail = app.CreateItem(0)  # olMailItem
    mail.Subject = f"Permit Status Update"
    mail.HTMLBody = "<p>Hi Brett,<br><br>The order(s) below have been submitted for over 45 days and still not issued. Please review and provide updates on them.<br><br>" + html_table + "Thank You"
    mail.To = RECIPIENTS_MAP["to"]
    mail.CC = RECIPIENTS_MAP["cc"]
    mail.Display(True)