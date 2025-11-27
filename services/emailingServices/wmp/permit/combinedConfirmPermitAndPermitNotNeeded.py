from helpers.emailHelpers.loadSheet import load_sheet
from helpers.emailHelpers.email import df_to_excelish_html
from ledgers.emailLedgers.wmp.column_ledger import permitColumns
from ledgers.emailLedgers.wmp.email_ledger import wmp_permit_combinedConfirmPermitAndPermitNotNeeded_el
import pandas as pd

COLUMNS = permitColumns
RECIPIENTS_MAP = wmp_permit_combinedConfirmPermitAndPermitNotNeeded_el

def wmp_permit_combinedConfirmPermitAndPermitNotNeeded(path: str):
    #Load Sheet
    df = load_sheet(path, "Permit")
    print(df)

    #Keep only required actions
    action_norm = df["Action"].astype(str).str.strip().str.casefold()
    df_confirm_permit_is_approved = df[action_norm == "confirm permit is approved and complete sap task"].copy()
    df_permit_not_needed = df[action_norm == "permit not needed. close sp/rp56"].copy()

    # tidy dates for display
    for col in ("Work Plan Date", "CLICK Start Date", "CLICK End Date", "Permit Application Date", "Encroachment Permit Expiration Date", "Permit Expiration Date"):
        if col in df_confirm_permit_is_approved.columns:
            df_confirm_permit_is_approved[col] = pd.to_datetime(df_confirm_permit_is_approved[col], errors="coerce").dt.strftime("%m/%d/%Y")
        if col in df_permit_not_needed.columns:
            df_permit_not_needed[col] = pd.to_datetime(df_permit_not_needed[col], errors="coerce").dt.strftime("%m/%d/%Y")

    #combine the two
    df_final = pd.concat([df_confirm_permit_is_approved, df_permit_not_needed], ignore_index=True)

    #open one outlook draft with the full table
    try:
        import win32com.client as win32
    except ImportError as e:
        raise ImportError("pywin32 is required for Outlook automation. Install: pip install pywin32") from e

    html_table = df_to_excelish_html(df_final, COLUMNS)
    app = win32.Dispatch("Outlook.Application")
    mail = app.CreateItem(0)  # olMailItem
    mail.Subject = f"Permit Task Pending Completion"
    mail.HTMLBody = "<p>Hi Brett,<br><br>The order(s) below have been flagged as permit is not needed or permit approved but the SP56/RP56 tasks are still open. Please review and complete the tasks. Also, let us know if anything else is pending for these order(s).<br><br>" + html_table + "Thank You"
    mail.To = RECIPIENTS_MAP["to"]
    mail.CC = RECIPIENTS_MAP["cc"]
    mail.Display(True)