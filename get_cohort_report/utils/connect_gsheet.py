import os
import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe, get_as_dataframe

load_dotenv()


class GSheetClient:
    def __init__(self):
        self.service_account_file = os.getenv("SERVICE_ACCOUNT_FILE")
        self.scopes = ["https://www.googleapis.com/auth/spreadsheets"]

        if not self.service_account_file:
            raise ValueError("SERVICE_ACCOUNT_FILE not found in environment variables")

        creds = Credentials.from_service_account_file(
            self.service_account_file,
            scopes=self.scopes
        )
        self.client = gspread.authorize(creds)

    def read_df(self, spreadsheet_id, worksheet_index=0):
        sheet = self.client.open_by_key(spreadsheet_id).get_worksheet(worksheet_index)
        df = get_as_dataframe(sheet, evaluate_formulas=True, header=0)
        return df.dropna(how="all")

    def write_df(self, spreadsheet_id, df, worksheet_index=0):
        sheet = self.client.open_by_key(spreadsheet_id).get_worksheet(worksheet_index)
        sheet.clear()
        set_with_dataframe(sheet, df, include_column_header=True)

    def append_df(
        self,
        spreadsheet_id,
        df,
        worksheet_index=0,
        include_header=False
    ):
        sheet = self.client.open_by_key(spreadsheet_id).get_worksheet(worksheet_index)
        last_row = len(sheet.get_all_values())

        if last_row == 0:
            set_with_dataframe(sheet, df, row=1, include_column_header=True)
        else:
            set_with_dataframe(
                sheet,
                df,
                row=last_row + 1,
                include_column_header=include_header
            )
