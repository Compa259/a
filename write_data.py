import os
from Google import Create_Service

CLIENT_SECRET_FILE = 'client_secret.json'
API_NAME = 'sheets'
API_VERSION = 'v4'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

service = Create_Service(CLIENT_SECRET_FILE, API_NAME, API_VERSION, SCOPES)


def write_to_sheet(spreadsheet_id, values):
    # spreadsheet_id = '1oqwamY99YWP9it7pjR3fAwcrUlE1Zuynbg5QSKBywOo'

    worksheet_name = 'Sheet1!'
    cell_range_insert = 'A1'
    # values = (
    #     ('Col A', 'Col B', 'Col C', 'Col D'),
    #     ('Apple', 'Orange', 'Watermelon', 'Banana')
    # )

    value_range_body = {
        'majorDimension': 'ROWS',
        'values': values
    }

    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        valueInputOption='USER_ENTERED',
        range=worksheet_name + cell_range_insert,
        body=value_range_body
    ).execute()