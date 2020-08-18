import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from pprint import pprint

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# SPREADSHEET_ID = '1oqwamY99YWP9it7pjR3fAwcrUlE1Zuynbg5QSKBywOo'
RANGE_NAME = 'Sheet1'


def writer(df, sheet_id):
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)  # here enter the name of your downloaded JSON file
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)
    sheet_name = RANGE_NAME
    data = [{'range': sheet_name, 'values': df}]
    batch_update_values_request_body = {
        'value_input_option': 'RAW',
        'data': data}

    request = service.spreadsheets().values().batchUpdate(spreadsheetId=sheet_id,
                                                          body=batch_update_values_request_body)
    response = request.execute()
    pprint(response)

# df = [[5, 6, 7, 8]]
# writer(df)
