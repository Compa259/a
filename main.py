import time
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow, Flow
from google.auth.transport.requests import Request
import os
import pickle
from sqlalchemy import create_engine, text
from pandas import DataFrame
from add_data import writer
from clickhouse_driver import Client
import datetime
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# here enter the id of your google sheet
SPREADSHEET_ID_INPUT = '1o5MbIyM6C5g1aNYF1i7tcIuK1VwiiFAd6YrGhS7wekI'
RANGE_NAME_INPUT = 'test!A2:G4'


def read_data():
    global values_input, service
    execute_commands = []
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

    # Call the Sheets API
    sheet = service.spreadsheets()
    result_input = sheet.values().get(spreadsheetId=SPREADSHEET_ID_INPUT,
                                      range=RANGE_NAME_INPUT).execute()
    values_input = result_input.get('values', [])

    if not values_input:
        print('No data found.')

    execute_hours = []
    for sheet_row in values_input:
        sheet_info = {'sheet_id': sheet_row[0], 'header': sheet_row[2], 'sql': sheet_row[3], 'time': sheet_row[4]}
        execute_commands.append(sheet_info)
        execute_hours.append(int(sheet_row[4]))

    return execute_commands, execute_hours


if __name__ == '__main__':
    # client = Client(host='13.251.26.93', port=9000,
    #                          user='crawler_db',
    #                          password='m6MVBJWBcjBZhPsjMFJl',
    #                          database='crawler_db')
    connection_str = 'clickhouse://ducnm:H7R67ciSgvcyxCNodD9c@13.229.34.221:8128/tripi_data?charset=utf8'
    engine = create_engine(connection_str)

    execute_commands, execute_hours = read_data()
    print(execute_commands)
    # for sheet_info in execute_commands:
    #     sql = sheet_info['sql']
    #     result = engine.execute(f'{sql}')
    #     # result = client.execute(f'{sql} FORMAT TabSeparatedWithNamesAndTypes')
    #
    #     sheet_id = sheet_info['sheet_id'].split('/')[5]
    #     header = sheet_info['header'].split(',')
    #     df = DataFrame(result.fetchall(), columns=header)
    #     df = df.T.reset_index().values.T.tolist()
        # writer(df, sheet_id)


    # for sheet_info in execute_commands:
    #     sql = sheet_info['sql']
    #     result = engine.execute(f'{sql}')
    #     # result = client.execute(f'{sql} FORMAT TabSeparatedWithNamesAndTypes')
    #
    #     sheet_id = sheet_info['sheet_id'].split('/')[5]
    #     header = sheet_info['header'].split(',')
    #     df = DataFrame(result.fetchall(), columns=header)
    #     df = df.T.reset_index().values.T.tolist()
    #     writer(df, sheet_id)
