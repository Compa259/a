import datetime

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine
from pandas import DataFrame
from write_data import write_to_sheet
from clickhouse_driver import Client
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)

client = gspread.authorize(creds)

if __name__ == '__main__':
    clickhouse_client = Client(host='172.31.25.244', port=9000,
                              user='streamsets',
                              password='bWqFHseP8KjIZw+RhzQL',
                              database='tripi_data')
    #connection_str = 'clickhouse://ducnm:H7R67ciSgvcyxCNodD9c@13.229.34.221:8128/tripi_data?charset=utf8'
    sheet = client.open('[clickhouse] - DAILY UPDATE FROM DB TO GGSHEET').get_worksheet(0)
    rows = sheet.get_all_records()

    for row in rows:
        try:
            now = datetime.datetime.now()
            if now.hour == int(row['Time']):
                try:
                    worksheet_name = row['Sheet']
                    sql = row['Query command']
                    header = row['Header'].split(',')
                    spreadsheet_id = row['Link'].split('/')[5]
                    sql = sql.replace(r"\n", "\t")
                    result = clickhouse_client.execute(f'{sql}')
                    df = DataFrame(result, columns=header)
                    df = df.applymap(str)
                    df = df.T.reset_index().values.T.tolist()
                    write_to_sheet(spreadsheet_id, df, worksheet_name)
                    sheet.update_cell(row['status_row_index'], row['status_col_index'], 'Done')
                except Exception as e:
                    sheet.update_cell(row['status_row_index'], row['status_col_index'], str(e))
                    pass
        except:
            pass




# import datetime
#
# import gspread
# from oauth2client.service_account import ServiceAccountCredentials
# from sqlalchemy import create_engine
# from pandas import DataFrame
# from write_data import write_to_sheet
# scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
# creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
#
# client = gspread.authorize(creds)
#
# if __name__ == '__main__':
#     connection_str = 'clickhouse://ducnm:H7R67ciSgvcyxCNodD9c@13.229.34.221:8128/tripi_data?charset=utf8'
#     engine = create_engine(connection_str)
#
#     sheet = client.open('Tripi-data_clickhouse_DAILY UPDATE FROM DB TO GGSHEET (OPS)').get_worksheet(0)
#     rows = sheet.get_all_records()
#
#     for row in rows:
#         try:
#             now = datetime.datetime.now()
#             if now.hour == int(row['Time']):
#                 try:
#                     worksheet_name = row['Sheet']
#                     sql = row['Query command']
#                     header = row['Header'].split(',')
#                     spreadsheet_id = row['Link'].split('/')[5]
#                     result = engine.execute(f'{sql}')
#                     df = DataFrame(result.fetchall(), columns=header)
#                     df = df.T.reset_index().values.T.tolist()
#                     write_to_sheet(spreadsheet_id, df, worksheet_name)
#                     sheet.update_cell(row['status_row_index'], row['status_col_index'], 'Done')
#                 except Exception as e:
#                     sheet.update_cell(row['status_row_index'], row['status_col_index'], str(e))
#                     pass
#         except:
#             pass
