import pandas as pd
import gspread
from df2gspread import gspread2df as d2g
from oauth2client.service_account import ServiceAccountCredentials


credentials = ServiceAccountCredentials.from_json_keyfile_name(
    'aaa.json')
gc = gspread.authorize(credentials)

spreadsheet_key = '1q9BVl2-qg9Kzo_je1EfxrzmTv7dMjRZ1YS5YSvmCXcc'
wks_name = 'Sheet1'
df = d2g.download(gfile=spreadsheet_key, col_names = True, row_names = True, credentials=credentials)
print(df)
