
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

from write_data import write_to_sheet
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)

client = gspread.authorize(creds)

def map_aggregate(input_df, aggregate_df):
    pd.set_option('display.max_columns', None)
    aggregate_df['checkin_date'] = aggregate_df['checkin_date'].astype(str)
    aggregate_df = aggregate_df.pivot(index='MT_hotel_id', columns='checkin_date', values='sum(--a.max_num_room)')
    z_df = map(input_df, aggregate_df)
    return z_df


def map_detail(input_df, detail_df, name):
    detail_df['checkin_date'] = detail_df['checkin_date'].astype(str)
    detail_df = detail_df[detail_df.domain == name]
    detail_df = detail_df.pivot(index='MT_hotel_id', columns='checkin_date', values='sum(--a.max_num_room)')
    ota_df = map(input_df, detail_df, name)
    return ota_df


def map(input_df, ota_df, name=None):
    input_df = input_df.iloc[:, 0:6]
    merged_df = pd.merge(input_df, ota_df, how='left', left_on='ID Khách Sạn', right_on='MT_hotel_id')
    merged_df['is_na'] = merged_df.iloc[:, 6:35].isnull().apply(lambda x: all(x), axis=1)
    is_na_true_df = merged_df[merged_df.is_na == True]
    is_na_true_df = is_na_true_df.fillna('Chưa mapping được')

    is_na_false_df = merged_df[merged_df.is_na == False]
    is_na_false_df = is_na_false_df.fillna('OTA không có phòng')

    z_df = pd.concat([is_na_false_df, is_na_true_df])
    if name is not None:
        z_df.insert(6, 'OTA', name)

    return z_df


def get_detail_df(sheet_number):
    sheet = client.open('OTAs Inventory Output - Raw').get_worksheet(sheet_number)
    aggregate_data = sheet.get_all_values()
    aggregate_headers = aggregate_data.pop(0)
    aggregate_df = pd.DataFrame(aggregate_data, columns=aggregate_headers)
    return aggregate_df


def export_aggregate_df(input_df, aggregate_df):
    z_df = map_aggregate(input_df, aggregate_df)
    z_df = z_df.drop(columns='is_na')
    z_df['ID Khách Sạn'] = z_df['ID Khách Sạn'].astype(int)
    z_df = z_df.sort_values('ID Khách Sạn')
    z_df.columns = z_df.columns.map(str)
    z_df = z_df.T.reset_index().values.T.tolist()
    write_to_sheet(
        'https://docs.google.com/spreadsheets/d/18EFhn8EmBQ-TZMxfkz_OBQRLDjitquZ---nHV521Ks8/edit#gid=1876320160'.split('/')[5],
        z_df, '[Final] - Output Aggregate')


def export_detail_df(input_df, aggregate_df):
    booking_df = map_detail(input_df, aggregate_df, 'Booking')
    agoda_df = map_detail(input_df, aggregate_df, 'Agoda')
    traveloka_df = map_detail(input_df, aggregate_df, 'Traveloka')
    z_df = pd.concat([booking_df, agoda_df, traveloka_df])
    z_df = z_df.drop(columns='is_na')
    z_df['ID Khách Sạn'] = z_df['ID Khách Sạn'].astype(int)
    z_df = z_df.sort_values('ID Khách Sạn')
    z_df.columns = z_df.columns.map(str)
    z_df = z_df.T.reset_index().values.T.tolist()
    write_to_sheet(
        'https://docs.google.com/spreadsheets/d/18EFhn8EmBQ-TZMxfkz_OBQRLDjitquZ---nHV521Ks8/edit#gid=2052161903'.split('/')[5],
        z_df, '[Final] - Output Aggregate')


if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    sheet = client.open('OTAs Inventory Output - Raw').get_worksheet(4)
    input_data = sheet.get_all_values()
    input_headers = input_data.pop(1)
    input_trash = input_data.pop(0)
    input_df = pd.DataFrame(input_data, columns=input_headers)

    # aggregate_df = get_detail_df(1)
    # export_detail_df(input_df, aggregate_df)

    aggregate_df = get_detail_df(3)
    export_aggregate_df(input_df, aggregate_df)

