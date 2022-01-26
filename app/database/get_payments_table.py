# Подключаем библиотеки
import httplib2
import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import pickle as pkl
from config import DATA_FOLDER, CREDENTIALS_FILE, RESULTS_FOLDER
import os
from urllib import parse
from urllib.parse import urlparse, urlsplit

def get_payments_table():
    spreadsheet_id = '1j_JK0jLGctDhIx3PJHjznTgYEKeAZQikxX1eSOnO5dg'
    # Читаем ключи из файла
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE,
        ['https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/drive'])

    httpAuth = credentials.authorize(httplib2.Http())  # Авторизуемся в системе
    service = apiclient.discovery.build('sheets', 'v4', http=httpAuth)  # Выбираем работу с таблицами и 4 версию API

    # Получаем значения из таблицы с расходами по посевам
    values = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range='Лист2',
        majorDimension='ROWS'
    ).execute()
    values = values['values']
    payments_table = pd.DataFrame(values[1:], columns=values[0] + [''])
    payments_table = payments_table[payments_table['Дата заявки'] != '']
    payments_table = payments_table[payments_table['Сумма оплаты'] != '']
    payments_table['Дата заявки'] = pd.to_datetime(payments_table['Дата заявки'], format='%d.%m.%Y')
    payments_table['Дата оплаты'] = pd.to_datetime(payments_table['Дата оплаты'], format='%d.%m.%Y')
    payments_table.sort_values(by=['Дата заявки'], inplace=True)
    payments_table = payments_table[payments_table['Дата заявки'] >= '2021-01-04']
    payments_table.reset_index(drop=True, inplace=True)
    for i in range(payments_table.shape[0]):
        payments_table['Сумма оплаты'][i] = payments_table['Сумма оплаты'][i].replace(' ', '')
    payments_table['Сумма оплаты'] = payments_table['Сумма оплаты'].astype(int)
    return payments_table

if __name__ == '__main__':
    df = get_payments_table()
    print(df.columns)
    print(df)