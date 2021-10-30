import os
import pandas as pd
import numpy as np
import pickle as pkl
import httplib2
import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials
from config import CREDENTIALS_FILE, RESULTS_FOLDER

def get_statuses():
    """
        Read statuses from google sheet and clean it
    """
    spreadsheet_id = '1oQuX7eA5eijO98Ht_wO9O1noR20yJB73YiKfAnvQQ1g'
    # Читаем ключи из файла
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE,
        ['https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'])

    httpAuth = credentials.authorize(httplib2.Http()) # Авторизуемся в системе
    service = apiclient.discovery.build('sheets', 'v4', http = httpAuth) # Выбираем работу с таблицами и 4 версию API

    values = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range='Статусы сделок',
        majorDimension='ROWS'
    ).execute()

    statuses = values['values']
    statuses = pd.DataFrame(statuses[1:], columns=statuses[0])
    statuses.replace(to_replace=[None], value='-', inplace=True)  # Заменяем значения [None]
    # with open(os.path.join(RESULTS_FOLDER, 'statuses.pkl'), 'wb') as f:
    #    pkl.dump(statuses, f)
    return statuses

if __name__ == '__main__':
    get_statuses()
    with open(os.path.join(RESULTS_FOLDER, 'statuses.pkl'), 'rb') as f:
        statuses = pkl.load(f)
    print(statuses)
