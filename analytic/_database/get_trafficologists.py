"""
Module with functions that reads trafficologists list from google sheets
"""
import os
import pandas as pd
import pickle as pkl
import httplib2
import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials
from config import CREDENTIALS_FILE, DATA_FOLDER

def get_trafficologists():
    """
        Read trafficologists from google sheet
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
        range='Трафикологи',
        majorDimension='ROWS'
    ).execute()
    trafficologists = values['values']
    trafficologists = pd.DataFrame(trafficologists[1:], columns=trafficologists[0])
    return trafficologists

if __name__ == '__main__':
    get_trafficologists()
    with open(os.path.join(DATA_FOLDER, 'trafficologists.pkl'), 'rb') as f:
        trafficologists = pkl.load(f)
    print(trafficologists)
