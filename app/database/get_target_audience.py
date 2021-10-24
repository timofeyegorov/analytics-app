"""
Module with functions that reads target audience list from google sheets and clear it
https://docs.google.com/spreadsheets/d/1oQuX7eA5eijO98Ht_wO9O1noR20yJB73YiKfAnvQQ1g/edit#gid=0
"""
import requests
import os
import numpy as np
import pickle as pkl
import httplib2
import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials
from config import CREDENTIALS_FILE, DATA_FOLDER
from pprint import pprint
# dictionary for cleaning target audience where keys are recordings needs to be overwritten by recordings in values
change_dict = {'23 - 30': '24 - 30', '25 - 30': '26 - 30', '30 - 40': '31 - 40', '40 - 50': '41 - 50', '50 - 60': '51 - 60',
              '0 руб./ $0': '0 руб.', '0руб.': '0 руб.', 'более 100 000 руб. / более $1400': 'более 100 000 руб.',
              'до 100 000 руб. / до $1400': 'до 100 000 руб.', 'Ваш_средний_доход_в_месяц': 'до 100 000 руб.',
               'до 30 000 руб. / до $400': 'до 30 000 руб.', 'до 60 000 руб. / до $800': 'до 60 000 руб.',
              'Да, если это поможет мне в реализации проекта.': 'Да, проект',
              'Да, если я точно найду работу после обучения.': 'Да, работа',
              'Нет. Хочу получить только бесплатные материалы.': 'Нет',
              'Профессии связанные с числами (аналитик, бухгалтер, инженер и т.п.)': 'Связано с числами',
              'Гуманитарий (общение с людьми, искусство, медицина и т.д.)': 'Гуманитарий',
              'IT сфера (разработчик, тестировщик, администратор и т.п.)': 'IT сфера'}

def preprocess_target_audience(target_audience):
    """
        Clean target audience list from erroneous values
    :return: clean target audience
    """
    for idx, el in enumerate(target_audience):
        if el in change_dict.keys():
            target_audience[idx] = change_dict[el]
    indexes = np.unique(target_audience, return_index=True)[1]
    change_target = [target_audience[index] for index in sorted(indexes)]
    target_audience = change_target
    return target_audience

def get_target_audience():
    """
        Read target audience from google sheet and clean it
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
        range='ЦА',
        majorDimension='COLUMNS'
    ).execute()

    target_audience = preprocess_target_audience(values['values'][0])
    with open(os.path.join(DATA_FOLDER, 'target_audience.pkl'), 'wb') as f:
        pkl.dump(target_audience, f)
    return None

if __name__ == '__main__':
    get_target_audience()
    with open(os.path.join(DATA_FOLDER, 'target_audience.pkl'), 'rb') as f:
        ta = pkl.load(f)
    print(ta)