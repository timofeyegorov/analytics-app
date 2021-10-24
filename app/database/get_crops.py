# Подключаем библиотеки
import httplib2
import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import pickle as pkl
from config import DATA_FOLDER, CREDENTIALS_FILE
import os
from urllib import parse


spreadsheet_id = '1dHxep6G9uQ9HSSwhKOzrXZiJX9d5a0wlY6SPSyLh1Qg'
# Читаем ключи из файла
credentials = ServiceAccountCredentials.from_json_keyfile_name(
    CREDENTIALS_FILE,
    ['https://www.googleapis.com/auth/spreadsheets',
     'https://www.googleapis.com/auth/drive'])

httpAuth = credentials.authorize(httplib2.Http()) # Авторизуемся в системе
service = apiclient.discovery.build('sheets', 'v4', http = httpAuth) # Выбираем работу с таблицами и 4 версию API


# Подгружаем таблицу трафикологов
trafficologist_data = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vSmqOg_7uGFrrEl3TW64vk0XScmPjmVFBjU2Jk7oL685odu9ZZbvBxZxwUmZ71Z-26oGrMl6Ls4EVpb/pub?gid=1388466384&single=true&output=csv'
traff_data = pd.read_csv(trafficologist_data)  # Получаем датафрейм из таблицы трафикологов

# Получаем значения из таблицы с расходами по посевам
values = service.spreadsheets().values().get(
    spreadsheetId=spreadsheet_id,
    range='A1:L100000',
    majorDimension='ROWS'
).execute()

# Создаем датасет из значений, полученных из таблицы
crops = pd.DataFrame.from_records(values['values'][1:], columns = values['values'][0])
crops.replace(to_replace=[None], value='None', inplace=True) # Заменяем значения [None]
crops = crops.iloc[:, [0,5,11]]
crops.rename(columns = {'': 'Дата'}, inplace = True)
crops = crops[(crops['Ссылка'] != '') & (crops['Ссылка'] != 'None')]
crops = crops[(crops['Дата'] != '') & (crops['Дата'] != 'None')]
crops = crops[(crops['Бюджет'] != '') & (crops['Бюджет'] != 'None')]
crops.reset_index(drop=True, inplace=True)
# crops['Дата'] = pd.to_datetime(crops['Дата'], format="%d.%m.%Y")
# crops['Метка'] = 'Неизвестно'
# for label in traff_data.label.unique().tolist():
#   for idx, link_ in enumerate(crops['Ссылка']):
#     o = urlparse(link_).query
#     if label == o:
#       crops.loc[idx, 'Метка'] = label
# crops.drop(columns='Ссылка', inplace = True)

# Получаем список уникальных имен посевов
crops_links = crops['Ссылка'].unique()
crops_list = []
for link in crops_links[:2]:
    o = parse.urlparse(link).query
    params = parse.parse_qs(o)
    utm_source = params['utm_source']
    crops_list.append(f"utm_source={utm_source[0]}")
    print(link, f"utm_source={utm_source[0]}")
# crops.to_csv('crops_expences.csv', index=False, encoding='cp1251')
# with open(os.path.join(DATA_FOLDER, 'crops.pkl'), 'wb') as f:
    # pkl.dump(crops_list, f)
