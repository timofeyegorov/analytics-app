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

def get_ca_payment_analytic():
    # https://docs.google.com/spreadsheets/d/1wGckyRyZH--5NPg1f7KWVKYA_NfZvkRY0mhmFVflpZI/edit#gid=0
    spreadsheet_id = '1wGckyRyZH--5NPg1f7KWVKYA_NfZvkRY0mhmFVflpZI'
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
        range='Рейтинг',
        majorDimension='ROWS'
    ).execute()
    values = values['values']
    payments_analytic = pd.DataFrame(values[1:], columns=values[0] + 7 * [''])

    payments_analytic.loc[payments_analytic['Доход']=='До 100', 'Доход'] = 'до 100 000 руб.'
    payments_analytic.loc[payments_analytic['Доход']=='Более 100', 'Доход'] = 'более 100 000 руб.'
    payments_analytic.loc[payments_analytic['Доход']=='0', 'Доход'] = '0 руб.'
    payments_analytic.loc[payments_analytic['Доход']=='До 30', 'Доход'] = 'до 30 000 руб.'
    payments_analytic.loc[payments_analytic['Доход']=='До 60', 'Доход'] = 'до 60 000 руб.'

    payments_analytic.loc[payments_analytic['Профессия']=='Предприниматели', 'Профессия'] = 'Предприниматель, руководитель'
    payments_analytic.loc[payments_analytic['Профессия']=='IT', 'Профессия'] = 'IT сфера'
    payments_analytic.loc[payments_analytic['Профессия']=='Числа', 'Профессия'] = 'Связано с числами'

    payments_analytic.loc[payments_analytic['Возраст']=='До 18', 'Возраст'] = 'до 18 лет'
    payments_analytic.loc[payments_analytic['Возраст']=='51-60', 'Возраст'] = '51 - 60'
    payments_analytic.loc[payments_analytic['Возраст']=='60+', 'Возраст'] = '60 +'
    payments_analytic.loc[payments_analytic['Возраст']=='41-50', 'Возраст'] = '41 - 50'
    payments_analytic.loc[payments_analytic['Возраст']=='31-40', 'Возраст'] = '31 - 40'
    payments_analytic.loc[payments_analytic['Возраст']=='25-30', 'Возраст'] = '26 - 30'
    payments_analytic.loc[payments_analytic['Возраст']=='18-24', 'Возраст'] = '18 - 25'

    payments_analytic.sort_values(by=['Профессия', 'Да/нет', 'Возраст', 'Доход'], inplace=True)
    payments_analytic.reset_index(drop=True, inplace=True)

    temp_profession = payments_analytic['Профессия'].unique().tolist()
    temp_education = payments_analytic['Да/нет'].unique().tolist()
    temp_age = payments_analytic['Возраст'].unique().tolist()[1:]
    temp_earning = payments_analytic['Доход'].unique().tolist()[:-1]

    values = []

    for profession in temp_profession:
        for education in temp_education:
            for age in temp_age:

                # print(profession, education, age, earning)
                # print(payments_analytic[
                #         (payments_analytic['Профессия'] == profession) &
                #         (payments_analytic['Да/нет'] == education) &
                #         (payments_analytic['Возраст'] == age)
                #     ].values)
                try:
                    turn_on_lead_1 = \
                        payments_analytic[
                            (payments_analytic['Профессия'] == profession) &
                            (payments_analytic['Да/нет'] == education) &
                            (payments_analytic['Возраст'] == age)
                            ].values[0][5]
                except IndexError as e:
                    continue
                for earning in temp_earning:
                    # print(payments_analytic[
                    #         (payments_analytic['Профессия'] == profession) &
                    #         (payments_analytic['Да/нет'] == education) &
                    #         (payments_analytic['Доход'] == earning)
                    #     ].values)
                    # print('------------------------------')
                    # print()
                    turn_on_lead_2 = \
                        payments_analytic[
                            (payments_analytic['Профессия'] == profession) &
                            (payments_analytic['Да/нет'] == education) &
                            (payments_analytic['Доход'] == earning)
                            ].values[0][5]

                    trun_in_lead = (int(turn_on_lead_1) + int(turn_on_lead_2)) / 2

                    values.append([
                        age,
                        profession,
                        earning,
                        education,
                        trun_in_lead
                    ])
    return pd.DataFrame(columns=['Возраст', 'Профессия', 'Доход', 'Да/нет', 'Оборот на лида'],
                                         data=values)
if __name__ == '__main__':
    out_df = get_ca_payment_analytic()
    with open(os.path.join(RESULTS_FOLDER, 'ca_payment_analytic.pkl'), 'wb') as f:
        pkl.dump(out_df, f)