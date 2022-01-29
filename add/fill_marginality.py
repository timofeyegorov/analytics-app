"""
Module with functions that reads list from google sheets
"""
import os
import pickle as pkl
from config import RESULTS_FOLDER
from app.database import load_sheet, write_sheet
df = load_sheet(spreadsheetId='1zTeURPPE-xKKz-E81nPTwd4XMvAV-Uw9lN8qXyKybaA',
                sheetName='leads-1118292_2022-01-28', majorDimension='ROWS')

def get_turnover_on_lead(leads, ca_payment_analytic):
    leads.insert(10, 'turnover_on_lead', 0)
    for i in range(ca_payment_analytic.shape[0]):
        if ca_payment_analytic.values[i][1] in ['IT сфера', 'Гуманитарий', 'Предприниматель, руководитель',
                                                  'Связано с числами']:
            leads.loc[
                (leads['сколько_вам_лет'] == ca_payment_analytic.values[i][0]) &
                (leads['в_какой_сфере_сейчас_работаете'] == ca_payment_analytic.values[i][1]) &
                (leads['ваш_средний_доход_в_месяц'] == ca_payment_analytic.values[i][2]) &
                (leads['рассматриваете_ли_в_перспективе_платное_обучение_профессии_разработчик_искусственного_интеллекта'] == ca_payment_analytic.values[i][3]),
                'turnover_on_lead'
            ] = ca_payment_analytic.values[i][4]
        else:
            leads.loc[
                (leads['сколько_вам_лет'] == ca_payment_analytic.values[i][0]) &
                (~leads['в_какой_сфере_сейчас_работаете'].isin(
                    ['IT сфера', 'Гуманитарий', 'Предприниматель, руководитель', 'Связано с числами'])) &
                (leads['ваш_средний_доход_в_месяц'] == ca_payment_analytic.values[i][2]) &
                (leads['рассматриваете_ли_в_перспективе_платное_обучение_профессии_разработчик_искусственного_интеллекта'] == ca_payment_analytic.values[i][3]),
                'turnover_on_lead'
            ] = ca_payment_analytic.values[i][4]
        leads['Маржинальность'] = (leads['turnover_on_lead'] / (850 + 0.35 * leads['turnover_on_lead']) - 1) /\
                                  1 + (leads['turnover_on_lead'] / (850 + 0.35 * leads['turnover_on_lead']) - 1)
    return leads

if __name__ == '__main__':
    df = load_sheet(spreadsheetId='1zTeURPPE-xKKz-E81nPTwd4XMvAV-Uw9lN8qXyKybaA',
                       sheetName='leads-1118292_2022-01-28',
                       majorDimension='ROWS')
    with open(os.path.join(RESULTS_FOLDER, 'ca_payment_analytic.pkl'), 'rb') as f:
        ca_payment_analytic = pkl.load(f)
    df.loc[df[
               'в_какой_сфере_сейчас_работаете'] == 'Гуманитарий (общение с людьми, искусство, медицина и т.д.)', 'в_какой_сфере_сейчас_работаете'] = 'Гуманитарий'
    df.loc[df[
               'в_какой_сфере_сейчас_работаете'] == 'IT сфера (разработчик, тестировщик, администратор и т.п.)', 'в_какой_сфере_сейчас_работаете'] = 'IT сфера'
    df.loc[df[
               'в_какой_сфере_сейчас_работаете'] == 'Профессии связанные с числами (аналитик, бухгалтер, инженер и т.п.)', 'в_какой_сфере_сейчас_работаете'] = 'Связано с числами'

    df.loc[df[
               'рассматриваете_ли_в_перспективе_платное_обучение_профессии_разработчик_искусственного_интеллекта'] == 'Да, если это поможет мне в реализации проекта.', 'рассматриваете_ли_в_перспективе_платное_обучение_профессии_разработчик_искусственного_интеллекта'] = 'Да, проект'
    df.loc[df[
               'рассматриваете_ли_в_перспективе_платное_обучение_профессии_разработчик_искусственного_интеллекта'] == 'Да, если я точно найду работу после обучения.', 'рассматриваете_ли_в_перспективе_платное_обучение_профессии_разработчик_искусственного_интеллекта'] = 'Да, работа'
    df.loc[df[
               'рассматриваете_ли_в_перспективе_платное_обучение_профессии_разработчик_искусственного_интеллекта'] == 'Нет. Хочу получить только бесплатные материалы.', 'рассматриваете_ли_в_перспективе_платное_обучение_профессии_разработчик_искусственного_интеллекта'] = 'Нет'
    df = get_turnover_on_lead(df, ca_payment_analytic)
    df['Маржинальность'] = round(df['Маржинальность'], 2) * 100
    vals = [['Маржинальность'] + df['Маржинальность'].tolist()]
    # write_sheet(spreadsheetId='1zTeURPPE-xKKz-E81nPTwd4XMvAV-Uw9lN8qXyKybaA',
    #             startCell='O1', endCell='O1000',
    #             majorDimension='ROWS', values=df['Маржинальность'].tolist())
    print(df['Маржинальность'].tolist())
    import httplib2
    import apiclient.discovery
    from oauth2client.service_account import ServiceAccountCredentials
    from config import CREDENTIALS_FILE
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE,
        ['https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'])

    httpAuth = credentials.authorize(httplib2.Http()) # Авторизуемся в системе
    service = apiclient.discovery.build('sheets', 'v4', http = httpAuth) # Выбираем работу с таблицами и 4 версию API
    results = service.spreadsheets().values().batchUpdate(spreadsheetId='1zTeURPPE-xKKz-E81nPTwd4XMvAV-Uw9lN8qXyKybaA', body={
        "valueInputOption": "USER_ENTERED",
        # Данные воспринимаются, как вводимые пользователем (считается значение формул)
        "data": [
            {"range":'O1:O10000',
            "majorDimension": 'COLUMNS',  # Сначала заполнять строки, затем столбцы
            "values": vals
            }
        ]
    }).execute()
    print(df)

