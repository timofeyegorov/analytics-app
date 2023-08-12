import time
import pandas as pd
import gspread
import re
from datetime import datetime

document_list = ['[Интенсив 3 дня] Участники', '[Интенсив 2 дня] Участники ', '[Интенсив chatGPT] Участники ',
                 '[Вебинары] Участники', '[Мини-уроки] Участники', '[Вебинары] Регистрации ',
                 '[Мини-уроки] Регистрации ', '[Акции] Предзаказы', '[Интенсив 2 дня] Предзаказы',
                 '[Интенсив 3 дня] Предзаказ ', '[Интенсив chatGPT] Предзаказ', '[Вебинары] Предзаказы',
                 '[Мини-уроки] Предзаказ ']

intensive2_list = ['[Интенсив 2 дня] Регистрации', '[Интенсив 2 дня] Участники ', '[Интенсив 2 дня] Предзаказы']
intensive3_list = ['[Интенсив 3 дня] Регистрации', '[Интенсив 3 дня] Участники', '[Интенсив 3 дня] Предзаказ ']
intensiveGPT_list = ['[Интенсив chatGPT] Регистрации', '[Интенсив chatGPT] Участники ', '[Интенсив chatGPT] Предзаказ']
vebinar_list = ['[Вебинары] Регистрации ', '[Вебинары] Участники', '[Вебинары] Предзаказы']
minilesson_list = ['[Мини-уроки] Регистрации ', '[Мини-уроки] Участники', '[Мини-уроки] Предзаказ ']
bonus_list = ['[Акции] Предзаказы']

# Словарь мероприятий
event_names = {
    0: 'Интенсив 2 дня',
    1: 'Интенсив 3 дня',
    2: 'Интенсив chatGPT',
    3: 'Вебинары',
    4: 'Мини-урок',
    5: 'Акции'
}
# Словарь типов документа
file_type = {
    0: 'C регистраций',
    1: 'С участников',
    2: 'С предзаказов'
}

# Для сущностей оплат
email_list = []


def format_percent(x):
    return '{:.2%}'.format(x)


# Получение оплат
def get_payment(date_from, date_to) -> int:
    # Авторизация по сервисному аккаунту
    gc = gspread.service_account(filename='app/intensives/intensives-394715-16cd73454d4f.json')
    # Открытие таблицы
    table_main = gc.open('Аналитика по оплатам')
    # Получение первого листа (worksheet)
    worksheet = table_main.get_worksheet(0)
    data = worksheet.get_all_values()
    # Заворачиваем в dataframe
    df = pd.DataFrame(data)
    # оставляем все строки и только нужные столбцы
    df = df.iloc[:, [2, 8, 13, 17]]
    # Переименовываем столбцы и передаем словарь с новыми именами
    new_column_name = ['email', 'price', 'date', 'type']
    df = df[1:]  # Удаляем первую строку из данных
    df = df.rename(columns=dict(zip(df.columns, new_column_name)))
    # Преобразуем данные в datatime
    df['date'] = pd.to_datetime(df['date'], format='%d.%m.%Y')
    # Заменить все неразрывные пробелы '\xa0' на обычные пробелы
    df['price'] = df['price'].str.replace('\xa0', ' ')
    # Удалить все символы, кроме цифр и пробелов
    df['price'] = df['price'].str.replace(r'[^\d\s]', '', regex=True)
    df['price'] = df['price'].str.replace(' ', '')
    df['price'] = df['price'].astype(int)
    df['email'] = df['email'].str.lower()
    # Делаем выборку данных
    selected_data = df[(df['date'] >= pd.to_datetime(date_from)) & (df['date'] <= pd.to_datetime(date_to)) & (
            df['type'] != 'доплата')]
    result = selected_data['price'].sum()
    # Фрейм диапазона для обработки в функции просмотра таблиц мероприятий
    explore_frame = selected_data.loc[:, ['email', 'price']]
    # Сохраняем пары значений в список списков
    global email_list
    email_list = explore_frame.values.tolist()
    return result


# Список всех документов
event_list = [intensive2_list, intensive3_list, intensiveGPT_list, vebinar_list, minilesson_list, bonus_list]


# Формирование отчета
def get_funnel_payment(start, end):
    data_from = datetime.strptime(start, '%Y-%m-%d')
    data_to = datetime.strptime(end, '%Y-%m-%d')

    # Создаем пустой dataframe для сбора данных
    data = {'date': [], 'event': [], 'full_price': [], 'reg_price': [], 'peop_price': [], 'so_price': []}
    df = pd.DataFrame(data, columns=['date', 'event', 'full_price', 'reg_price', 'peop_price', 'so_price'])
    df['date'] = pd.to_datetime(df['date'])
    df['event'] = df['event'].astype(str)
    df['full_price'] = df['full_price'].astype(int)
    df['reg_price'] = df['reg_price'].astype(int)
    df['peop_price'] = df['peop_price'].astype(int)
    df['so_price'] = df['so_price'].astype(int)

    # Авторизация по сервисному аккаунту
    gc = gspread.service_account(filename='app/intensives/intensives-394715-16cd73454d4f.json')
    # Делаем нормальный список почт без дубликатов
    email_set = set(email[0] for email in email_list)

    # Проходим по мероприятиям и вложенным в них документам
    for index, document_list in enumerate(event_list):
        event_name = event_names.get(index, 'Неизвестное мероприятие')
        # Проходимся по каждому документу
        for index2, document in enumerate(document_list):
            # Получаем индекс вложенного документа
            if event_name == 'Акции':
                type_document = 'С предзаказов'
            else:
                type_document = file_type.get(index2, '')
            # Пауза для api
            time.sleep(5)
            # Открываем документ
            table_main = gc.open(document)
            # проходимся по каждому листу из листов документа
            for worksheet in table_main.worksheets():
                # получаем дату из названия листа и проверяем на вхождение в наши условия
                ws_title = worksheet.title
                date_sheet = datetime.strptime(ws_title.replace(' ', ''), '%d.%m.%Y')
                if data_from <= date_sheet <= data_to:
                    ws_data = worksheet.get_all_values()
                    for row in ws_data:
                        # Проверяем входит ли значение (первый элемент строки) в наш список почт
                        if row[0] in email_set:
                            result_reg = 0
                            result_pe = 0
                            result_so = 0
                            result_summ = 0
                            # распаковываем наши данные парами почта-цена
                            for email, value in email_list:
                                if row[0] == email:
                                    if type_document == 'C регистраций':
                                        result_reg += value
                                    if type_document == 'С участников':
                                        result_pe += value
                                    if type_document == 'С предзаказов':
                                        result_so += value
                                    result_summ += value
                                    # Наполняем dataframe
                            datarow = [date_sheet, event_name, result_summ, result_reg, result_pe, result_so]
                            df.loc[len(df)] = datarow
    # получаем сгруппированный dataframe
    grouped = df.groupby(['date', 'event']).agg(
        {'full_price': 'sum', 'reg_price': 'sum', 'peop_price': 'sum', 'so_price': 'sum'}).reset_index()
    # Добавляем процентные колонки
    grouped['%reg'] = grouped['reg_price'] / grouped['full_price']
    grouped['%peop'] = grouped['peop_price'] / grouped['full_price']
    grouped['%so'] = grouped['so_price'] / grouped['full_price']
    # Применяем форматирование вывода
    grouped['%reg'] = grouped['%reg'].apply(format_percent)
    grouped['%peop'] = grouped['%peop'].apply(format_percent)
    grouped['%so'] = grouped['%so'].apply(format_percent)
    grouped = grouped.rename(columns={'date': 'Дата', 'event': 'Мероприятие', 'full_price': 'Оборот общий',
                                      'reg_price': 'Оборот с регистраций', 'peop_price': 'Оборот с участников',
                                      'so_price': 'Оборот с SO',
                                      '%reg': '% с регистраций', '%peop': '% с участников', '%so': '% с предзаказов'})

    return grouped
