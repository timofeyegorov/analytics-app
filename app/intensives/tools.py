import time
import pandas as pd
import gspread
from datetime import datetime

document_list = ['[Интенсив 3 дня] Участники', '[Интенсив 2 дня] Участники ', '[Интенсив chatGPT] Участники ',
                 '[Вебинары] Участники', '[Мини-уроки] Участники', '[Вебинары] Регистрации ',
                 '[Мини-уроки] Регистрации ', '[Акции] Предзаказы', '[Интенсив 2 дня] Предзаказы',
                 '[Интенсив 3 дня] Предзаказ ', '[Интенсив chatGPT] Предзаказ', '[Вебинары] Предзаказы',
                 '[Мини-уроки] Предзаказ ']
email_list = []


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
    # Готовим колонку price для расчетов
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


def get_funnel_payment(start, end) -> int:
    data_from = datetime.strptime(start, '%Y-%m-%d')
    data_to = datetime.strptime(end, '%Y-%m-%d')
    result_summ = 0
    # Авторизация по сервисному аккаунту
    gc = gspread.service_account(filename='app/intensives/intensives-394715-16cd73454d4f.json')
    # Делаем нормальный список почт без дубликатов
    email_set = set(email[0] for email in email_list)
    # Проходимся по каждому документу
    for document in document_list:
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
                        # распаковываем наши данные парами почта-цена
                        for email, value in email_list:
                            if row[0] == email:
                                result_summ += value
                                email_set.remove(email)
                                break
                        if not email_set:  # Если все email найдены, выходим из цикла
                            break
        if not email_set:  # Если все email найдены, выходим из внешнего цикла
            break
    return result_summ

