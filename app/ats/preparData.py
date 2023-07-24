import pandas as pd

file_path = 'api/data.csv'
filter_open: list = []
filter_num: list = []


# Функция подготовки базовой таблицы для всех срезов
def prep_data():
    global filter_num
    global filter_open
    data = pd.read_csv(file_path, delimiter=';', low_memory=False)
    # удалим лишние данные
    new_data = data.drop(
        columns=['Схема', 'Кто разговаривал', 'Кто ответил', 'Оценка', 'ID записи', 'Метка', 'Теги', 'ID заказа звонка',
                 'Запись существует', 'Новый клиент', 'Состояние перезвона', 'Время перезвона', 'Информация из CRM',
                 'Ответственный из CRM'])
    # добавим нужные столбы
    new_data['Дозвон'] = [1 if duration > 10 and status == 'Отвечен' else 0 for duration, status in
                          zip(new_data['Длительность звонка'], new_data['Статус'])]
    new_data['Звонок'] = 1
    # Это нужно чтобы работал query, с пробелами в названии не работает
    new_data = new_data.rename(columns={'Исходящая линия': 'ИсходящаяЛиния'})
    if filter_open != []:
        new_data = new_data.query(f'Откуда == {filter_open}')
    if filter_num != []:
        new_data = new_data.query(f'ИсходящаяЛиния == {filter_num}')
    return new_data


# TODO подход с global ущербный, пока так, но нужно переделать
def filter_numbers(numbers: list):
    global filter_num
    filter_num = numbers


def filter_openers(openers: list):
    global filter_open
    filter_open = openers


def filter_delete():
    global filter_open
    global filter_num
    filter_open = []
    filter_num = []
