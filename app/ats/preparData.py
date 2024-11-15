import re
import pandas as pd

file_path = 'app/ats/api/data.csv'
filter_open: list = ['236 (Хмелева Мария)', '306 (Исангильдина Гузель)', '565 (Шустанова Наталья)', '576 (Жердева Ангелина)', '645 (Филимонова Оксана)', '798 (Михайлов Максим)', '852 (Жакевич Артем)']
filter_num: list = []
settings_open: list = []
data_range = ''


def contains_digits(s):
    return bool(re.search(r'\d', s))


def format_phone(number: str) -> str:
    if not contains_digits(number):
        return number
    digits = re.sub(r'\D', '', number)
    bias = 1 if digits.startswith('9') else 0
    return (f"+7 {digits[1 - bias:4 - bias]} {digits[4 - bias:7 - bias]}"
            f"-{digits[7 - bias:9 - bias]}-{digits[9 - bias:]}")


# Функция подготовки базовой таблицы для всех срезов
def prep_data():
    global filter_num
    global filter_open
    global settings_open
    try:
        data = pd.read_csv(file_path, delimiter=';', low_memory=False)
    except FileNotFoundError:
        return pd.DataFrame({}, columns=['Схема', 'ИсходящаяЛиния', 'Дозвон', 'Звонок'])
    # TODO может быть стоит брать этот параметр 'Информация из CRM' в работу вместо Исходящая линия
    # удалим лишние данные
    new_data = data.drop(
        columns=['Откуда', 'Кто разговаривал', 'Кто ответил', 'Оценка', 'ID записи', 'Метка', 'Теги', 'ID заказа звонка',
                 'Запись существует', 'Новый клиент', 'Состояние перезвона', 'Время перезвона', 'Информация из CRM',
                 'Ответственный из CRM', 'Unnamed: 0'])
    new_data = new_data[~new_data['Схема'].isna()]
    # форматируем телефоны
    new_data['Схема'] = new_data['Схема'].apply(format_phone)
    new_data['Схема'] = new_data['Схема'].astype(str)
    # добавим нужные столбы
    new_data['Дозвон'] = [1 if duration > 5 and status == 'Отвечен' else 0 for duration, status in
                          zip(new_data['Длительность разговора'], new_data['Тип'])]
    new_data['Звонок'] = 1
    # Это нужно чтобы работал query, с пробелами в названии не работает
    new_data = new_data.rename(columns={'Исходящая линия': 'ИсходящаяЛиния'})
    if settings_open != []:
        new_data = new_data.query(f'Схема == {settings_open}')
    if filter_open != []:
        new_data = new_data.query(f'ИсходящаяЛиния == {filter_open}')
    if filter_num != []:
        new_data = new_data[new_data['Схема'].isin(filter_num)]
    return new_data


# TODO подход с global ущербный, пока так, но нужно переделать
def filter_numbers(numbers: list):
    global filter_num
    filter_num = numbers


def filter_openers(openers: list):
    global filter_open
    filter_open = openers


def settings_openers(openers: list):
    global settings_open
    settings_open = openers


def filter_delete():
    global filter_open
    global filter_num
    global settings_open
    filter_open = settings_open
    filter_num = []


def settings_delete():
    global settings_open
    global filter_open
    data = pd.read_csv(file_path, delimiter=';', low_memory=False)
    data_set = data['Откуда'].tolist()
    result = list(set(data_set))
    settings_open = result
    filter_open = result
    return settings_open


def set_datarange(data):
    global data_range
    data_range = data


def show_datarange():
    global data_range
    return data_range
