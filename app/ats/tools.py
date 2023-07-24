from typing import Union
import pandas as pd
import numpy as np
from preparData import prep_data


# Функция для форматирования числа в процентный формат
def format_percent(x):
    return '{:.2%}'.format(x)


# для фильтра по опенерам
def show_openers_list():
    data = prep_data()
    data_set = data['Откуда'].tolist()
    result = list(set(data_set))
    return result


# для фильтра по номерам
def show_numbers_list():
    data = prep_data()
    data_set = data['ИсходящаяЛиния'].tolist()
    result = list(set(data_set))
    return result


# По номера телефона
def table_number():
    data = prep_data()
    # делаем сводную таблицу
    table = data.pivot_table(index=['ИсходящаяЛиния', 'Откуда'], values=['Дозвон', 'Звонок'],
                             aggfunc=[np.sum], margins=True, margins_name='Всего')
    # добавляем в нее вычисляемое поле
    table['Дозвон%'] = table[('sum', 'Дозвон')] / table[('sum', 'Звонок')]
    # Применяем форматирование процентного значения к столбцу '%'
    table['Дозвон%'] = table['Дозвон%'].apply(format_percent)
    return table


# По номерам опенерам
def table_opener():
    data = prep_data()
    table = data.pivot_table(index=['Откуда', 'ИсходящаяЛиния'], values=['Звонок', 'Дозвон'],
                             aggfunc=[np.sum], margins=True, margins_name='Всего')
    # добавляем в нее вычисляемое поле
    table['Дозвон%'] = table[('sum', 'Дозвон')] / table[('sum', 'Звонок')]
    # Применяем форматирование процентного значения к столбцу '%'
    table['Дозвон%'] = table['Дозвон%'].apply(format_percent)
    return table


# По времени дня
def table_time_day():
    data = prep_data()
    # Делаем объект в Datatime
    data['Время'] = pd.to_datetime(data['Время'], format='%d.%m.%Y %H:%M:%S')
    # Создаем часы
    data['Часы'] = data['Время'].dt.hour
    # Делаем сводную таблицу
    table = data.pivot_table(index='Часы', values=['Звонок', 'Дозвон'], aggfunc=[np.sum], margins=True,
                             margins_name='Всего')
    table['Дозвон%'] = table[('sum', 'Дозвон')] / table[('sum', 'Звонок')]
    table = table.drop(columns=('sum', 'Дозвон'))
    # Применяем форматирование процентного значения к столбцу '%'
    table['Дозвон%'] = table['Дозвон%'].apply(format_percent)
    return table


# Опенер-телефон
def table_opener_number(options: Union[int, str]):
    data = prep_data()
    table = data.pivot_table(index=['Откуда', 'ИсходящаяЛиния'], values=['Звонок', 'Дозвон'],
                             aggfunc=[np.sum], fill_value=0)
    # разрез звонков
    if options == 1:
        # Убираю лишнее поле
        table = table.drop(columns=('sum', 'Дозвон'))
        result_table = table.pivot_table(index='Откуда', columns='ИсходящаяЛиния', fill_value=0)
        return result_table

    # разрез дозвонов
    elif options == 2:
        # добавляем в нее вычисляемое поле
        table['Дозвон%'] = table[('sum', 'Дозвон')] / table[('sum', 'Звонок')]
        table = table.drop(columns=('sum', 'Дозвон'))
        table = table.drop(columns=('sum', 'Звонок'))
        # сводная от сводной, чтобы имея проценты разбить по номерам в столбцах
        result_table = table.pivot_table(index='Откуда', columns='ИсходящаяЛиния', fill_value=0)
        return result_table


# Опенер-время
def table_opener_time(options: Union[int, str]):
    data = prep_data()
    # Делаем объект в Datatime
    data['Время'] = pd.to_datetime(data['Время'], format='%d.%m.%Y %H:%M:%S')
    # Создаем часы
    data['Часы'] = data['Время'].dt.hour
    table = data.pivot_table(index=['Откуда', 'Часы'], values=['Звонок', 'Дозвон'],
                             aggfunc=[np.sum], fill_value=0)
    # разрез звонков
    if options == 1:
        # Убираю лишнее поле
        table = table.drop(columns=('sum', 'Дозвон'))
        result_table = table.pivot_table(index='Откуда', columns='Часы', fill_value=0)
        return result_table
    # разрез дозвонов
    elif options == 2:
        # добавляем в нее вычисляемое поле
        table['Дозвон%'] = table[('sum', 'Дозвон')] / table[('sum', 'Звонок')]
        table = table.drop(columns=('sum', 'Дозвон'))
        table = table.drop(columns=('sum', 'Звонок'))
        # сводная от сводной, чтобы имея проценты разбить по номерам в столбцах
        result_table = table.pivot_table(index='Откуда', columns='Часы', fill_value=0)
        return result_table