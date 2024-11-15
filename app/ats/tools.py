from typing import Union
import pandas as pd
import numpy as np

from app.ats.preparData import prep_data


# Функция для форматирования числа в процентный формат
def format_percent(x):
    percent = x * 100
    if x >= 1:
        return "{:.0f}%".format(percent)
    elif x >= 0.1:
        decimal_part = int((percent % 1) * 10)
        if decimal_part > 5:
            return "{:.0f}%".format(percent)
        else:
            return "{:.1f}%".format(percent)
    else:
        return "{:.2f}%".format(percent)


# для фильтра по опенерам
def show_openers_list():
    data = prep_data()
    data_set = data['ИсходящаяЛиния'].tolist()
    result = list(set(data_set))
    return result


# для фильтра по номерам
def show_numbers_list():
    data = prep_data()
    data_set = data['Схема'].tolist()
    result = list(set(data_set))
    return result


# По номера телефона
def table_number(options: Union[int, str] = None):
    data = prep_data()
    if data.empty:
        return data
    if options is None:
        # делаем сводную таблицу
        table = data.pivot_table(index=['Схема', 'ИсходящаяЛиния'], values=['Дозвон', 'Звонок'],
                                 aggfunc=[np.sum], margins=True, margins_name='Итого')
        # добавляем в нее вычисляемое поле
        table['Дозвон%'] = table[('sum', 'Дозвон')] / table[('sum', 'Звонок')]
        # Применяем форматирование процентного значения к столбцу '%'
        table['Дозвон%'] = table['Дозвон%'].apply(format_percent)
        # Извлекаем строку с итогами
        total_row = table.iloc[-1].copy()
        # Удаляем строку с итогами из исходной таблицы
        table.drop(index='Итого', level='Схема', inplace=True)
        # Вставляем строку с итогами в начало таблицы
        table = pd.concat([total_row.to_frame().T, table], axis=0)
        return table
    else:
        # делаем сводную таблицу
        table = data.pivot_table(index='Схема', values=['Дозвон', 'Звонок'],
                                 aggfunc=[np.sum], margins=True, margins_name='Итого')
        # добавляем в нее вычисляемое поле
        table['Дозвон%'] = table[('sum', 'Дозвон')] / table[('sum', 'Звонок')]
        # Применяем форматирование процентного значения к столбцу '%'
        table['Дозвон%'] = table['Дозвон%'].apply(format_percent)
        # Извлекаем строку с итогами
        total_row = table.iloc[-1].copy()
        table = table.reindex(index=['Итого'] + table.index[:-1].tolist())
        return table


# По номерам опенерам
def table_opener(options: Union[int, str] = None):
    data = prep_data()
    if options is None:
        table = data.pivot_table(index=['ИсходящаяЛиния', 'Схема'], values=['Звонок', 'Дозвон'], aggfunc=[np.sum],
                                 margins=True, margins_name='Итого')
        # добавляем в нее вычисляемое поле
        table['Дозвон%'] = table[('sum', 'Дозвон')] / table[('sum', 'Звонок')]
        # Применяем форматирование процентного значения к столбцу '%'
        table['Дозвон%'] = table['Дозвон%'].apply(format_percent)
        # Извлекаем строку с итогами
        total_row = table.iloc[-1].copy()
        # Удаляем строку с итогами из исходной таблицы
        table.drop(index='Итого', level='ИсходящаяЛиния', inplace=True)
        # Вставляем строку с итогами в начало таблицы
        table = pd.concat([total_row.to_frame().T, table], axis=0)
        return table
    else:
        table = data.pivot_table(index='ИсходящаяЛиния', values=['Звонок', 'Дозвон'], aggfunc=[np.sum],
                                 margins=True, margins_name='Итого')
        # добавляем в нее вычисляемое поле
        table['Дозвон%'] = table[('sum', 'Дозвон')] / table[('sum', 'Звонок')]
        # Применяем форматирование процентного значения к столбцу '%'
        table['Дозвон%'] = table['Дозвон%'].apply(format_percent)
        # Извлекаем строку с итогами
        total_row = table.iloc[-1].copy()
        table = table.reindex(index=['Итого'] + table.index[:-1].tolist())
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
                             margins_name='Итого')
    table['Дозвон%'] = table[('sum', 'Дозвон')] / table[('sum', 'Звонок')]
    # Применяем форматирование процентного значения к столбцу '%'
    table['Дозвон%'] = table['Дозвон%'].apply(format_percent)
    table = table.reindex(index=['Итого'] + table.index[:-1].tolist())
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


# Среднее время дозвонов
def table_timecall():
    data = prep_data()
    table = data.pivot_table(index='Откуда', values=['Длительность звонка', 'Время ответа'], aggfunc=[np.mean],
                             margins=True, margins_name='Итого', fill_value=0)
    # Округляем значения до одного знака после запятой
    # for col in ['Длительность звонка', 'Время ответа']:
    #     table[('mean', col)] = table[('mean', col)].apply(lambda x: round(x, 1))
    #     или
    for col in ['Длительность звонка', 'Время ответа']:
        table[('mean', col)] = table[('mean', col)].round(1).astype(int)
    # Перемещаем итоговую строку вверх
    table = table.reindex(index=['Итого'] + table.index[:-1].tolist())
    return table
