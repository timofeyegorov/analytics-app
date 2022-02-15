import os
import numpy as np
import pandas as pd
import pickle as pkl
from config import RESULTS_FOLDER

profession = ['IT сфера', 'Связано с числами', 'Гуманитарий', 'Предприниматель, руководитель', 'Другое']
education = ['Да, работа', 'Да, проект', 'Нет']
age = [
       'до 18 лет', '18 - 23', '18 - 25', '24 - 30', '26 - 30',
       '31 - 40', '41 - 50', '51 - 60', '60 +'
       ]
earning = ['0 руб.', 'до 30 000 руб.', 'до 60 000 руб.',
           'до 100 000 руб.', 'более 100 000 руб.']

def calculate_type_by_date(df, subcategory, column):
    '''
        Функция считает количество лидов по каждому типу аудитории в разбивке по дням,
        возвращает датасет
    '''
    # df = df.iloc[:1000,:]
    df.created_at = pd.to_datetime(df.created_at).dt.normalize()
    dates = np.sort(df.created_at.unique()) # Сортируем даты по убыванию
    values = []
    for el in subcategory: # Проходим по каждому типу
        for date_ in dates: # Проходим по каждой дате этого трафиколога
            temp_ = []
            temp_.append(el)
            if el != 'Другое':
                temp_.append(
                    df[(df[column] == el) & (df['created_at'] == date_)].shape[0])  # Кол-во лидов
            else:
                temp_.append(
                    df[(~df[column].isin(subcategory)) & (df['created_at'] == date_)].shape[0])  # Кол-во лидов
            temp_.append(date_)  # Дата
            values.append(temp_)
    output_df = pd.DataFrame(
        columns=['Тип', 'Лидов', 'Дата'],
        data=values)
    return output_df

def calculate_audience_type(df, subcategory, column):
    output_df = calculate_type_by_date(df, subcategory, column) # Получаем датасет с разбивкой данных по каждому трафикологу и дате

    values = []
    for category in subcategory:

        filtered_df = output_df[output_df['Тип'] == category]
        start_date = filtered_df['Дата'][filtered_df.index[0]]
        temp_values = [0] * 33

        temp_values[0] = category
        temp_values[-1] = start_date

        for i in filtered_df.index:
            if (start_date.month == filtered_df['Дата'][i].month) & (
                    start_date.year == filtered_df['Дата'][i].year):
                temp_values[filtered_df['Дата'][i].day] = filtered_df['Лидов'][i]
            else:
                start_date = filtered_df['Дата'][i]
                values.append(temp_values)
                temp_values = [0] * 33
                temp_values[0] = category
                temp_values[-1] = start_date
                temp_values[filtered_df['Дата'][i].day] = filtered_df['Лидов'][i]

        values.append(temp_values)

    out_df = pd.DataFrame(values)
    output_dict = {}
    months_dict = {1: 'Январь', 2: 'Февраль', 3: 'Март', 4: 'Апрель',
                   5: 'Май', 6: 'Июнь', 7: 'Июль', 8: 'Август',
                   9: 'Сентябрь', 10: 'Октябрь', 11: 'Ноябрь', 12: 'Декабрь'
                   }

    for date_ in out_df[32].unique():
        output_dict.update({str(pd.to_datetime(date_).year) + '_' + (str(0) + str(pd.to_datetime(date_).month))[-2:] + '_' + months_dict[pd.to_datetime(date_).month]:
                                out_df[out_df[32] == date_]
                            })
    return output_dict

def calculate_audience_tables():
    with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'rb') as f:
        leads = pkl.load(f)

    profession_tables = calculate_audience_type(leads, profession, 'quiz_answers3')
    education_tables = calculate_audience_type(leads, education, 'quiz_answers5')
    age_tables = calculate_audience_type(leads, age, 'quiz_answers2')
    earning_tables = calculate_audience_type(leads, earning, 'quiz_answers4')

    month_list = np.unique(
        list(profession_tables.keys()) + \
        list(education_tables.keys()) + \
        list(age_tables.keys()) + \
        list(earning_tables.keys())
    ).tolist()

    output_dict = {}
    for month in month_list:
        output_dict.update(
            {
                month: [
                    {'Профессия': profession_tables[month]},
                    {'Образование': education_tables[month]},
                    {'Возраст': age_tables[month]},
                    {'Заработок': earning_tables[month]}
                ]
            }
        )
    return output_dict
