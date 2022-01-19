import os
import numpy as np
import pandas as pd
import pickle as pkl
from config import RESULTS_FOLDER

def calculate_channels_detailed(df):
    output_df = calculate_channels_by_date(df) # Получаем датасет с разбивкой данных по каждому трафикологу и дате
    filtered_trafficologists = df['trafficologist'].unique()  # Берем уникальных трафикологов
    values = []
    for traff in filtered_trafficologists:
        filtered_df = output_df[output_df['Канал'] == traff]
        start_date = filtered_df['Дата'][filtered_df.index[0]]
        temp_values = [[0] * 33, [0] * 33, [0] * 33]
        for n in range(3):
            temp_values[n][0] = traff
            temp_values[n][-1] = start_date
        for i in filtered_df.index:
            if (start_date.month == filtered_df['Дата'][i].month) & (start_date.year == filtered_df['Дата'][i].year):
                temp_values[0][filtered_df['Дата'][i].day] = filtered_df['Маржинальность'][i]
                temp_values[1][filtered_df['Дата'][i].day] = filtered_df['ROI'][i]
                temp_values[2][filtered_df['Дата'][i].day] = filtered_df['Оборот на лида'][i]
            else:
                start_date = filtered_df['Дата'][i]
                for n in range(3):
                    values.append(temp_values[n])
                temp_values = [[0] * 33, [0] * 33, [0] * 33]
                for n in range(3):
                    temp_values[n][0] = traff
                    temp_values[n][-1] = start_date
                temp_values[0][filtered_df['Дата'][i].day] = filtered_df['Маржинальность'][i]
                temp_values[1][filtered_df['Дата'][i].day] = filtered_df['ROI'][i]
                temp_values[2][filtered_df['Дата'][i].day] = filtered_df['Оборот на лида'][i]
        for n in range(3):
            values.append(temp_values[n])
    out_df = pd.DataFrame(values)

    output_dict = {}
    months_dict = {1: 'Январь', 2: 'Февраль', 3: 'Март', 4: 'Апрель',
                   5: 'Май', 6: 'Июнь', 7: 'Июль', 8: 'Август',
                   9: 'Сентябрь', 10: 'Октябрь', 11: 'Ноябрь', 12: 'Декабрь'
                   }
    for date_ in out_df[32].unique():
        output_dict.update({months_dict[pd.to_datetime(date_).month]:
                                [{'Маржинальность': out_df[out_df[32] == date_].iloc[0::3,:-1]},
                                 {'ROI': out_df[out_df[32] == date_].iloc[1::3,:-1]},
                                 {'Оборот на лида': out_df[out_df[32] == date_].iloc[2::3,:-1]}]
                            })
    return output_dict

def calculate_channels_by_date(df):
    '''
        Функция считает показатели по источникам в разбивке по дням,
        возвращает датасет
    '''
    df.created_at = pd.to_datetime(df.created_at).dt.normalize()
    dates = np.sort(df.created_at.unique()) # Сортируем даты по убыванию
    filtered_trafficologists = df['trafficologist'].unique() # Берем уникальных трафикологов
    values = []
    for el in filtered_trafficologists: # Проходим по каждому трафикологу
        for date_ in dates: # Проходим по каждой дате этого трафиколога
            temp_ = []
            temp_.append(el)
            temp_.append(
                df[(df['trafficologist'] == el) & (df['created_at'] == date_)].shape[0])  # Кол-во лидов
            temp_.append(df[(df['trafficologist'] == el) & (df['created_at'] == date_)][
                             'turnover_on_lead'].sum())  # Оборот
            temp_.append(round(temp_[2] / temp_[1]) if temp_[1] != 0 else 0)  # Оборот на лида
            temp_.append(round(df[(df['trafficologist'] == el) & (df['created_at'] == date_)][
                                   'channel_expense'].sum()) * 1.2)  # Трафик
            if temp_[4] == 0:  # Если расход на трафик равен нулю, то считаем каждый лид по 400 рублей
                temp_[4] = temp_[1] * 400 * 1.2
            temp_.append(round(temp_[1] * 250 + temp_[2] * 0.35))  # Остальное
            temp_.append(round(temp_[2] - temp_[4] - temp_[5]))  # Прибыль
            temp_.append(
                round((temp_[2] / (temp_[4] + temp_[5]) - 1) * 100) if (temp_[4] + temp_[5]) != 0 else 0)  # ROI
            temp_.append(round((temp_[7] / 100) / (1 + (temp_[7] / 100)) * 100) if (1 + (
                        temp_[7] / 100)) != 0 else 0)  # Маржинальность
            temp_.append(date_)  # Дата
            values.append(temp_)
    output_df = pd.DataFrame(
        columns=['Канал', 'Лидов', 'Оборот*', 'Оборот на лида', 'Трафик', 'Остальное', 'Прибыль', 'ROI',
                 'Маржинальность', 'Дата'],
        data=values)
    return output_df

if __name__ == '__main__':
    with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'rb') as f:
        leads = pkl.load(f)
    # print(leads.shape)
    # print(leads.columns)
    result_df = calculate_channels_detailed(leads)
    with open(os.path.join(RESULTS_FOLDER, 'channels_detailed.pkl'), 'wb') as f:
        pkl.dump(result_df, f)
    print(result_df)