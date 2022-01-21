import os
import numpy as np
import pandas as pd
import pickle as pkl
from config import RESULTS_FOLDER

def calculate_channels_summary(df):

    # Получаем массив трафикологов в отфильтрованном датасете
    filtered_trafficologists = df['trafficologist'].unique()
    values = []
    temp_ = []
    temp_.append('СУММА:')
    temp_.append(df.shape[0]) # Кол-во лидов
    temp_.append(round(df['turnover_on_lead'].sum())) # Оборот
    temp_.append(round(temp_[2] / temp_[1])) # Оборот на лида
    temp_.append(round(df['channel_expense'].sum() * 1.2 + df[df['channel_expense'] == 0].shape[0] * 400 * 1.2)) # Трафик
    # if temp_[4] == 0: # Если расход на трафик равен нулю, то считаем каждый лид по 400 рублей
    #     temp_[4] = round(temp_[1] * 400 * 1.2)
    temp_.append(round(temp_[1] * 250 + temp_[2] * 0.35)) # Остальное
    temp_.append(round(temp_[2] - temp_[4] - temp_[5])) # Прибыль
    temp_.append(round((temp_[2] / (temp_[4] + temp_[5]) - 1) * 100)  if (temp_[4] + temp_[5]) != 0 else 0) # ROI
    temp_.append(round((temp_[7] / 100) / (1 + (temp_[7] / 100)) * 100) if (1 + (temp_[7] / 100)) != 0 else 0) # Маржинальность
    values.append(temp_)
    for el in filtered_trafficologists:
        temp_ = []
        temp_.append(el)
        temp_.append(df[df['trafficologist'] == el].shape[0])  # Кол-во лидов
        temp_.append(round(df[df['trafficologist'] == el]['turnover_on_lead'].sum())) # Оборот
        temp_.append(round(temp_[2] / temp_[1]) if temp_[1] != 0 else 0)  # Оборот на лида
        temp_.append(round(df[df['trafficologist'] == el]['channel_expense'].sum() * 1.2 + \
                           df[(df['trafficologist'] == el) & (df['channel_expense'] == 0)].shape[0] * 400 * 1.2))  # Трафик
        # if temp_[4] == 0:  # Если расход на трафик равен нулю, то считаем каждый лид по 400 рублей
        #     temp_[4] = round(temp_[1] * 400 * 1.2)
        temp_.append(round(temp_[1] * 250 + temp_[2] * 0.35))  # Остальное
        temp_.append(round(temp_[2] - temp_[4] - temp_[5]))  # Прибыль
        temp_.append(round((temp_[2] / (temp_[4] + temp_[5]) - 1) * 100) if (temp_[4] + temp_[5]) != 0 else 0)  # ROI
        temp_.append(round((temp_[7] / 100) / (1 + (temp_[7] / 100)) * 100) if (1 + (temp_[7] / 100)) != 0 else 0)  # Маржинальность
        values.append(temp_)
    output_df = pd.DataFrame(columns=['Канал', 'Лидов', 'Оборот*', 'Оборот на лида', 'Трафик', 'Остальное', 'Прибыль', 'ROI',
                               'Маржинальность'],
                      data=values)
    return {'Источники - сводная таблица': output_df}