import os
import numpy as np
import pandas as pd
import pickle as pkl
from config import RESULTS_FOLDER

def calculate_channels_summary(df):
    with open(os.path.join(RESULTS_FOLDER, 'trafficologists.pkl'), 'rb') as f:
        traff_data = pkl.load(f)
    # Получаем массив трафикологов в отфильтрованном датасете
    filtered_trafficologists = df['trafficologist'].unique()
    values = []
    for el in filtered_trafficologists:
        temp_ = []
        temp_.append(el)
        temp_.append(df[df['trafficologist'] == el].shape[0])  # Кол-во лидов
        temp_.append(df[df['trafficologist'] == el]['payment_amount'].sum())  # Оборот
        temp_.append(round(temp_[2] / temp_[1], 1))  # Оборот на лида
        temp_.append(df[df['trafficologist'] == el]['channel_expense'].sum())  # Трафик
        temp_.append(temp_[1] * 250 + temp_[2] * 0.35)  # Остальное
        temp_.append(temp_[2] - temp_[4] - temp_[5])  # Прибыль
        temp_.append(round((temp_[3] / (temp_[4] + temp_[5]) - 1) * 100, 1))  # ROI
        temp_.append(round(temp_[7] / (1 + temp_[7]) * 100, 1))  # Маржинальность
        values.append(temp_)
        # for i in range(len(temp_)):
        #     print(i, temp_[i])
    output_df = pd.DataFrame(columns=['Канал', 'Лидов', 'Оборот*', 'Оборот на лида', 'Трафик', 'Остальное', 'Прибыль', 'ROI',
                               'Маржинальность'],
                      data=values)
    return {'Источники - сводная таблица': output_df}