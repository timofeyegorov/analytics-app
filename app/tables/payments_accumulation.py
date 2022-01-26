from datetime import timedelta
import numpy as np
import pandas as pd
import os
import pickle as pkl
from config import RESULTS_FOLDER

def calculate_payments_accumulation(payments_df):
    unique_order_dates = np.unique(payments_df['Дата заявки'].tolist())
    values = []
    cur_week = unique_order_dates[0].week
    cur_year = unique_order_dates[0].year
    temp_vals = [0] * 46
    temp_vals[0] = unique_order_dates[0].month_name()
    first_day_week = unique_order_dates[0].day - unique_order_dates[0].weekday()
    last_day_week = (unique_order_dates[0] - timedelta(days=unique_order_dates[0].weekday()) + timedelta(days=6)).day
    temp_vals[1] = str(first_day_week) + ' - ' + str(last_day_week)

    for cur_order_date in unique_order_dates:
        if (cur_order_date.week != cur_week) & (cur_order_date.year == cur_year):
            for i in range(2, 54 - cur_week):
                temp_vals[i + 1] = temp_vals[i + 1] + temp_vals[i]
            values.append(temp_vals)
            cur_week = cur_order_date.week
            cur_year = cur_order_date.year

            temp_vals = [0] * 46
            temp_vals[0] = cur_order_date.month_name()
            first_day_week = cur_order_date.day - cur_order_date.weekday()
            last_day_week = (cur_order_date - timedelta(days=cur_order_date.weekday()) + timedelta(days=6)).day
            temp_vals[1] = str(first_day_week) + ' - ' + str(last_day_week)

        filtered_payments_df = payments_df[payments_df['Дата заявки'] == cur_order_date]
        temp_unique_payment_dates = np.unique(filtered_payments_df['Дата оплаты'].tolist())
        for cur_payment_date in temp_unique_payment_dates:
            if cur_payment_date.week < cur_order_date.week:
                pass
            else:
                temp_vals[cur_payment_date.week - cur_order_date.week + 2] += \
                filtered_payments_df[filtered_payments_df['Дата оплаты'] == cur_payment_date]['Сумма оплаты'].sum()
    res_df = pd.DataFrame(columns=['Месяц', 'Диапазон'] + [i for i in range(1, 45)], data=values)
    return {'Оплаты - сводная таблица': res_df}

if __name__ == '__main__':
    with open(os.path.join(RESULTS_FOLDER, 'payments_table.pkl'), 'rb') as f:
        data = pkl.load(f)
    payments_accumulation = calculate_payments_accumulation(data)
