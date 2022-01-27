from datetime import timedelta
import numpy as np
import pandas as pd
import os
import pickle as pkl
from config import RESULTS_FOLDER
import json

def get_expenses_df():
    with open(os.path.join(RESULTS_FOLDER, 'expenses.json'), 'r', encoding='cp1251') as f:
        expenses = json.load(f)
    exp = []
    for i in range(len(expenses)):
        for k, v in expenses[i]['items'].items():
            exp.append([k, v,
                        expenses[i]['dateFrom'],
                        ])
    exp = pd.DataFrame(exp, columns=['Ройстат', 'Расход', 'Дата'])
    exp['Дата'] = pd.to_datetime(exp['Дата']).dt.normalize()
    return exp

def payments_week_accumulation(payments_df):
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
    for i in range(2, 54 - cur_week):
        temp_vals[i + 1] = temp_vals[i + 1] + temp_vals[i]
    values.append(temp_vals)

    res_df = pd.DataFrame(columns=['Месяц', 'Диапазон'] + [i for i in range(1, 45)], data=values)
    return res_df

def payments_month_accumulation(payments_df):
    unique_order_dates = np.unique(payments_df['Дата заявки'].tolist())

    values = []
    cur_month = unique_order_dates[0].month
    cur_year = unique_order_dates[0].year

    temp_vals = [0] * 11
    temp_vals[0] = unique_order_dates[0].month_name()

    for cur_order_date in unique_order_dates:

        if (cur_order_date.month != cur_month) & (cur_order_date.year == cur_year):
            for i in range(1, 13 - cur_month):
                temp_vals[i+1] = temp_vals[i+1] + temp_vals[i]
            values.append(temp_vals)
            cur_month = cur_order_date.month
            cur_year = cur_order_date.year

            temp_vals = [0] * 11
            temp_vals[0] = cur_order_date.month_name()

        filtered_payments_df = payments_df[payments_df['Дата заявки'] == cur_order_date]
        temp_unique_payment_dates = np.unique(filtered_payments_df['Дата оплаты'].tolist())
        for cur_payment_date in temp_unique_payment_dates:
            temp_vals[cur_payment_date.month - cur_payment_date.month + 1] += filtered_payments_df[filtered_payments_df['Дата оплаты'] == cur_payment_date]['Сумма оплаты'].sum()
    for i in range(1, 12 - cur_month):
        temp_vals[i+1] = temp_vals[i+1] + temp_vals[i]
    values.append(temp_vals)

    res_df = pd.DataFrame(columns=['Месяц'] + [i for i in range(1, 11)], data=values)
    return res_df

def roi_week_accumulation(payments_df):
    exp = get_expenses_df()
    unique_order_dates = np.unique(payments_df['Дата заявки'].tolist())

    values = []
    expenses = []
    cur_week = unique_order_dates[0].week
    cur_year = unique_order_dates[0].year
    temp_vals = [0] * 46
    temp_exp = [0] * 46

    temp_vals[0] = unique_order_dates[0].month_name()
    temp_exp[0] = unique_order_dates[0].month_name()

    first_day_week = unique_order_dates[0].day - unique_order_dates[0].weekday()
    last_day_week = (unique_order_dates[0] - timedelta(days=unique_order_dates[0].weekday()) + timedelta(days=6)).day
    temp_vals[1] = str(first_day_week) + ' - ' + str(last_day_week)
    temp_exp[1] = str(first_day_week) + ' - ' + str(last_day_week)

    for cur_order_date in unique_order_dates:
        if (cur_order_date.week != cur_week) & (cur_order_date.year == cur_year):
            for i in range(2, 54 - cur_week):
                temp_vals[i + 1] = temp_vals[i + 1] + temp_vals[i]
                temp_exp[i + 1] = temp_exp[i + 1] + temp_exp[i]
            values.append(temp_vals)
            expenses.append(temp_exp)
            cur_week = cur_order_date.week
            cur_year = cur_order_date.year

            temp_vals = [0] * 46
            temp_vals[0] = cur_order_date.month_name()

            temp_exp = [0] * 46
            temp_exp[0] = cur_order_date.month_name()

            first_day_week = cur_order_date.day - cur_order_date.weekday()
            last_day_week = (cur_order_date - timedelta(days=cur_order_date.weekday()) + timedelta(days=6)).day
            temp_vals[1] = str(first_day_week) + ' - ' + str(last_day_week)
            temp_exp[1] = str(first_day_week) + ' - ' + str(last_day_week)

        filtered_payments_df = payments_df[payments_df['Дата заявки'] == cur_order_date]
        temp_unique_payment_dates = np.unique(filtered_payments_df['Дата оплаты'].tolist())
        for cur_payment_date in temp_unique_payment_dates:
            if cur_payment_date.week < cur_order_date.week:
                pass
            else:
                temp_vals[cur_payment_date.week - cur_order_date.week + 2] += \
                filtered_payments_df[filtered_payments_df['Дата оплаты'] == cur_payment_date]['Сумма оплаты'].sum()
                temp_exp[cur_payment_date.week - cur_order_date.week + 2] += \
                exp[exp['Дата'] == str(cur_payment_date)[:10]]['Расход'].sum()
    for i in range(2, 54 - cur_week):
        temp_vals[i + 1] = temp_vals[i + 1] + temp_vals[i]
        temp_exp[i + 1] = temp_exp[i + 1] + temp_exp[i]
    values.append(temp_vals)
    expenses.append(temp_exp)

    np_values = np.array(values)
    np_expenses = np.array(expenses)

    np_values_str = np.array(np_values[:, :2])

    np_values = np_values[:, 2:].astype('float32')
    np_expenses = np_expenses[:, 2:].astype('float32')
    vals = np_values / np_expenses * 100
    vals[vals == np.inf] = 0
    vals = np.nan_to_num(vals)
    vals = np.around(vals, decimals=1)
    vals = np.concatenate((np_values_str, vals), axis=1)
    res_df = pd.DataFrame(columns=['Месяц', 'Диапазон'] + [i for i in range(1, 45)], data=vals)
    return res_df

def roi_month_accumulation(payments_df):
    exp = get_expenses_df()
    unique_order_dates = np.unique(payments_df['Дата заявки'].tolist())

    values = []
    expenses = []
    cur_month = unique_order_dates[0].month
    cur_year = unique_order_dates[0].year
    temp_vals = [0] * 11
    temp_exp = [0] * 11
    temp_vals[0] = unique_order_dates[0].month_name()
    temp_exp[0] = unique_order_dates[0].month_name()

    for cur_order_date in unique_order_dates:

        if (cur_order_date.month != cur_month) & (cur_order_date.year == cur_year):
            for i in range(1, 13 - cur_month):
                temp_vals[i + 1] = temp_vals[i + 1] + temp_vals[i]
                temp_exp[i + 1] = temp_exp[i + 1] + temp_exp[i]
            values.append(temp_vals)
            expenses.append(temp_exp)
            cur_month = cur_order_date.month
            cur_year = cur_order_date.year

            temp_vals = [0] * 11
            temp_vals[0] = cur_order_date.month_name()
            temp_exp = [0] * 11
            temp_exp[0] = cur_order_date.month_name()

        filtered_payments_df = payments_df[payments_df['Дата заявки'] == cur_order_date]
        temp_unique_payment_dates = np.unique(filtered_payments_df['Дата оплаты'].tolist())
        for cur_payment_date in temp_unique_payment_dates:
            if cur_payment_date.month < cur_order_date.month:
                pass
            else:
                temp_vals[cur_payment_date.month - cur_payment_date.month + 1] += \
                filtered_payments_df[filtered_payments_df['Дата оплаты'] == cur_payment_date]['Сумма оплаты'].sum()
                temp_exp[cur_payment_date.month - cur_order_date.month + 1] += \
                exp[exp['Дата'] == str(cur_payment_date)[:10]]['Расход'].sum()

    for i in range(1, 12 - cur_month):
        temp_vals[i + 1] = temp_vals[i + 1] + temp_vals[i]
        temp_exp[i + 1] = temp_exp[i + 1] + temp_exp[i]
    values.append(temp_vals)
    expenses.append(temp_exp)

    np_values = np.array(values)
    np_expenses = np.array(expenses)

    np_values_str = np.array(np_values[:, :1])

    np_values = np_values[:, 1:].astype('float32')
    np_expenses = np_expenses[:, 1:].astype('float32')
    vals = np_values / np_expenses * 100
    vals[vals == np.inf] = 0
    vals = np.nan_to_num(vals)
    vals = np.around(vals, decimals=1)
    vals = np.concatenate((np_values_str, vals), axis=1)
    res_df = pd.DataFrame(columns=['Месяц'] + [i for i in range(1, 11)], data=vals)
    return res_df

def calculate_payments_accumulation(df):
    payments_week_acc = payments_week_accumulation(df)
    payments_month_acc = payments_month_accumulation(df)
    roi_week_acc = roi_week_accumulation(df)
    roi_month_acc = roi_month_accumulation(df)

    return {'Оплаты - накопление по неделям': payments_week_acc,
            'Оплаты - накопление по месяцам': payments_month_acc,
            'Рои - накопление по неделям': roi_week_acc,
            'Рои - накопление по месяцам': roi_month_acc}

if __name__ == '__main__':
    with open(os.path.join(RESULTS_FOLDER, 'payments_table.pkl'), 'rb') as f:
        data = pkl.load(f)
    payments_accumulation = calculate_payments_accumulation(data)
    print(type(payments_accumulation))
    print(len(payments_accumulation))
    for item in payments_accumulation.values():
      # print(item)
      print(type(item))
    print()
