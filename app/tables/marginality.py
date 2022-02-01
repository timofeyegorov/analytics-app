import os
import numpy as np
import pandas as pd
import pickle as pkl
from config import RESULTS_FOLDER

def calculate_marginality(leads):
    output = {}
    leads['marginality'] = (leads['turnover_on_lead'] / ((leads['channel_expense2']) * 1.2 + (250 + leads['turnover_on_lead'] * 0.35)) - 1) / \
                           (1 + (leads['turnover_on_lead'] / ((leads['channel_expense2']) * 1.2 + (250 + leads['turnover_on_lead'] * 0.35)) - 1)) * 100
    leads_filter = leads[leads['created_at'] >= '2022-02-01']
    leads_filter['created_at'] = leads_filter['created_at'].dt.normalize()
    unique_dates = leads_filter['created_at'].unique()
    values = [[
        '',
        'Сумма',
        'М>30',
        '15<М<30',
        '0<М<15',
        '-50<М<0',
        '-200<М<-50',
        'М<-200'
    ]]
    for date_ in unique_dates:
        temp_df = leads_filter[leads_filter['created_at'] == date_]
        temp_vals = [
            str(date_)[5:10],
            temp_df.shape[0],
            temp_df[temp_df['marginality'] > 30].shape[0],
            temp_df[(temp_df['marginality'] > 15) & (temp_df['marginality'] <= 30)].shape[0],
            temp_df[(temp_df['marginality'] > 0) & (temp_df['marginality'] <= 15)].shape[0],
            temp_df[(temp_df['marginality'] > -50) & (temp_df['marginality'] <= 0)].shape[0],
            temp_df[(temp_df['marginality'] > -200) & (temp_df['marginality'] <= -50)].shape[0],
            temp_df[(temp_df['marginality'] <= -200)].shape[0],
        ]
        values.append(temp_vals)
    df = pd.DataFrame(values[1:], columns=values[0]).T
    df.columns = df.iloc[0, :]
    df.drop([''], inplace=True)
    output.update({'Общее число': df})

    values = [[
        '',
        'М>30',
        '15<М<30',
        '0<М<15',
        '-50<М<0',
        '-200<М<-50',
        'М<-200'
    ]]
    for date_ in unique_dates:
        temp_df = leads_filter[leads_filter['created_at'] == date_]
        temp_vals = [
            str(date_)[5:10],
            round(temp_df[temp_df['marginality'] > 30].shape[0] / temp_df.shape[0] * 100),
            round(temp_df[(temp_df['marginality'] > 15) & (temp_df['marginality'] <= 30)].shape[0] / temp_df.shape[
                0] * 100),
            round(temp_df[(temp_df['marginality'] > 0) & (temp_df['marginality'] <= 15)].shape[0] / temp_df.shape[
                0] * 100),
            round(temp_df[(temp_df['marginality'] > -50) & (temp_df['marginality'] <= 0)].shape[0] / temp_df.shape[
                0] * 100),
            round(temp_df[(temp_df['marginality'] > -200) & (temp_df['marginality'] <= -50)].shape[0] / temp_df.shape[
                0] * 100),
            round(temp_df[(temp_df['marginality'] <= -200)].shape[0] / temp_df.shape[0] * 100)
        ]
        values.append(temp_vals)
    df = pd.DataFrame(values[1:], columns=values[0]).T
    df.columns = df.iloc[0, :]
    df.drop([''], inplace=True)
    output.update({'Процент': df})
    return output

if __name__=='__main__':
    with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'rb') as f:
        leads = pkl.load(f)
    x = calculate_marginality(leads)
    print(x)
