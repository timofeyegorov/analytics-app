import os
import numpy as np
import pandas as pd
import pickle as pkl
from config import RESULTS_FOLDER

def calculate_channels_summary_detailed(df, utm_source, source, utm_2, utm_2_value):
    if (utm_source == '') & (source == '') & (utm_2 == ''):
        if utm_2_value != 'все лиды':
            signature = 'Канал=' + utm_2_value
            output_df = df[df['account'] == utm_2_value]
        else:
            signature = utm_2_value
            output_df = df
    elif (utm_source != ''):
        if utm_2_value != 'все лиды':
            if utm_2 != '':
                signature = 'utm_source=' + utm_source + '&' + utm_2 + '=' + utm_2_value
                output_df = df[(df['utm_source'] == utm_source) & (df[utm_2] == utm_2_value)]
            else:
                signature = 'utm_source=' + utm_source + '(канал)' + utm_2_value
                output_df = df[df['utm_source'] == utm_source]
        else:
                signature = 'Все лиды по utm_source=' + utm_source + '(канал)' + utm_2_value
                output_df = df[df['utm_source'] == utm_source]
    elif (utm_source == '') & (source != ''):
        if utm_2_value != 'все лиды':
            if utm_2 != '':
                signature = 'Канал=' + source + '&' + utm_2 + '=' + utm_2_value
                output_df = df[(df['account'] == source) & (df[utm_2] == utm_2_value)]
            else:
                signature = 'Канал=' + utm_2_value
                output_df = df[df['account'] == utm_2_value]
        else:
            signature = 'Все лиды по каналу=' + source
            output_df = df[df['account'] == source]
    elif (utm_source == '') & (source == '') & (utm_2 != ''):
        if utm_2_value != 'все лиды':
            signature = utm_2 + '=' + utm_2_value
            output_df = df[(df[utm_2] == utm_2_value)]
        else:
            signature = utm_2_value
            output_df = df
    else:
        signature = 'Неизвестная фильтрация'
        output_df = df

    # if (utm_2 == '') & (utm_source_value != ''):
    #     print(1)
    #     signature = utm_source + '=' + utm_source_value
    #     if utm_source == 'канал':
    #         output_df = df[(df['trafficologist'] == utm_source_value)]
    #     else:
    #         output_df = df[(df[utm_source] == utm_source_value)]
    # elif (utm_2 != '') & (utm_source_value == ''):
    #     print(2)
    #     if utm_2_value == 'все лиды':
    #         print(2.1)
    #         signature = utm_2_value
    #         output_df = df
    #     else:
    #         print(2.2)
    #         signature = utm_2 + '=' + utm_2_value
    #         output_df = df[(df[utm_2] == utm_2_value)]
    # elif (utm_2 == '') & (utm_source_value == ''):
    #     print(3)
    #     # utm_2_value = df[df['trafficologist'] == utm_2_value]['utm_source'].unique()[0]
    #     if utm_2_value == 'все лиды':
    #         print(3.1)
    #         signature = utm_2_value
    #         output_df = df
    #     else:
    #         print(3.2)
    #         signature = utm_source + '=' + utm_2_value
    #         output_df = df[(df['trafficologist'] == utm_2_value)]
    # else:
    #     print(4)
    #     if utm_2_value == 'все лиды':
    #         print(3.1)
    #         signature = utm_source + '=' + utm_source_value + '&' + utm_2
    #         if utm_source == 'канал':
    #             output_df = df[(df['trafficologist'] == utm_source_value)]
    #         else:
    #             output_df = df[(df[utm_source] == utm_source_value)]
    #     else:
    #         signature = utm_source + '=' + utm_source_value + '&' + utm_2 + '=' + utm_2_value
    #         if utm_source == 'канал':
    #             output_df = df[(df['trafficologist'] == utm_source_value) & (df[utm_2] == utm_2_value)]
    #         else:
    #             output_df = df[(df[utm_source] == utm_source_value) & (df[utm_2] == utm_2_value)]
    output_df = output_df[['created_at', 'turnover_on_lead', 'quiz_answers1', 'quiz_answers2', 'quiz_answers3', 'quiz_answers4', 'quiz_answers5']]
    output_df.reset_index(drop=True, inplace=True)
    return {'Лиды в разбивке по: ' + signature: output_df}

