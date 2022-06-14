import os
import numpy as np
import pandas as pd
import pickle as pkl
from config import RESULTS_FOLDER

def calculate_channels_summary_detailed(df, utm_source, utm_source_value, utm_2, utm_2_value):
    if (utm_2 == '') & (utm_source_value != ''):
        signature = utm_source + '=' + utm_source_value
        output_df = df[(df[utm_source] == utm_source_value)]
    elif (utm_2 != '') & (utm_source_value == ''):
        signature = utm_2 + '=' + utm_2_value
        output_df = df[(df[utm_2] == utm_2_value)]
    elif utm_2 == '':
        utm_2_value = df[df['trafficologist'] == utm_2_value]['utm_source'].unique()[0]
        signature = utm_source + '=' + utm_2_value
        output_df = df[(df[utm_source] == utm_2_value)]
    else:
        output_df = df[(df[utm_source] == utm_source_value) & (df[utm_2] == utm_2_value)]
        signature = utm_source + '=' + utm_source_value + '&' + utm_2 + '=' + utm_2_value
    output_df = output_df[['created_at', 'turnover_on_lead', 'quiz_answers1', 'quiz_answers2', 'quiz_answers3', 'quiz_answers4', 'quiz_answers5']]
    output_df.reset_index(drop=True, inplace=True)
    return {'Лиды в разбивке по: ' + signature: output_df}