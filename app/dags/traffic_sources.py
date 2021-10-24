from app.database import get_accounts, get_target_audience, get_leads_data

import pandas as pd
from urllib import parse
import pickle as pkl
import os
from config import DATA_FOLDER
import numpy as np

path = DATA_FOLDER

def calculate_traffic_sources(df):

    with open(os.path.join(DATA_FOLDER, 'target_audience.pkl'), 'rb') as f:
        target_audience = pkl.load(f)

    with open(os.path.join(DATA_FOLDER, 'trafficologists.pkl'), 'rb') as f:
        traff_data = pkl.load(f)

    status = pd.read_csv('dags/data/status.csv')
    status.fillna(0, inplace=True)

    payment_status = status[status['Оплатил'] == ' +'][
        'Соответствующий статус в воронке Теплые продажи'].unique().tolist()  # Статусы с этапом "оплатил"
    was_conversation_status = status[status['Был разговор'] == ' +'][
        'Соответствующий статус в воронке Теплые продажи'].unique().tolist()  # Статусы с этапом "был разговор"
    in_progress_status = status[status['В работе'] == ' +'][
        'Соответствующий статус в воронке Теплые продажи'].unique().tolist()  # Статусы с этапом "в работе"

    # Получаем массив трафикологов в отфильтрованном датасете
    filtered_trafficologists = df['trafficologist'].unique()
    # Создаем список с трафикологами и названиями их кабинетов - это будут заголовки колонок результирующей таблицы
    created_columns = []
    for name in filtered_trafficologists:
        created_columns.append(name)
        # Проверяем, что у трафиколога более 1 кабинета, чтобы не дублировать инфу, если кабинет только один
        if traff_data[traff_data['name'] == name]['title'].shape[0] > 1:
            created_columns.extend(list(df[df['trafficologist'] == name]['account'].unique()))
    created_columns.sort()

    columns = ['Бюджет', 'Кол-во лидов',
               '% в работе', '% дозвонов', '% офферов', '% счетов',
               'Цена лида',
               '% ЦА 5-6', '% ЦА 5', '% ЦА 6', '% ЦА 4-5-6', '% ЦА 4', '% не ЦА', '% ЦА 3', '% ЦА 2', '% ЦА 1',
               'Цена ЦА 5-6', 'Цена ЦА 4-5-6',
               '% продаж', 'Средний чек', 'Оборот', 'Расход на ОП', 'Расход общий (ОП+бюджет)',
               '% расходов на трафик (доля от Расход общий)', '% расходов на ОП (доля от Расход общий)',
               'ROI', 'Маржа', 'Цена Разговора', 'Цена Оффера', 'Цена Счета',
               'Оборот на лида (на обработанного лида)', 'Оборот на разговор', 'CV обр.лид/оплата',
               'CV разговор/оплата', 'CPO',
               '% лидов (доля от общего)', '% Оборот (доля от общего)', '% Оплат (доля от общего)',
               '% Затрат (доля от общего)']

    data_category = np.zeros((len(created_columns), len(columns))).astype('int')
    df_category = pd.DataFrame(data_category, columns=columns)  # Датасет для процентного кол-ва лидов
    df_category.insert(0, 'Все', 0)  # Столбец для подсчета всех лидов (сумма по всем таргетологам)
    df_category.insert(0, 'Источник', 0)  # Столбец названия подкатегорий указанной категории

    for i, subcategory_name in enumerate(created_columns):
        temp_df = df[(df['trafficologist'] == subcategory_name) | (df['account'] == subcategory_name)]
        if temp_df.shape[0] != 0:

            df_category.loc[i, 'Источник'] = subcategory_name
            df_category.loc[i, 'Бюджет'] = temp_df['channel_expense'].sum()
            df_category.loc[i, 'Кол-во лидов'] = temp_df.shape[0]
            df_category.loc[i, '% в работе'] = round(
                temp_df[temp_df['status_amo'].isin(in_progress_status)].shape[0] / temp_df.shape[0] * 100, 1)
            df_category.loc[i, '% дозвонов'] = round(
                temp_df[temp_df['status_amo'].isin(was_conversation_status)].shape[0] / temp_df.shape[0] * 100, 1)
            df_category.loc[i, '% офферов'] = round(
                temp_df[temp_df['status_amo'] == 'Сделан оффер из Теплые продажи'].shape[0] / temp_df.shape[
                    0] * 100, 1)
            df_category.loc[i, '% счетов'] = round(
                temp_df[temp_df['status_amo'] == 'Выставлен счет из Теплые продажи'].shape[0] / temp_df.shape[
                    0] * 100, 1)
            df_category.loc[i, 'Цена лида'] = round(
                temp_df[temp_df['channel_expense'] != 0]['channel_expense'].sum() / temp_df.shape[0], 1)

            df_category.loc[i, '% ЦА 5-6'] = round(
                temp_df[temp_df['target_class'].isin([5, 6])].shape[0] / temp_df.shape[0] * 100, 2)
            df_category.loc[i, '% ЦА 5'] = round(
                temp_df[temp_df['target_class'] == 5].shape[0] / temp_df.shape[0] * 100, 2)
            df_category.loc[i, '% ЦА 6'] = round(
                temp_df[temp_df['target_class'] == 6].shape[0] / temp_df.shape[0] * 100, 2)
            df_category.loc[i, '% ЦА 4-5-6'] = round(
                temp_df[temp_df['target_class'].isin([4, 5, 6])].shape[0] / temp_df.shape[0] * 100, 2)
            df_category.loc[i, '% ЦА 4'] = round(
                temp_df[temp_df['target_class'] == 4].shape[0] / temp_df.shape[0] * 100, 2)
            df_category.loc[i, '% не ЦА'] = round(
                temp_df[temp_df['target_class'] == 0].shape[0] / temp_df.shape[0] * 100, 2)
            df_category.loc[i, '% ЦА 3'] = round(
                temp_df[temp_df['target_class'] == 3].shape[0] / temp_df.shape[0] * 100, 2)
            df_category.loc[i, '% ЦА 2'] = round(
                temp_df[temp_df['target_class'] == 2].shape[0] / temp_df.shape[0] * 100, 2)
            df_category.loc[i, '% ЦА 1'] = round(
                temp_df[temp_df['target_class'] == 1].shape[0] / temp_df.shape[0] * 100, 2)
            df_category.loc[i, 'Цена ЦА 5-6'] = temp_df[temp_df['target_class'].isin([5, 6])][
                                                    'channel_expense'].sum() / \
                                                temp_df[temp_df['target_class'].isin([5, 6])].shape[0]
            df_category.loc[i, 'Цена ЦА 4-5-6'] = temp_df[temp_df['target_class'].isin([4, 5, 6])][
                                                      'channel_expense'].sum() / \
                                                  temp_df[temp_df['target_class'].isin([4, 5, 6])].shape[0]

            df_category.loc[i, '% продаж'] = round(
                temp_df[temp_df['status_amo'].isin(payment_status)].shape[0] / temp_df.shape[0] * 100, 1)
            df_category.loc[i, 'Средний чек'] = temp_df['payment_amount'].sum() / \
                                                temp_df[temp_df['payment_amount'] != 0].shape[0] if \
            temp_df[temp_df['payment_amount'] != 0].shape[0] != 0 else 0
            df_category.loc[i, 'Оборот'] = temp_df['payment_amount'].sum()
            df_category.loc[i, 'Расход на ОП'] = (temp_df['payment_amount'].sum() * 0.6 + 150 *
                                                  temp_df[temp_df['payment_amount'] != 0].shape[0] + 27 *
                                                  temp_df[temp_df['payment_amount'] != 0].shape[0] + 20 *
                                                  temp_df[temp_df['payment_amount'] != 0].shape[0]) * 1.4
            df_category.loc[i, 'Расход общий (ОП+бюджет)'] = df_category.loc[i, 'Бюджет'] + df_category.loc[
                i, 'Расход на ОП']
            df_category.loc[i, '% расходов на трафик (доля от Расход общий)'] = round(
                df_category.loc[i, 'Бюджет'] / df_category.loc[i, 'Расход общий (ОП+бюджет)'] * 100, 2)
            df_category.loc[i, '% расходов на ОП (доля от Расход общий)'] = round(
                df_category.loc[i, 'Расход на ОП'] / df_category.loc[i, 'Расход общий (ОП+бюджет)'] * 100, 2)
            df_category.loc[i, 'ROI'] = round(
                (temp_df['payment_amount'].sum() - temp_df['channel_expense'].sum()) / temp_df[
                    'channel_expense'].sum() * 100, 2) if temp_df[temp_df['payment_amount'] != 0].shape[
                                                              0] != 0 else 0
            df_category.loc[i, 'Маржа'] = temp_df['payment_amount'].sum() - temp_df['channel_expense'].sum()
            df_category.loc[i, 'Цена Разговора'] = temp_df['channel_expense'].sum() / temp_df[
                temp_df['status_amo'].isin(was_conversation_status)].shape[0] \
                if temp_df[temp_df['status_amo'].isin(was_conversation_status)].shape[0] != 0 else 0
            df_category.loc[i, 'Цена Оффера'] = temp_df['channel_expense'].sum() / temp_df[
                temp_df['status_amo'] == 'Сделан оффер из Теплые продажи'].shape[0] \
                if temp_df[temp_df['channel_expense'] == 'Сделан оффер из Теплые продажи'].shape[0] != 0 else 0
            df_category.loc[i, 'Цена Счета'] = temp_df['channel_expense'].sum() / temp_df[
                temp_df['status_amo'] == 'Выставлен счет из Теплые продажи'].shape[0] \
                if temp_df[temp_df['status_amo'] == 'Выставлен счет из Теплые продажи'].shape[0] != 0 else 0

            df_category.loc[i, 'Оборот на лида (на обработанного лида)'] = df_category.loc[i, 'Оборот'] / temp_df[
                temp_df['status_amo'] == 'Обработанная заявка из Теплые продажи'].shape[0]
            df_category.loc[i, 'Оборот на разговор'] = df_category.loc[i, 'Оборот'] / temp_df[
                temp_df['status_amo'].isin(was_conversation_status)].shape[0]
            df_category.loc[i, 'CV обр.лид/оплата'] = \
            temp_df[temp_df['status_amo'] == 'Обработанная заявка из Теплые продажи'].shape[0] / \
            temp_df[temp_df['payment_amount'] != 0].shape[0] \
                if temp_df[temp_df['payment_amount'] != 0].shape[0] != 0 else 0
            df_category.loc[i, 'CV разговор/оплата'] = \
            temp_df[temp_df['status_amo'].isin(was_conversation_status)].shape[0] / \
            temp_df[temp_df['payment_amount'] != 0].shape[0] \
                if temp_df[temp_df['payment_amount'] != 0].shape[0] != 0 else 0
            # lead_df.loc[i, 'CPO'] = round(temp_df[temp_df['payment_amount'] != 0].sum() / temp_df[temp_df['payment_amount'] != 0].shape[0], 2)
            # lead_df.loc[i, '% лидов (доля от общего)'] = round(temp_df.shape[0]/result.shape[0]*100, 2)
            # lead_df.loc[i, '% Оборот (доля от общего)'] = round(temp_df['payment_amount'].sum()/result['payment_amount'].sum(), 2)
            # lead_df.loc[i, '% Оплат (доля от общего)'] = round(temp_df[temp_df['payment_amount'].isin(payment_status)].shape[0]/result[result['payment_amount'].isin(payment_status)].shape[0], 2)
            # lead_df.loc[i, '% Затрат (доля от общего)'] = round(lead_df.loc[i, 'Бюджет'] + lead_df.loc[i, 'Расход на ОП']/\
            #                                                     (result['channel_expense'].sum() + \
            #                                                      ((result['payment_amount'].sum() * 0.6 + 150 * result[result['payment_amount'] != 0].shape[0] + 27 * result[result['payment_amount'] != 0].shape[0] \
            #                                                       + 20 * result[result['payment_amount'] != 0].shape[0]) * 1.4)*100), 2)
        else:
            pass
    return {'Источники трафика': df_category}