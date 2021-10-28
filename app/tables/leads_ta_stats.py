import numpy as np
import pickle as pkl
import pandas as pd
import os
from config import DATA_FOLDER, RESULTS_FOLDER

def calculate_leads_ta_stats(df):
    """
        function calculates leads target audience report
    :return: dataframe report
    """
    df.reset_index(inplace=True, drop=True)
    with open(os.path.join((DATA_FOLDER), 'statuses.pkl'), 'rb') as f:
        status = pkl.load(f)

    payment_status = status[status['Продажа'] == ' +'][
        'Статус АМО'].unique().tolist()  # Статусы с этапом "Продажа"
    conversation_status = status[status['Дозвон'] == ' +'][
        'Статус АМО'].unique().tolist()  # Статусы с этапом "Дозвон"
    progress_status = status[status['В работе'] == ' +'][
        'Статус АМО'].unique().tolist()  # Статусы с этапом "В работе"
    offer_status = status[status['Оффер'] == ' +'][
        'Статус АМО'].unique().tolist()  # Статусы с этапом "Оффер"
    bill_status = status[status['Счет'] == ' +'][
        'Статус АМО'].unique().tolist()  # Статусы с этапом "Счет"
    done_status = status[status['Обработанная заявка'] == ' +'][
        'Статус АМО'].unique().tolist()  # Статусы с этапом "Обработанная заявка"

    columns = ['Кол-во лидов',
               '% в работе', '% дозвонов', '% офферов', '% счетов', '% продаж',
               'Цена лида', 'Средний чек', 'Оборот',
               'Бюджет', 'Расход на ОП', 'Расход общий (ОП+бюджет)',
               '% расходов на трафик (доля от Расход общий)', '% расходов на ОП (доля от Расход общий)',
               'ROI ?', 'ROMI', 'ROSI', 'ROI', 'Маржа', 'Маржа %', 'Цена Разговора', 'Цена Оффера', 'Цена Счета',
               'Оборот на лида (на обработанного лида)', 'Оборот на разговор', 'CV обр.лид/оплата',
               'CV разговор/оплата', 'CPO', '% лидов (доля от общего)', '% Оборот (доля от общего)',
               '% Оплат (доля от общего)', '% Затрат (доля от общего)'
    ]

    data = np.zeros((9, len(columns))).astype('int')
    output_df = pd.DataFrame(data=data, columns=columns)

    for i in range(7):
        temp_df = df[df['target_class'] == i]
        finished_leads = float(temp_df[temp_df['status_amo'].isin(done_status)].shape[0])
        if temp_df.shape[0] != 0:
            output_df.loc[i, 'Кол-во лидов'] = temp_df.shape[0]

            output_df.loc[i, '% в работе'] = round(temp_df[temp_df['status_amo'].isin(progress_status)].shape[0] \
                                             / temp_df.shape[0] * 100, 1)
            output_df.loc[i, '% дозвонов'] = round(temp_df[temp_df['status_amo'].isin(conversation_status)].shape[0] \
                                             / temp_df.shape[0] * 100, 1)
            output_df.loc[i, '% офферов'] = round(temp_df[temp_df['status_amo'].isin(offer_status)].shape[0] \
                                            / temp_df.shape[0] * 100, 1)
            output_df.loc[i, '% счетов'] = round(temp_df[temp_df['status_amo'].isin(bill_status)].shape[0] \
                                           / temp_df.shape[0] * 100, 1)
            output_df.loc[i, '% продаж'] = round(temp_df[temp_df['status_amo'].isin(payment_status)].shape[0] \
                                           / temp_df.shape[0] * 100, 1)

            output_df.loc[i, 'Цена лида'] = round(temp_df['channel_expense'].sum() / temp_df.shape[0], 1)
            output_df.loc[i, 'Средний чек'] = round(temp_df['payment_amount'].sum() \
                                              / temp_df[temp_df['payment_amount']!= 0].shape[0], 1)\
                                              if temp_df[temp_df['payment_amount'] != 0].shape[0] != 0 else 0
            output_df.loc[i, 'Оборот'] = round(float(temp_df['payment_amount'].sum()), 1)
            output_df.loc[i, 'Бюджет'] = round(float(temp_df['channel_expense'].sum()), 1)
            output_df.loc[i, 'Расход на ОП'] = round(float(output_df.loc[i, 'Оборот']) * 0.06
                                               + 150 * float(temp_df[temp_df['payment_amount'] != 0].shape[0])
                                               + 27 * float(temp_df[temp_df['payment_amount'] != 0].shape[0])
                                               + 20 * float(temp_df[temp_df['payment_amount'] != 0].shape[0]) * 1.4, 1)

            output_df.loc[i, 'Расход общий (ОП+бюджет)'] = float(output_df.loc[i, 'Бюджет'])\
                                                           + float(output_df.loc[i, 'Расход на ОП'])

            output_df.loc[i, '% расходов на трафик (доля от Расход общий)'] =\
                round(float(output_df.loc[i, 'Бюджет']) / output_df.loc[i, 'Расход общий (ОП+бюджет)'] * 100, 1)\
                if output_df.loc[i, 'Расход общий (ОП+бюджет)'] != 0 else 0

            output_df.loc[i, '% расходов на ОП (доля от Расход общий)'] =\
                round(output_df.loc[i, 'Расход на ОП'] / output_df.loc[i, 'Расход общий (ОП+бюджет)'] * 100, 1) \
                if output_df.loc[i, 'Расход общий (ОП+бюджет)'] != 0 else 0

            output_df.loc[i, 'ROI ?'] = round((float(temp_df['payment_amount'].sum())
                                        - float(temp_df['channel_expense'].sum()))
                                        / float(temp_df['channel_expense'].sum()) * 100, 1)\
                                        if temp_df['channel_expense'].sum() != 0 else 0

            output_df.loc[i, 'ROMI'] = round((output_df.loc[i, 'Оборот'] / output_df.loc[i, 'Бюджет']) - 1, 1)
            output_df.loc[i, 'ROSI'] = round((output_df.loc[i, 'Оборот'] / output_df.loc[i, 'Расход на ОП']) - 1, 1) \
                                                                    if output_df.loc[i, 'Расход на ОП'] != 0 else -1
            output_df.loc[i, 'ROI'] = round((output_df.loc[i, 'Оборот'] / output_df.loc[i, 'Расход общий (ОП+бюджет)']) - 1, 1) \
                                                            if output_df.loc[i, 'Расход общий (ОП+бюджет)'] != 0 else -1

            output_df.loc[i, 'Маржа'] = float(temp_df['payment_amount'].sum())\
                                        - float(output_df.loc[i, 'Расход общий (ОП+бюджет)'])
            output_df.loc[i, 'Маржа %'] = float(output_df.loc[i, 'Маржа']) \
                                          / float(output_df.loc[i, 'Оборот']) \
                                          if output_df.loc[i, 'Оборот'] != 0 else 0

            output_df.loc[i, 'Цена Разговора'] = round(temp_df['channel_expense'].sum() / \
                                                 temp_df[temp_df['status_amo'].isin(conversation_status)].shape[0], 1) \
                                        if temp_df[temp_df['status_amo'].isin(conversation_status)].shape[0] != 0 else 0


            output_df.loc[i, 'Цена Оффера'] = round(temp_df['channel_expense'].sum() / \
                                              temp_df[temp_df['status_amo'].isin(offer_status)].shape[0], 1) \
                                              if temp_df[temp_df['status_amo'].isin(offer_status)].shape[0] != 0 else 0

            output_df.loc[i, 'Цена Счета'] = round(temp_df['channel_expense'].sum() / \
                                             temp_df[temp_df['status_amo'].isin(bill_status)].shape[0], 1) \
                                             if temp_df[temp_df['status_amo'].isin(bill_status)].shape[0] != 0 else 0

            output_df.loc[i, 'Оборот на лида (на обработанного лида)'] = round(float(output_df.loc[i, 'Оборот']) /\
                                                                         finished_leads, 1) \
                                                                         if finished_leads != 0 else 0

            output_df.loc[i, 'Оборот на разговор'] = round(float(output_df.loc[i, 'Оборот']) / \
                                        temp_df[temp_df['status_amo'].isin(conversation_status)].shape[0], 1) \
                                        if temp_df[temp_df['status_amo'].isin(conversation_status)].shape[0] != 0 else 0

            output_df.loc[i, 'CV обр.лид/оплата'] = round(finished_leads \
                                                    / temp_df[temp_df['payment_amount'] != 0].shape[0], 1) \
                                                    if temp_df[temp_df['payment_amount'] != 0].shape[0] != 0 else 0

            output_df.loc[i, 'CV разговор/оплата'] = \
                                            round(temp_df[temp_df['status_amo'].isin(conversation_status)].shape[0] \
                                            / temp_df[temp_df['payment_amount'] != 0].shape[0], 1)\
                                            if temp_df[temp_df['payment_amount'] != 0].shape[0] != 0 else 0


            output_df.loc[i, 'CPO'] = round(output_df.loc[i, 'Расход общий (ОП+бюджет)'] \
                                      / temp_df[temp_df['status_amo'].isin(payment_status)].shape[0], 1) \
                                      if temp_df[temp_df['status_amo'].isin(payment_status)].shape[0] != 0 else 0

            output_df.loc[i, '% лидов (доля от общего)'] = round(temp_df.shape[0]/df.shape[0]*100, 1)
            output_df.loc[i, '% Оборот (доля от общего)'] = round(float(temp_df['payment_amount'].sum()) \
                                                                  /float(df['payment_amount'].sum()), 1) \
                                                                  if df['payment_amount'].sum() != 0 else 0
            output_df.loc[i, '% Оплат (доля от общего)'] = \
                                                round(temp_df[temp_df['status_amo'].isin(payment_status)].shape[0] \
                                                /df[df['status_amo'].isin(payment_status)].shape[0], 1) \
                                                if df[df['status_amo'].isin(payment_status)].shape[0] != 0 else 0

            output_df.loc[i, '% Затрат (доля от общего)'] = \
                                            round(float(output_df.loc[i, 'Расход общий (ОП+бюджет)']) / \
                                            (float(df['channel_expense'].sum()) + \
                                            ((float(df['payment_amount'].sum()) * 0.06 \
                                              + 150 * float(df[df['payment_amount'] != 0].shape[0]) \
                                              + 27 * float(df[df['payment_amount'] != 0].shape[0]) \
                                              + 20 * float(df[df['payment_amount'] != 0].shape[0])) * 1.4)) * 100, 1) \
                                              if (float(df['channel_expense'].sum()) + \
                                              ((float(df['payment_amount'].sum()) * 0.6 \
                                              + 150 * float(df[df['payment_amount'] != 0].shape[0]) \
                                              + 27 * float(df[df['payment_amount'] != 0].shape[0]) \
                                              + 20 * float(df[df['payment_amount'] != 0].shape[0])) * 1.4) != 0) else 0
        else:
            pass

    for col in columns:
        if col not in ['% в работе', '% дозвонов', '% офферов', '% счетов', '% продаж',
                       'Цена лида', 'Средний чек',
                       'Средний чек', '% расходов на трафик (доля от Расход общий)', '% расходов на ОП (доля от Расход общий)',
                       'ROI ?', 'ROMI', 'ROSI', 'ROI']:
            try:
                output_df.loc[7, col] = output_df.loc[5, col] + output_df.loc[6, col]
                output_df.loc[8, col] = output_df.loc[:6, col].sum()
            except TypeError as ex:
                output_df.loc[7, col] = '?'

        elif col == '% в работе':
            output_df.loc[7, '% в работе'] = round(df[(df['target_class'].isin([5,6])) & \
                                                  (df['status_amo'].isin(progress_status))].shape[0] / \
                                                   df[df['target_class'].isin([5,6])].shape[0] * 100, 1)
            output_df.loc[8, '% в работе'] = round(df[df['status_amo'].isin(progress_status)].shape[0] / \
                                                   df.shape[0] * 100, 1)

        elif col == '% дозвонов':
            output_df.loc[7, '% дозвонов'] = round(df[(df['target_class'].isin([5,6])) & \
                                                  (df['status_amo'].isin(conversation_status))].shape[0] / \
                                                   df[df['target_class'].isin([5,6])].shape[0] * 100, 1)
            output_df.loc[8, '% дозвонов'] = round(df[df['status_amo'].isin(conversation_status)].shape[0] / \
                                                   df.shape[0] * 100, 1)

        elif col == '% офферов':
            output_df.loc[7, '% офферов'] = round(df[(df['target_class'].isin([5,6])) & \
                                                  (df['status_amo'].isin(offer_status))].shape[0] / \
                                                   df[df['target_class'].isin([5,6])].shape[0] * 100, 1)
            output_df.loc[8, '% офферов'] = round(df[df['status_amo'].isin(offer_status)].shape[0] / \
                                                   df.shape[0] * 100, 1)

        elif col == '% счетов':
            output_df.loc[7, '% счетов'] = round(df[(df['target_class'].isin([5,6])) & \
                                                  (df['status_amo'].isin(bill_status))].shape[0] / \
                                                   df[df['target_class'].isin([5,6])].shape[0] * 100, 1)
            output_df.loc[8, '% счетов'] = round(df[df['status_amo'].isin(bill_status)].shape[0] / \
                                                   df.shape[0] * 100, 1)

        elif col == '% продаж':
            output_df.loc[7, '% продаж'] = round(df[(df['target_class'].isin([5,6])) & \
                                                  (df['status_amo'].isin(payment_status))].shape[0] / \
                                                   df[df['target_class'].isin([5,6])].shape[0] * 100, 1)
            output_df.loc[8, '% продаж'] = round(df[df['status_amo'].isin(payment_status)].shape[0] / \
                                                   df.shape[0] * 100, 1)
        elif col == 'Цена лида':
            output_df.loc[7, 'Цена лида'] = round(df[df['target_class'].isin([5,6])]['channel_expense'].sum() /\
                                                 df[df['target_class'].isin([5,6])].shape[0], 1)
            output_df.loc[8, 'Цена лида'] = round(df['channel_expense'].sum() / df.shape[0], 1)

        elif col == 'Средний чек':
            output_df.loc[7, 'Средний чек'] = round(df[df['target_class'].isin([5,6])]['payment_amount'].sum() \
                          / df[(df['target_class'].isin([5,6])) & (df['payment_amount']!= 0)].shape[0], 1)\
                          if df[(df['target_class'].isin([5,6])) & (df['payment_amount']!= 0)].shape[0] != 0 else 0
            output_df.loc[8, 'Средний чек'] = round(df['payment_amount'].sum() \
                                                  / df[df['payment_amount']!= 0].shape[0], 1)\
                                                  if df[df['payment_amount']!= 0].shape[0] != 0 else 0


        elif col == '% расходов на трафик (доля от Расход общий)':
            output_df.loc[7, col] = round(float(output_df.loc[7, 'Бюджет'])/output_df.loc[7, 'Расход общий (ОП+бюджет)'] * 100, 1) if  output_df.loc[7, 'Расход общий (ОП+бюджет)'] != 0 else 0
            output_df.loc[8, col] = round(float(output_df.loc[8, 'Бюджет'])/output_df.loc[8, 'Расход общий (ОП+бюджет)'] * 100, 1)

        elif col == '% расходов на ОП (доля от Расход общий)':
            output_df.loc[7, col] = round(float(output_df.loc[7, 'Расход на ОП'])/output_df.loc[7, 'Расход общий (ОП+бюджет)'] * 100, 1)
            output_df.loc[8, col] = round(float(output_df.loc[8, 'Расход на ОП'])/output_df.loc[8, 'Расход общий (ОП+бюджет)'] * 100, 1)

        elif col in ['ROI ?', 'ROMI', 'ROSI', 'ROI']:
            output_df.loc[7, 'ROI ?'] = round((float(df[df['target_class'].isin([5,6])]['payment_amount'].sum())
                                        - float(df[df['target_class'].isin([5,6])]['channel_expense'].sum()))
                                        / float(df[df['target_class'].isin([5,6])]['channel_expense'].sum()) * 100, 1)\
                                        if df[df['target_class'].isin([5,6])]['channel_expense'].sum() != 0 else 0
            output_df.loc[7, 'ROMI'] = round((output_df['Оборот'][5:7].sum() / output_df['Бюджет'][5:7].sum()) - 1, 1)
            output_df.loc[7, 'ROSI'] = round((output_df['Оборот'][5:7].sum() / output_df['Расход на ОП'][5:7].sum()) - 1, 1)
            output_df.loc[7, 'ROI'] = round((output_df['Оборот'][5:7].sum() / output_df['Расход общий (ОП+бюджет)'][5:7].sum()) - 1, 1)

            output_df.loc[8, 'ROI ?'] = round((float(df['payment_amount'].sum())
                                        - float(df['channel_expense'].sum()))
                                        / float(df['channel_expense'].sum()) * 100, 1)\
                                        if df[df['target_class'].isin([5,6])]['channel_expense'].sum() != 0 else 0
            output_df.loc[8, 'ROMI'] = round((output_df['Оборот'][0:7].sum() / output_df['Бюджет'][0:7].sum()) - 1, 1)
            output_df.loc[8, 'ROSI'] = round((output_df['Оборот'][0:7].sum() / output_df['Расход на ОП'][0:7].sum()) - 1, 1)
            output_df.loc[8, 'ROI'] = round((output_df['Оборот'][0:7].sum() / output_df['Расход общий (ОП+бюджет)'][0:7].sum()) - 1, 1)

        elif col == 'Маржа':
            output_df.loc[7, 'Маржа %'] = float(output_df['Маржа'][5:7].sum()) \
                                          / float(output_df['Оборот'][5:7].sum()) \
                                          if output_df['Оборот'][5:7].sum() != 0 else 0
            output_df.loc[8, 'Маржа %'] = float(output_df['Маржа'][:7].sum()) \
                                          / float(output_df['Оборот'][:7].sum()) \
                                          if output_df['Оборот'][:7].sum() != 0 else 0

    output_df.rename(index={7: '5-6'}, inplace = True)
    output_df.rename(index={8: 'Sum'}, inplace=True)
    # print(df[df['payment_amount']])
    return {'Статистика лиды': output_df}

# if __name__=='__main__':
#     data = pd.read_csv(os.path.join(RESULTS_FOLDER, 'leads.csv'))
#     result = calculate_leads_ta_stats(data)
