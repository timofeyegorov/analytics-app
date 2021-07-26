from app.database import get_accounts

import pandas as pd
from urllib import parse

def get_segments_stats(df):
    result = dict()
    traficologist_data = get_accounts()

    def utm_replace(line):
        return line.replace('utm_source=', '')

    df_drop = df.drop(df.columns[[0, 10, 11, 12, 18, 19, 21, 22]], axis=1)

    def url2acc(df):
        # print(list(df.index))
        # for i, v in zip(df.index, df):
        #   if i == 'utm_source':
        #     print(i, type(v))
        query = parse.parse_qs(parse.urlparse(df.values[0]).query)
        if 'rs' in query.keys():
            query['rs'][0] = query['rs'][0].split('_')[0]
        if 'utm_source' in query.keys():

            if query['utm_source'][0] in traficologist_data['label'].values:
                df['Трафиколог'] = traficologist_data[traficologist_data['label'] == query['utm_source'][0]].values[0][
                    2]
                df['Кабинет'] = traficologist_data[traficologist_data['label'] == query['utm_source'][0]].values[0][1]
            elif 'rs' in query.keys():
                if query['rs'][0] in traficologist_data['label'].values:
                    df['Трафиколог'] = traficologist_data[traficologist_data['label'] == query['rs'][0]].values[0][2]
                    df['Кабинет'] = traficologist_data[traficologist_data['label'] == query['rs'][0]].values[0][1]
                else:
                    df['Трафиколог'] = query['utm_source'][0]
                    df['Кабинет'] = query['utm_source'][0]
            else:
                df['Трафиколог'] = query['utm_source'][0]
                df['Кабинет'] = query['utm_source'][0]
        elif 'rs' in query.keys():
            if query['rs'][0] in traficologist_data['label'].values:
                df['Трафиколог'] = traficologist_data[traficologist_data['label'] == query['rs'][0]].values[0][2]
                df['Кабинет'] = traficologist_data[traficologist_data['label'] == query['rs'][0]].values[0][1]
            else:
                df['Трафиколог'] = query['rs'][0]
                df['Кабинет'] = query['rs'][0]
        else:
            df['Трафиколог'] = 'Not found'
            df['Кабинет'] = 'Not found'
        return df

    df_drop = df_drop.apply(url2acc, axis=1)
    # df_drop['status_amo'] = df_drop['status_amo'].apply(str.lower)

    traff_table = df_drop.groupby(['Трафиколог', 'Кабинет']).agg(
        {'payment_amount': 'sum', 'channel_expense': 'sum', 'quiz_answers1': 'count'}).reset_index()
    traff_table = traff_table.rename(columns={traff_table.columns[2]: 'Оборот', traff_table.columns[3]: 'Бюджет',
                                              traff_table.columns[4]: 'Количество лидов'})

    for traff in traff_table['Трафиколог'].unique():
        temp = traff_table[traff_table['Трафиколог'] == traff]
        if temp.shape[0] != 1:
            temp = temp.groupby(['Трафиколог']).agg(
                {'Кабинет': 'count', 'Оборот': 'sum', 'Бюджет': 'sum', 'Количество лидов': 'sum'}).reset_index()
            traff_table = pd.concat([traff_table, temp])

    def count2traff(df):
        if type(df['Кабинет']) is int:
            df['Кабинет'] = df['Трафиколог']
        return df

    traff_table = traff_table.apply(count2traff, axis=1).sort_values(by='Кабинет')

    status_df = pd.read_csv('status.csv')

    ca = [['Россия', 'СНГ'], 
       ['24 - 30'], 
       ['Связано с числами'], 
       ['до 100 000 руб'], 
       ['Да, проект'], 
       ['до 10 часов в неделю']]

    qa = []
    for qestion in range(1, 7):

        def dupl_count(df):
            count = []
            per = []
            # global qestion
            filt_data = df_drop[df_drop[df_drop.columns[qestion]] == df.values[0]]
            #     df['Доля лидов'] = round(traff_table[traff_table['Трафиколог'] == df.values[0]]['Количество лидов'].sum() / sum(traff_table['Количество лидов'])*100, 2)
            dif = round(filt_data[filt_data['is_double'] == 'yes'].shape[0] / filt_data.shape[0] * 100, 2)
            df['Количество дублей'] = filt_data[filt_data['is_double'] == 'yes'].shape[0]
            df['% дублей'] = round(dif, 2)
            df['% в работе'] = round(filt_data[filt_data['status_amo'].isin(status_df[~status_df['В работе'].isna()]['Статус'].unique().tolist())].shape[0] / filt_data.shape[0] * 100, 2)
            df['% дозвонов'] = round(filt_data[filt_data['status_amo'].isin(status_df[~status_df['Дозвон'].isna()]['Статус'].unique().tolist())].shape[0] / filt_data.shape[0] * 100, 2)
            df['% назначеных'] = round(filt_data[filt_data['status_amo'].isin(status_df[~status_df['Назначен zoom'].isna()]['Статус'].unique().tolist())].shape[0] / filt_data.shape[0] * 100, 2)
            # df['% недозвон'] = round(filt_data[filt_data['status_amo'].isin(status_df[~status_df['Назначен zoom'].isna()]['Статус'].unique().tolist())].shape[0] / filt_data.shape[0] * 100, 2)
            df['% недозвон'] = round(filt_data[~filt_data['status_amo'].isin(status_df.dropna(thresh=2).dropna(how='all')['Статус'].unique().tolist())].shape[0] / filt_data.shape[0] * 100, 2)
            for i, target in enumerate(df_drop.columns[1:7]):
                df[f'ЦА {i + 1}'] = 0
            df[f'ЦА 4-5-6'] = 0
            df[f'ЦА 5-6'] = 0
            df[f'не ЦА'] = 0
            for line in filt_data.values:
                count = 0
                for i in range(len(ca)):
                    if line[i + 1] in ca[i]:
                        count += 1
                if count == 0:
                    df[f'не ЦА'] += 1
                else:
                    df[f'ЦА {count}'] += 1
                    if count >= 5:
                        df[f'ЦА 5-6'] += 1
                    if count >= 4:
                        df[f'ЦА 4-5-6'] += 1

            df[f'Цена ЦА 4-5-6'] = 0 if df[f'ЦА 4-5-6'] == 0 else df['Бюджет'] / df[f'ЦА 4-5-6']
            df[f'Цена ЦА 5-6'] = 0 if df[f'ЦА 5-6'] == 0 else df['Бюджет'] / df[f'ЦА 5-6']
            df['% продаж'] = round(
                filt_data[filt_data['payment_amount'] != 0].shape[0] / filt_data['payment_amount'].shape[0] * 100, 2)
            df['Средний чек'] = round(filt_data['payment_amount'].mean(), 2)
            done_zoom = filt_data[filt_data['status_amo'].str.contains('zoom')].shape[0]
            count_pay = filt_data[filt_data['payment_amount'] != 0].shape[0]
            expenses = filt_data[filt_data['status_amo'].str.contains('zoom')]['channel_expense'].sum()
            turnover = filt_data[filt_data['status_amo'].str.contains('zoom')]['payment_amount'].sum()
            df['Расход на ОП'] = done_zoom * 340 + done_zoom * 268 + count_pay * 3584
            # df['ROI'] = round((df['Оборот'] - df['Расход на ОП']) / (df['Расход на ОП']) * 100, 2)
            df['ROMI'] = round(
                (df['Оборот'] - df['Бюджет'] - df['Расход на ОП']) / (df['Бюджет'] + df['Расход на ОП']) * 100, 2)
            df['Маржа'] = df['Оборот'] - df['Бюджет']
            df['Цена проведенного ZOOM'] = round(0 if done_zoom == 0 else expenses / done_zoom, 2)
            df['Оборот на ZOOM'] = round(0 if done_zoom == 0 else turnover / done_zoom, 2)
            df['CV счет'] = round(filt_data[filt_data['payment_amount'] != 0].shape[0] / df_drop.shape[0], 2)
            # df['CPO'] = round(filt_data['payment_amount'].mean() / filt_data[filt_data['payment_amount'] != 0].shape[0],
            #                   2)
            df['Доля канала в бюджете'] = round(df['Бюджет'] / filt_data['channel_expense'].sum(), 2)
            df['Доля канала в обороте'] = round(df['Оборот'] / filt_data['payment_amount'].sum(), 2) if filt_data['payment_amount'].sum() != 0 else 0
            df['Оборот на лида/анкету'] = round(df['Оборот'] / df['Количество лидов'], 2)
            df['% лидов/анкет (доля от общего)'] = round(
                traff_table[traff_table['Трафиколог'] == df.values[0]]['Количество лидов'].sum() / sum(
                    traff_table['Количество лидов']) * 100, 2)
            df['% Оборот  (доля от общего)'] = round(df['Оборот'] / df_drop['payment_amount'].sum(), 2)
            df['% Оплат (доля от общего)'] = round(df['Бюджет'] / df_drop['channel_expense'].sum(), 2)
            df['% Затрат (доля от общего)'] = round(
                (df['Бюджет'] + df['Расход на ОП']) / df_drop['channel_expense'].sum(), 2)
            df['Расход общий (ОП+бюджет)'] = df['Бюджет'] + df['Расход на ОП']
            df['% расходов на трафик (доля от Расход общий)'] = round(
                df['Бюджет'] / (df['Бюджет'] + df['Расход на ОП']), 2)
            df['% расходов на ОП (доля от Расход общий)'] = round(
                df['Расход на ОП'] / (df['Бюджет'] + df['Расход на ОП']), 2)

            return df

        qa1 = df_drop.groupby(df_drop.columns[qestion]).agg(
            {'payment_amount': 'sum', 'channel_expense': 'sum', 'status_amo': 'count'}).reset_index()
        qa1 = qa1.rename(
            columns={qa1.columns[1]: 'Оборот', qa1.columns[2]: 'Бюджет', qa1.columns[3]: 'Количество лидов'})
        qa1_result = qa1.apply(dupl_count, axis=1)
        qa.append(qa1_result)
        result[f'question{qestion}'] = qa1_result
    #     qa1_result.to_excel(f'qa{i+1}.xlsx')
    # qa[0].sort_values(qa[0].columns[0], ascending=True, inplace=True, key=(lambda x: (x.isin(ca[0]), x.str)))
    # traff_table1.to_excel('analitic.xlsx')

    ca_stat = qa[0].head(0).copy()
    ca_stat = ca_stat.rename(columns={ca_stat.columns[0]: 'ЦА'})
    for i in range(len(qa)):
        temp = qa[i][qa[i][qa[i].columns[0]].isin(ca[i])].copy()
        temp = temp.rename(columns={temp.columns[0]: 'ЦА'})
        ca_stat = pd.concat([ca_stat, temp], sort=False, axis=0)
    result['Статистика по ЦА'] = ca_stat

    qa_stat = qa[0].head(0).copy()
    qa_stat = qa_stat.rename(columns={qa_stat.columns[0]: 'ЦА'})
    col = ['Страна', 'Возраст', 'Вид деятельности', 'Траты на учебу', 'Цель обучения',
           'Количество часов на обучение в неделю']
    for i in range(len(qa)):
        temp = qa[i].copy()
        temp[temp.columns[0]] = col[i]
        temp = temp.groupby(temp.columns[0]).sum().reset_index()
        temp = temp.rename(columns={temp.columns[0]: 'ЦА'})
        qa_stat = pd.concat([qa_stat, temp], sort=False, axis=0)
    result['Статистика по вопросам'] = qa_stat

    return result
