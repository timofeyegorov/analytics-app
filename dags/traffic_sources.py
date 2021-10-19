from database import get_accounts, get_target_audience, get_leads_data

import pandas as pd
from urllib import parse
import pickle as pkl


def get_traffic_sources(df):
    data = df

    status_df = pd.read_csv('data/status.csv')
    in_process = status_df[~status_df['В работе'].isna()]['Статус'].unique().tolist()
    reached = status_df[~status_df['Дозвон'].isna()]['Статус'].unique().tolist()
    appointment = status_df[~status_df['Назначен zoom'].isna()]['Статус'].unique().tolist()
    any_status = status_df.dropna(thresh=2).dropna(how='all')['Статус'].unique().tolist()

    target_audience = get_target_audience()
    traff_data = get_accounts()

    def parse_label(url):
        query = parse.parse_qs(parse.urlparse(url).query)
        if 'rs' in query.keys():
            query['rs'][0] = query['rs'][0].split('_')[0]
        if 'utm_source' in query.keys():

            if query['utm_source'][0] in traff_data['label'].values:
                return traff_data[traff_data['label'] == query['utm_source'][0]].values[0][1]
            elif 'rs' in query.keys():
                if query['rs'][0] in traff_data['label'].values:
                    return traff_data[traff_data['label'] == query['rs'][0]].values[0][1]
                else:
                    return query['utm_source'][0]
            else:
                return query['utm_source'][0]
        elif 'rs' in query.keys():
            if query['rs'][0] in traff_data['label'].values:
                return traff_data[traff_data['label'] == query['rs'][0]].values[0][1]
            else:
                return query['rs'][0]
        return 'Not found'

    def parse_label2(url):
        args = parse.parse_qs(parse.urlparse(url).query)
        label = None
        if 'rs' in args:
            label = args['rs'][0].split('_')[0] # facebook17_23848555310470208_23849132838180208_23849132838200208 (args['rs'])
        elif 'amp;rs' in args:
            label = args['amp;rs'][0].split('_')[0] 
        elif 'utm_source' in args:
            label = 'utm_source=' + args['utm_source'][0]
        elif 'amp;utm_source' in args:
            label = 'utm_source' + args['utm_source'][0]
        if label in traff_data.label.values:
            return label
        return 'Not found'


    data = data.assign(
        label = data.traffic_channel.apply(parse_label)
    )

    data = data.assign(
    target_audience = data.quiz_answers1.isin(target_audience).astype(int) + \
                      data.quiz_answers2.isin(target_audience) + \
                      data.quiz_answers3.isin(target_audience) + \
                      data.quiz_answers4.isin(target_audience) + \
                      data.quiz_answers5.isin(target_audience) + \
                      data.quiz_answers6.isin(target_audience)
    )
    target_audience_table = data.pivot_table(index='id', values='traffic_channel', columns='target_audience', aggfunc='count').reset_index()
    target_audience_table = target_audience_table.fillna(0)
    for i in range(0, 7):
        if i not in target_audience_table:
            target_audience_table.loc[:, str(i)] = 0
        target_audience_table = target_audience_table.rename(
            columns={ str(i): 'target_audience_' + str(i),
                    i: 'target_audience_' + str(i)}
        )

    data = data.merge(target_audience_table, on='id', how='inner')
    data = traff_data.drop(columns='id').merge(data, on='label', how='right')
    data['title'] = data['title'].fillna('Not found')
    data['name'] = data['name'].fillna('Not found')

    zoom_channel_expense = data.channel_expense.copy()
    zoom_channel_expense[data.status_amo.str.contains('zoom', na=False)] = 0

    zoom_payment_amount = data.payment_amount.copy()
    zoom_payment_amount[data.status_amo.str.contains('zoom', na=False)] = 0

    data = data.assign(
        sell_percent = data.payment_amount.copy(),
        mean_receipt = data.payment_amount.copy(),
        sell_count = data.payment_amount.copy(),
        cv_receipt = data.payment_amount.copy(),
        done_zoom = data.status_amo.copy(),
        zoom_channel_expense = zoom_channel_expense.copy(),
        zoom_payment_amount = zoom_payment_amount.copy(),
        zoom_price = zoom_channel_expense.copy()
    )

    result = data.groupby(['name', 'title', 'label'], as_index=False).agg({
        'payment_amount': 'sum',
        'sell_percent': lambda x: (x != 0).sum() / len(x),
        'sell_count': lambda x: (x != 0).sum(), 
        'done_zoom': lambda x: x.str.contains('zoom', na=False).sum(),
        # 'zoom_payment_amount': 'mean',
        # 'zoom_channel_expense': 'mean',
        # 'zoom_price': lam
        'cv_receipt': lambda x: (x != 0).sum() / len(data),
        # 'mean_receipt': 'mean',
        'channel_expense': 'sum',
        'traffic_channel': 'count',
        'is_double': lambda x: (x == 'yes').sum(),
        'status_amo': list,
        'quiz_answers1': lambda x: x.isin(target_audience).sum(),
        'quiz_answers2': lambda x: x.isin(target_audience).sum(),
        'quiz_answers3': lambda x: x.isin(target_audience).sum(),
        'quiz_answers4': lambda x: x.isin(target_audience).sum(),
        'quiz_answers5': lambda x: x.isin(target_audience).sum(),
        'quiz_answers6': lambda x: x.isin(target_audience).sum(),
        'target_audience_0': 'sum',
        'target_audience_1': 'sum',
        'target_audience_2': 'sum',
        'target_audience_3': 'sum',
        'target_audience_4': 'sum',
        'target_audience_5': 'sum',
        'target_audience_6': 'sum',
    }).rename(columns={
        'payment_amount': 'turnover',
        'channel_expense': 'budget',
        'traffic_channel': 'lead_number',
        'is_double': 'reoccurences',
    })

    result = result.assign(
        reoccurences_percentage = result.reoccurences / result.lead_number * 100,
        in_process = result.status_amo.apply(lambda x: len([el for el in x if x in in_process])) / result.lead_number * 100,
        reached = result.status_amo.apply(lambda x: len([el for el in x if x in reached])) / result.lead_number * 100,
        appointment = result.status_amo.apply(lambda x: len([el for el in x if x in appointment])) / result.lead_number * 100,
        not_reached = result.status_amo.apply(lambda x: len([el for el in x if x not in any_status])) / result.lead_number * 100,
        target_audience_5_6 = result.target_audience_5 + result.target_audience_6,
        target_audience_4_5_6 = result.target_audience_4 + result.target_audience_5 + result.target_audience_6,
        zoom_turnover = result.done_zoom * 340 + result.done_zoom * 268 + result.sell_count * 3584,
    )
    result = result.assign(
        price_target_audience_4_5_6 = result.budget.astype(float) / result.target_audience_4_5_6,
        price_target_audience_5_6 = result.budget.astype(float) / result.target_audience_5_6,
        # romi = (result.turnover - result.budget) / (result.budget + result.zoom_turnover) * 100,
        margin = result.turnover - result.budget,
    )

    result = result.drop(columns=['quiz_answers1', 'quiz_answers2', 'quiz_answers3', 'quiz_answers4', 'quiz_answers5', 'quiz_answers6', 'status_amo'])

    return {'traff_table': result.rename(columns={
        'label': 'Метка',
        'name': 'Имя',
        'title': 'Название',
        
        'sell_count': 'Кол-во продаж',
        'done_zoom': 'Проведено zoom',

        'target_audience_0': 'Не ЦА',
        'target_audience_1': 'ЦА-1',
        'target_audience_2': 'ЦА-2',
        'target_audience_3': 'ЦА-3',
        'target_audience_4': 'ЦА-4',
        'target_audience_5': 'ЦА-5',
        'target_audience_6': 'ЦА-6',
        'target_audience_5_6': 'ЦА-5-6',
        'target_audience_4_5_6': 'ЦА-4-5-6',
        'price_target_audience_5_6': 'Цена ЦА 5-6',
        'price_target_audience_4_5_6': 'Цена ЦА 4-5-6',

        'budget': 'Бюджет',
        'reoccurences': 'Кол-во дублей',
        'reoccurences_percentage': '% дублей',
        'sell_percent': '% продаж',

        'lead_number': 'Кол-во лидов',
        'turnover': 'Оборот',
        'romi': 'ROMI',
        'cv_receipt': 'CV счет',

        'in_process': '% В обработке',
        'reached': '% Дозвон',
        'appointment': '% Назначен zoom',
        'not_reached': '% Недозвон',
        'zoom_turnover': 'Цена проведенного zoom',

        'margin': 'Маржа'
    })}

data = get_leads_data()
print('get data')
traffic_sources = get_traffic_sources(data)
print('calculated traffic_sources')
with open('results/traffic_sources.pkl', 'wb') as f:
  pkl.dump(traffic_sources, f)
