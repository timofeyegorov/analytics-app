from database import get_accounts, get_leads_data, get_target_audience

import pandas as pd
from urllib import parse
import pickle as pkl
import numpy as np

def get_segments_stats(df):
    result = dict()
    traficologist_data = get_accounts()

    def utm_replace(line):
        return line.replace('utm_source=', '')

    df_drop = df.drop(df.columns[[0, 10, 11, 12, 18, 19, 22]], axis=1)

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

    status_df = pd.read_csv('data/status.csv')

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
            done_zoom = filt_data[filt_data['status_amo'].str.lower().str.contains('zoom', na=False)].shape[0]
            count_pay = filt_data[filt_data['payment_amount'] != 0].shape[0]
            expenses = filt_data[filt_data['status_amo'].str.lower().str.contains('zoom', na=False)]['channel_expense'].sum()
            turnover = filt_data[filt_data['status_amo'].str.lower().str.contains('zoom', na=False)]['payment_amount'].sum()
            df['Расход на ОП'] = done_zoom * 340 + done_zoom * 268 + count_pay * 3584
            # df['ROI'] = round((df['Оборот'] - df['Расход на ОП']) / (df['Расход на ОП']) * 100, 2)
            df['ROMI'] = float('nan') if (df['Бюджет'] + df['Расход на ОП']) == 0 else round(
                (df['Оборот'] - df['Бюджет'] - df['Расход на ОП']) / (df['Бюджет'] + df['Расход на ОП']) * 100, 2)
            df['Маржа'] = df['Оборот'] - df['Бюджет']
            df['Цена проведенного ZOOM'] = round(0 if done_zoom == 0 else expenses / done_zoom, 2)
            df['Оборот на ZOOM'] = round(0 if done_zoom == 0 else turnover / done_zoom, 2)
            df['CV счет'] = round(filt_data[filt_data['payment_amount'] != 0].shape[0] / df_drop.shape[0], 2)
            # df['CPO'] = round(filt_data['payment_amount'].mean() / filt_data[filt_data['payment_amount'] != 0].shape[0],
            #                   2)
            df['Доля канала в бюджете'] = round(0 if filt_data['channel_expense'].sum() == 0 else df['Бюджет'] / filt_data['channel_expense'].sum(), 2)
            df['Доля канала в обороте'] = round(df['Оборот'] / filt_data['payment_amount'].sum(), 2) if filt_data['payment_amount'].sum() != 0 else 0
            df['Оборот на лида/анкету'] = round(df['Оборот'] / df['Количество лидов'], 2)
            df['% лидов/анкет (доля от общего)'] = round(
                traff_table[traff_table['Трафиколог'] == df.values[0]]['Количество лидов'].sum() / sum(
                    traff_table['Количество лидов']) * 100, 2)
            df['% Оборот  (доля от общего)'] = round(0 if df_drop['payment_amount'].sum() == 0 else df['Оборот'] / df_drop['payment_amount'].sum(), 2)
            df['% Оплат (доля от общего)'] = round(0 if df_drop['channel_expense'].sum() == 0 else df['Бюджет'] / df_drop['channel_expense'].sum(), 2)
            df['% Затрат (доля от общего)'] = round(0 if df_drop['channel_expense'].sum() == 0 else 
                (df['Бюджет'] + df['Расход на ОП']) / df_drop['channel_expense'].sum(), 2)
            df['Расход общий (ОП+бюджет)'] = df['Бюджет'] + df['Расход на ОП']
            df['% расходов на трафик (доля от Расход общий)'] = 0 if (df['Бюджет'] + df['Расход на ОП']) == 0 else round(
                df['Бюджет'] / (df['Бюджет'] + df['Расход на ОП']), 2)
            df['% расходов на ОП (доля от Расход общий)'] = 0 if (df['Бюджет'] + df['Расход на ОП']) == 0 else round(
                df['Расход на ОП'] / (df['Бюджет'] + df['Расход на ОП']), 2)

            return df

        qa1 = df_drop.groupby(df_drop.columns[qestion]).agg(
            {'payment_amount': 'sum', 'channel_expense': 'sum', 'status_amo': 'count'}).reset_index()
        qa1 = qa1.rename(
            columns={qa1.columns[1]: 'Оборот', qa1.columns[2]: 'Бюджет', qa1.columns[3]: 'Количество лидов'})
        qa1_result = qa1.apply(dupl_count, axis=1)
        qa.append(qa1_result)
        
        d = {
            1: 'Страна',
            2: 'Возраст',
            3: 'Профессия',
            4: 'Доход',
            5: 'Обучение',
            6: 'Время',
        }

        result[d[qestion]] = qa1_result

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
    result['Статистика по вопросам'] = qa_stat.reset_index().drop(columns=['index'])

    return result

def get_segments_stats(df):

  target_audience = get_target_audience()

  status = pd.read_csv('data/status.csv')
  status.fillna(0, inplace=True)

  payment_status = status[status['Оплатил'] == ' +']['Соответствующий статус в воронке Теплые продажи'].unique().tolist() # Статусы с этапом "оплатил"
  was_conversation_status = status[status['Был разговор'] == ' +']['Соответствующий статус в воронке Теплые продажи'].unique().tolist() # Статусы с этапом "был разговор"
  in_progress_status = status[status['В работе'] == ' +']['Соответствующий статус в воронке Теплые продажи'].unique().tolist() # Статусы с этапом "в работе"


  try:
    df.insert(19, 'trafficologist', 'Неизвестно')                # Добавляем столбец trafficologist для записи имени трафиколога
    df.insert(20, 'account', 'Неизвестно 1')                       # Добавляем столбец account для записи аккаунта трафиколога
    df.insert(21, 'target_class', 0)  
  except ValueError:
    pass

  for i in range(df.shape[0]):
    target_class = 0
    if df.loc[i, 'quiz_answers1'] in target_audience:
      target_class += 1
    if df.loc[i, 'quiz_answers2'] in target_audience:
      target_class += 1
    if df.loc[i, 'quiz_answers3'] in target_audience:
      target_class += 1
    if df.loc[i, 'quiz_answers4'] in target_audience:
      target_class += 1         
    if df.loc[i, 'quiz_answers5'] in target_audience:
      target_class += 1
    if df.loc[i, 'quiz_answers6'] in target_audience:
      target_class += 1

    df.loc[i, 'target_class'] = target_class

  columns = ['Бюджет', 'Кол-во лидов',
             '% в работе', '% дозвонов', '% офферов', '% счетов',
             'Цена лида',
             '% ЦА 5-6', '% ЦА 5', '% ЦА 6', '% ЦА 4-5-6', '% ЦА 4',	'% не ЦА', '% ЦА 3', '% ЦА 2', '% ЦА 1', 'Цена ЦА 5-6', 'Цена ЦА 4-5-6',
             '% продаж', 'Средний чек', 'Оборот', 'Расход на ОП', 'Расход общий (ОП+бюджет)', '% расходов на трафик (доля от Расход общий)', '% расходов на ОП (доля от Расход общий)',
             'ROI', 'Маржа', 'Цена Разговора', 'Цена Оффера', 'Цена Счета',
             'Оборот на лида (на обработанного лида)', 'Оборот на разговор', 'CV обр.лид/оплата', 'CV разговор/оплата', 'CPO',
             '% лидов (доля от общего)', '% Оборот (доля от общего)', '% Оплат (доля от общего)', '% Затрат (доля от общего)']

  # Функция формирования двух датасетов с количеством лидов и процентным соотношением
  # по выбранной категории с разбивкой по трафикологам и их кабинетам

  def channels_table(df, column_name):
    '''
      Функция формирования двух датасетов по выбранной категории
      Вход:
          df - обрабатываемый датасет
          category - список подкатегорий по выбранной категории
          column_name - название колонки в датасете, которая содержит названия подкатегорий
      Выход:
          df_category - датасет с разбивкой заявок трафикологов по подкатегориям в %
          df_category_val - датасет с разбивкой заявок трафикологов по подкатегориям в штуках
    '''
    # Создаем заготовки результирующих датасетов из нулей
    data_category = np.zeros((len(df[column_name].unique())+1, len(columns))).astype('int')
    df_category = pd.DataFrame(data_category, columns=columns) # Датасет для процентного кол-ва лидов
    df_category.insert(0, column_name, 0) # Столбец названия подкатегорий указанной категории


    # Проходим в цикле по каждой подкатегории выбранной категории
    # Формируем список для прохода сначала по подкатегориям куда попадает ЦА
    if (column_name == df.columns[3]):
      subcategories = list(df[column_name].unique())
      subcategories.sort()
      try:
        subcategories.pop(subcategories.index('до 18 лет'))
        subcategories.insert(0, 'до 18 лет')
      except ValueError:
        pass
    elif (column_name == df.columns[5]):
      temp_list = ['0 руб.', 'до 30 000 руб.', 'до 60 000 руб.', 'до 100 000 руб.', 'более 100 000 руб.']
      subcategories = temp_list\
            + df[~df[column_name].isin(temp_list)][column_name].unique().tolist()
      subcategories_temp = subcategories.copy()
      for el in subcategories_temp:
        if el not in df[column_name].unique():
          subcategories.remove(el)
    elif (column_name == df.columns[7]):
      temp_list = ['до 5 часов в неделю', 'до 10 часов в неделю', 'более 10 часов в неделю']
      subcategories = temp_list\
            + df[~df[column_name].isin(temp_list)][column_name].unique().tolist()
      subcategories_temp = subcategories.copy()
      for el in subcategories_temp:
        if el not in df[column_name].unique():
          subcategories.remove(el)
    else:
      subcategories = df[df[column_name].isin(target_audience)][column_name].unique().tolist()\
                  + df[~df[column_name].isin(target_audience)][column_name].unique().tolist()



    for i, subcategory_name in enumerate(subcategories):
      temp_df = df[df[column_name] == subcategory_name]
      if temp_df.shape[0] != 0:

        df_category.loc[i, column_name] = subcategory_name
        df_category.loc[i, 'Бюджет'] = temp_df['channel_expense'].sum()
        df_category.loc[i, 'Кол-во лидов'] = temp_df.shape[0]
        df_category.loc[i, '% в работе'] = round(temp_df[temp_df['status_amo'].isin(in_progress_status)].shape[0] / temp_df.shape[0] * 100, 1)
        df_category.loc[i, '% дозвонов'] = round(temp_df[temp_df['status_amo'].isin(was_conversation_status)].shape[0] / temp_df.shape[0] * 100, 1)
        df_category.loc[i, '% офферов'] = round(temp_df[temp_df['status_amo'] == 'Сделан оффер из Теплые продажи'].shape[0] / temp_df.shape[0] * 100, 1)
        df_category.loc[i, '% счетов'] = round(temp_df[temp_df['status_amo'] == 'Выставлен счет из Теплые продажи'].shape[0] / temp_df.shape[0] * 100, 1)
        df_category.loc[i, 'Цена лида'] = round(temp_df[temp_df['channel_expense'] != 0]['channel_expense'].sum() / temp_df.shape[0], 1)

        df_category.loc[i, '% ЦА 5-6'] = round(temp_df[temp_df['target_class'].isin([5,6])].shape[0] / temp_df.shape[0] * 100, 2)
        df_category.loc[i, '% ЦА 5'] = round(temp_df[temp_df['target_class'] == 5].shape[0] / temp_df.shape[0] * 100, 2)
        df_category.loc[i, '% ЦА 6'] = round(temp_df[temp_df['target_class'] == 6].shape[0] / temp_df.shape[0] * 100, 2)
        df_category.loc[i, '% ЦА 4-5-6'] = round(temp_df[temp_df['target_class'].isin([4, 5,6])].shape[0] / temp_df.shape[0] * 100, 2)
        df_category.loc[i, '% ЦА 4'] = round(temp_df[temp_df['target_class'] == 4].shape[0] / temp_df.shape[0] * 100, 2)
        df_category.loc[i, '% не ЦА'] = round(temp_df[temp_df['target_class'] == 0].shape[0] / temp_df.shape[0] * 100, 2)
        df_category.loc[i, '% ЦА 3'] = round(temp_df[temp_df['target_class'] == 3].shape[0] / temp_df.shape[0] * 100, 2)
        df_category.loc[i, '% ЦА 2'] = round(temp_df[temp_df['target_class'] == 2].shape[0] / temp_df.shape[0] * 100, 2)
        df_category.loc[i, '% ЦА 1'] = round(temp_df[temp_df['target_class'] == 1].shape[0] / temp_df.shape[0] * 100, 2)
        df_category.loc[i, 'Цена ЦА 5-6'] = temp_df[temp_df['target_class'].isin([5,6])]['channel_expense'].sum() / temp_df[temp_df['target_class'].isin([5,6])].shape[0] if temp_df[temp_df['target_class'].isin([5,6])].shape[0] != 0 else float('nan')
        df_category.loc[i, 'Цена ЦА 4-5-6'] = temp_df[temp_df['target_class'].isin([4,5,6])]['channel_expense'].sum() / temp_df[temp_df['target_class'].isin([4,5,6])].shape[0] if temp_df[temp_df['target_class'].isin([4,5,6])].shape[0] != 0 else float('nan')

        df_category.loc[i, '% продаж'] = round(temp_df[temp_df['status_amo'].isin(payment_status)].shape[0] / temp_df.shape[0] * 100, 1)
        df_category.loc[i, 'Средний чек'] = temp_df['payment_amount'].sum() / temp_df[temp_df['payment_amount'] != 0].shape[0] if temp_df[temp_df['payment_amount'] != 0].shape[0] != 0 else 0
        df_category.loc[i, 'Оборот'] = temp_df['payment_amount'].sum()
        df_category.loc[i, 'Расход на ОП'] = (float(temp_df['payment_amount'].sum()) * 0.6 + 150 * float(temp_df[temp_df['payment_amount'] != 0].shape[0]) + 27 * float(temp_df[temp_df['payment_amount'] != 0].shape[0]) + 20 * float(temp_df[temp_df['payment_amount'] != 0].shape[0])) * 1.4
        df_category.loc[i, 'Расход общий (ОП+бюджет)'] = float(df_category.loc[i, 'Бюджет']) + float(df_category.loc[i, 'Расход на ОП'])
        df_category.loc[i, '% расходов на трафик (доля от Расход общий)'] = round(float(df_category.loc[i, 'Бюджет'])/float(df_category.loc[i, 'Расход общий (ОП+бюджет)'] * 100), 2) if float(df_category.loc[i, 'Расход общий (ОП+бюджет)']) != 0 else float('nan')
        df_category.loc[i, '% расходов на ОП (доля от Расход общий)'] = round(df_category.loc[i, 'Расход на ОП']/df_category.loc[i, 'Расход общий (ОП+бюджет)'] * 100, 2)
        df_category.loc[i, 'ROI'] = round((temp_df['payment_amount'].sum() - temp_df['channel_expense'].sum()) / temp_df['channel_expense'].sum() * 100, 2) if temp_df['channel_expense'].sum() != 0 else 0
        df_category.loc[i, 'Маржа'] = temp_df['payment_amount'].sum() - temp_df['channel_expense'].sum()
        df_category.loc[i, 'Цена Разговора'] = temp_df['channel_expense'].sum() / temp_df[temp_df['status_amo'].isin(was_conversation_status)].shape[0] \
                                           if temp_df[temp_df['status_amo'].isin(was_conversation_status)].shape[0] != 0 else 0 
        df_category.loc[i, 'Цена Оффера'] = temp_df['channel_expense'].sum() / temp_df[temp_df['status_amo'] == 'Сделан оффер из Теплые продажи'].shape[0] \
                                        if temp_df[temp_df['channel_expense'] == 'Сделан оффер из Теплые продажи'].shape[0] != 0 else 0 
        df_category.loc[i, 'Цена Счета'] = temp_df['channel_expense'].sum() / temp_df[temp_df['status_amo'] == 'Выставлен счет из Теплые продажи'].shape[0] \
                                       if temp_df[temp_df['status_amo'] == 'Выставлен счет из Теплые продажи'].shape[0] != 0 else 0


        df_category.loc[i, 'Оборот на лида (на обработанного лида)'] = float(df_category.loc[i, 'Оборот']) / float(temp_df[temp_df['status_amo'] == 'Обработанная заявка из Теплые продажи'].shape[0]) if float(temp_df[temp_df['status_amo'] == 'Обработанная заявка из Теплые продажи'].shape[0]) != 0 else float('nan')
        df_category.loc[i, 'Оборот на разговор'] = float(df_category.loc[i, 'Оборот']) / float(temp_df[temp_df['status_amo'].isin(was_conversation_status)].shape[0]) if float(temp_df[temp_df['status_amo'].isin(was_conversation_status)].shape[0]) != 0 else float('nan')
        df_category.loc[i, 'CV обр.лид/оплата'] = temp_df[temp_df['status_amo'] == 'Обработанная заявка из Теплые продажи'].shape[0]/temp_df[temp_df['payment_amount'] != 0].shape[0]\
                                              if temp_df[temp_df['payment_amount'] != 0].shape[0] != 0 else 0
        df_category.loc[i, 'CV разговор/оплата'] = temp_df[temp_df['status_amo'].isin(was_conversation_status)].shape[0]/temp_df[temp_df['payment_amount'] != 0].shape[0]\
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

    return df_category

  df_countries = channels_table(df = df, column_name = df.columns[2])
  df_ages = channels_table(df = df, column_name = df.columns[3])
  df_jobs = channels_table(df = df, column_name = df.columns[4])
  df_earnings = channels_table(df = df, column_name = df.columns[5])
  df_trainings = channels_table(df = df, column_name = df.columns[6])
  df_times = channels_table(df = df, column_name = df.columns[7])

  return {'Страны, абсолютные значения': df_countries,
          'Возраст, абсолютные значения': df_ages,
          'Профессия, абсолютные значения': df_jobs,
          'Заработок, абсолютные значения': df_earnings,
          'Обучение, абсолютные значения': df_trainings,
          'Время, абсолютные значения': df_times}

