from database import get_target_audience, get_leads_data

import numpy as np
import pickle as pkl
import pandas as pd

def get_leads_ta_stats(df):
  status = pd.read_csv('data/status.csv')
  status.fillna(0, inplace=True)

  target_audience = get_target_audience()

  payment_status = status[status['Оплатил'] != 0]['Статус'].unique().tolist() # Статусы с этапом оплатил
  zoom_in_status = status[status['Проведен zoom'] != 0]['Статус'].unique().tolist() # Статусы с этапом проведен зум
  ring_up_status = status[status['Дозвон'] != 0]['Статус'].unique().tolist() # Статусы с этапом дозвон
  in_progress_status = status[status['В работе'] != 0]['Статус'].unique().tolist() # Статусы с этапом в работе
  zoom_assigned_status = status[status['Назначен zoom'] != 0]['Статус'].unique().tolist() # Статусы с этапом назначен зум

  df.insert(2, 'ca_num', 0)
  for i in range(df.shape[0]):
      if df.loc[i, 'quiz_answers1'] in target_audience:
          df.loc[i, 'ca_num'] += 1
      if df.loc[i, 'quiz_answers2'] in target_audience:
          df.loc[i, 'ca_num'] += 1
      if df.loc[i, 'quiz_answers3'] in target_audience:
          df.loc[i, 'ca_num'] += 1
      if df.loc[i, 'quiz_answers4'] in target_audience:
          df.loc[i, 'ca_num'] += 1
      if df.loc[i, 'quiz_answers5'] in target_audience:
          df.loc[i, 'ca_num'] += 1
      if df.loc[i, 'quiz_answers6'] in target_audience:
          df.loc[i, 'ca_num'] += 1

  columns = ['Бюджет', 'Кол-во лидов/анкет', 'Кол-во дублей', '% дублей', '% в работе', '% дозвонов',
            '% назначенных zoom', '% проведенных zoom', '% продаж', 'Средний чек', 'Оборот', 'Затраты на ОП',
            'ROMI', 'ROI', 'Маржа', 'Цена проведенного ZOOM', 'Оборот на ZOOM', 'CV в счет',
            'CPO', 'Доля лидов', 'Доля канала в бюджете', 'Доля канала в обороте', 'CV в продажу',
            'Оборот на лида/анкету', 'Средний чек', '% лидов/анкет (доля от общего)',
            '% Оборот (доля от общего)', '% Оплат (доля от общего)', '% Затрат (доля от общего)',
            'Расход на ОП', 'Расход общий (ОП+бюджет)', '% расходов на трафик (доля от Расход общий)',
            '% расходов на ОП (доля от Расход общий)']

  data = np.zeros((8, len(columns))).astype('int')
  lead_df = pd.DataFrame(data=data, columns=columns)

  for i in range(7):
      temp_df = df[df['ca_num'] == i]
      if temp_df.shape[0] != 0:
        done_zoom = temp_df[temp_df['status_amo'].isin(zoom_in_status)].shape[0]
        expenses = temp_df[temp_df['status_amo'].isin(zoom_in_status)]['channel_expense'].sum()
        turnover = temp_df[temp_df['status_amo'].isin(zoom_in_status)]['payment_amount'].sum()

        lead_df.loc[i, 'Анкет'] = temp_df.shape[0]
        lead_df.loc[i, 'Бюджет'] = temp_df['channel_expense'].sum()
        lead_df.loc[i, 'Кол-во лидов/анкет'] = round(temp_df.shape[0]/df.shape[0], 2)
        lead_df.loc[i, '% дублей'] = round(temp_df[temp_df['is_double'] == 'yes'].shape[0] / temp_df.shape[0]*100, 2)

        lead_df.loc[i, '% в работе'] = round(temp_df[temp_df['status_amo'].isin(in_progress_status)].shape[0] / temp_df.shape[0] * 100, 1)
        lead_df.loc[i, '% дозвонов'] = round(temp_df[temp_df['status_amo'].isin(ring_up_status)].shape[0] / temp_df.shape[0] * 100, 1)
        lead_df.loc[i, '% назначенных zoom'] = round(temp_df[temp_df['status_amo'].isin(zoom_assigned_status)].shape[0] / temp_df.shape[0] * 100, 1)
        lead_df.loc[i, '% проведенных zoom'] = round(temp_df[temp_df['status_amo'].isin(zoom_in_status)].shape[0] / temp_df.shape[0] * 100, 1)
        lead_df.loc[i, '% продаж'] = round(temp_df[temp_df['status_amo'].isin(payment_status)].shape[0] / temp_df.shape[0] * 100, 1)

        # lead_df.loc[i, 'Средний чек'] = '?' # temp_df['payment_amount'].sum() / temp_df[temp_df['payment_amount'] != 0].shape[0]
        lead_df.loc[i, 'Оборот'] = temp_df['payment_amount'].sum()
        lead_df.loc[i, 'ROMI'] = round(0 if temp_df['channel_expense'].sum() == 0 else (temp_df['payment_amount'].sum() - temp_df['channel_expense'].sum()) / temp_df['channel_expense'].sum() * 100, 2)
        # lead_df.loc[i, 'ROI'] = '?'
        lead_df.loc[i, 'Маржа'] = temp_df['payment_amount'].sum() - temp_df['channel_expense'].sum()
        lead_df.loc[i, 'Цена проведенного ZOOM'] = round(0 if done_zoom == 0 else expenses / done_zoom, 2)
        lead_df.loc[i, 'Оборот на ZOOM'] = round(0 if done_zoom == 0 else turnover / done_zoom, 2)
        lead_df.loc[i, 'CV в счет'] = round(temp_df[temp_df['payment_amount'] != 0].shape[0] / temp_df.shape[0], 2)
        lead_df.loc[i, 'CPO'] = round(temp_df['channel_expense'].sum() / temp_df[temp_df['payment_amount'] != 0].shape[0], 2) if temp_df[temp_df['payment_amount'] != 0].shape[0] != 0 else 0
        lead_df.loc[i, 'Доля лидов'] = round(temp_df.shape[0] / df.shape[0] * 100, 2)
        lead_df.loc[i, 'Доля канала в бюджете'] = round(0 if df['channel_expense'].sum() == 0 else temp_df['channel_expense'].sum() / df['channel_expense'].sum() * 100, 2)
        lead_df.loc[i, 'Доля канала в обороте'] = round(0 if df['payment_amount'].sum() == 0 else temp_df['payment_amount'].sum() / df['payment_amount'].sum() * 100, 2)
        lead_df.loc[i, 'CV в продажу'] = '?'
        lead_df.loc[i, 'Оборот на лида/анкету'] = round(temp_df['payment_amount'].sum() / temp_df.shape[0], 2)
        lead_df.loc[i, '% лидов/анкет (доля от общего)'] = round(temp_df.shape[0] / df.shape[0] * 100, 2)
        lead_df.loc[i, '% Оборот (доля от общего)'] = round(0 if df['payment_amount'].sum() == 0 else temp_df['payment_amount'].sum() / df['payment_amount'].sum() * 100, 2)
        lead_df.loc[i, '% Оплат (доля от общего)'] = '?' # (temp_df[temp_df['payment_amount'] != 0].shape[0] / df[df['payment_amount'] != 0].shape[0] * 100, 2)
        lead_df.loc[i, '% Затрат (доля от общего)'] = round(0 if df['channel_expense'].sum() == 0 else temp_df['channel_expense'].sum() / df['channel_expense'].sum() * 100, 2)
        lead_df.loc[i, 'Расход на ОП'] = '?' # done_zoom * 340 + done_zoom * 268 + count_pay * 3584
        lead_df.loc[i, 'Расход общий (ОП+бюджет)'] = '?' # temp_df['Бюджет'] + temp_df['Расход на ОП']
        lead_df.loc[i, '% расходов на трафик (доля от Расход общий)'] = '?' # round(temp_df['Бюджет'] / (temp_df['Бюджет'] + temp_df['Расход на ОП']), 2)
        lead_df.loc[i, '% расходов на ОП (доля от Расход общий)'] = '?' # round(temp_df['Расход на ОП'] / (temp_df['Бюджет'] + temp_df['Расход на ОП']), 2)
      else:
        pass
  for i in range(len(columns)):
      lead_df.iloc[7, i] = lead_df.iloc[5, i] + lead_df.iloc[6, i]
  lead_df.rename(index={7 :'5-6'}, inplace = True)
  return {'Статистика лиды' :lead_df}

def get_leads_ta_stats(df):
  status = pd.read_csv('data/status.csv')
  status.fillna(0, inplace=True)

  target_audience = get_target_audience()
    
  status.fillna(0, inplace=True)

  payment_status = status[status['Оплатил'] == ' +']['Соответствующий статус в воронке Теплые продажи'].unique().tolist() # Статусы с этапом "оплатил"
  was_conversation_status = status[status['Был разговор'] == ' +']['Соответствующий статус в воронке Теплые продажи'].unique().tolist() # Статусы с этапом "был разговор"
  in_progress_status = status[status['В работе'] == ' +']['Соответствующий статус в воронке Теплые продажи'].unique().tolist() # Статусы с этапом "в работе"

  # df.insert(2, 'ca_num', 0)
  # for i in range(df.shape[0]):
  #     if df.loc[i, 'quiz_answers1'] in target_audience:
  #         df.loc[i, 'ca_num'] += 1
  #     if df.loc[i, 'quiz_answers2'] in target_audience:
  #         df.loc[i, 'ca_num'] += 1
  #     if df.loc[i, 'quiz_answers3'] in target_audience:
  #         df.loc[i, 'ca_num'] += 1
  #     if df.loc[i, 'quiz_answers4'] in target_audience:
  #         df.loc[i, 'ca_num'] += 1
  #     if df.loc[i, 'quiz_answers5'] in target_audience:
  #         df.loc[i, 'ca_num'] += 1
  #     if df.loc[i, 'quiz_answers6'] in target_audience:
  #         df.loc[i, 'ca_num'] += 1

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
             '% продаж', 'Средний чек', 'Оборот', 'Расход на ОП', 'Расход общий (ОП+бюджет)', '% расходов на трафик (доля от Расход общий)', '% расходов на ОП (доля от Расход общий)',
             'ROI', 'Маржа', 'Цена Разговора', 'Цена Оффера', 'Цена Счета',
             'Оборот на лида (на обработанного лида)', 'Оборот на разговор', 'CV обр.лид/оплата', 'CV разговор/оплата', 'CPO',
             '% лидов (доля от общего)', '% Оборот (доля от общего)', '% Оплат (доля от общего)', '% Затрат (доля от общего)']
	
  data = np.zeros((8, len(columns))).astype('int')
  lead_df = pd.DataFrame(data=data, columns=columns)
  lead_df['Оборот'] = lead_df['Оборот'].astype(float)
  for i in range(7):
      temp_df = df[df['target_class'] == i]
      temp_df['payment_amount'] = temp_df['payment_amount'].astype(float)
      if temp_df.shape[0] != 0:

        # done_zoom = temp_df[temp_df['status_amo'].isin(zoom_in_status)].shape[0]
        # expenses = temp_df[temp_df['status_amo'].isin(zoom_in_status)]['channel_expense'].sum()
        # turnover = temp_df[temp_df['status_amo'].isin(zoom_in_status)]['payment_amount'].sum()

        lead_df.loc[i, 'Бюджет'] = temp_df['channel_expense'].sum()
        lead_df.loc[i, 'Кол-во лидов'] = temp_df.shape[0]
        lead_df.loc[i, '% в работе'] = round(temp_df[temp_df['status_amo'].isin(in_progress_status)].shape[0] / temp_df.shape[0] * 100, 1)
        lead_df.loc[i, '% дозвонов'] = round(temp_df[temp_df['status_amo'].isin(was_conversation_status)].shape[0] / temp_df.shape[0] * 100, 1)
        lead_df.loc[i, '% офферов'] = round(temp_df[temp_df['status_amo'] == 'Сделан оффер из Теплые продажи'].shape[0] / temp_df.shape[0] * 100, 1)
        lead_df.loc[i, '% счетов'] = round(temp_df[temp_df['status_amo'] == 'Выставлен счет из Теплые продажи'].shape[0] / temp_df.shape[0] * 100, 1)
        lead_df.loc[i, 'Цена лида'] = round(temp_df[temp_df['channel_expense'] != 0]['channel_expense'].sum() / temp_df.shape[0], 1)
        lead_df.loc[i, '% продаж'] = round(temp_df[temp_df['status_amo'].isin(payment_status)].shape[0] / temp_df.shape[0] * 100, 1)
        lead_df.loc[i, 'Средний чек'] = temp_df['payment_amount'].sum() / temp_df[temp_df['payment_amount'] != 0].shape[0] if temp_df[temp_df['payment_amount'] != 0].shape[0] != 0 else 0
        lead_df.loc[i, 'Оборот'] = temp_df['payment_amount'].sum()
        lead_df.loc[i, 'Расход на ОП'] = (float(temp_df['payment_amount'].sum()) * 0.6 + 150 * float(temp_df[temp_df['payment_amount'] != 0].shape[0]) + 27 * float(temp_df[temp_df['payment_amount'] != 0].shape[0]) + 20 * float(temp_df[temp_df['payment_amount'] != 0].shape[0])) * 1.4
        lead_df.loc[i, 'Расход общий (ОП+бюджет)'] = float(lead_df.loc[i, 'Бюджет']) + float(lead_df.loc[i, 'Расход на ОП'])
        lead_df.loc[i, '% расходов на трафик (доля от Расход общий)'] = round(float(lead_df.loc[i, 'Бюджет'])/float(lead_df.loc[i, 'Расход общий (ОП+бюджет)'] * 100), 2) if float(lead_df.loc[i, 'Расход общий (ОП+бюджет)'] * 100) != 0 else float('nan')
        lead_df.loc[i, '% расходов на ОП (доля от Расход общий)'] = round(lead_df.loc[i, 'Расход на ОП']/lead_df.loc[i, 'Расход общий (ОП+бюджет)'] * 100, 2)
        lead_df.loc[i, 'ROI'] = round((temp_df['payment_amount'].sum() - temp_df['channel_expense'].sum()) / temp_df['channel_expense'].sum() * 100, 2) if temp_df[temp_df['payment_amount'] != 0].shape[0] != 0 else 0
        lead_df.loc[i, 'Маржа'] = float(temp_df['payment_amount'].sum()) - float(temp_df['channel_expense'].sum())
        lead_df.loc[i, 'Цена Разговора'] = temp_df['channel_expense'].sum() / temp_df[temp_df['status_amo'].isin(was_conversation_status)].shape[0] \
                                           if temp_df[temp_df['status_amo'].isin(was_conversation_status)].shape[0] != 0 else 0 
        lead_df.loc[i, 'Цена Оффера'] = temp_df['channel_expense'].sum() / temp_df[temp_df['status_amo'] == 'Сделан оффер из Теплые продажи'].shape[0] \
                                        if temp_df[temp_df['channel_expense'] == 'Сделан оффер из Теплые продажи'].shape[0] != 0 else 0 
        lead_df.loc[i, 'Цена Счета'] = temp_df['channel_expense'].sum() / temp_df[temp_df['status_amo'] == 'Выставлен счет из Теплые продажи'].shape[0] \
                                       if temp_df[temp_df['status_amo'] == 'Выставлен счет из Теплые продажи'].shape[0] != 0 else 0


        lead_df.loc[i, 'Оборот на лида (на обработанного лида)'] = float(lead_df.loc[i, 'Оборот']) / float(temp_df[temp_df['status_amo'] == 'Обработанная заявка из Теплые продажи'].shape[0]) if float(temp_df[temp_df['status_amo'] == 'Обработанная заявка из Теплые продажи'].shape[0]) != 0 else float('nan')
        lead_df.loc[i, 'Оборот на разговор'] = lead_df.loc[i, 'Оборот'] / temp_df[temp_df['status_amo'].isin(was_conversation_status)].shape[0]
        lead_df.loc[i, 'CV обр.лид/оплата'] = temp_df[temp_df['status_amo'] == 'Обработанная заявка из Теплые продажи'].shape[0]/temp_df[temp_df['payment_amount'] != 0].shape[0]\
                                              if temp_df[temp_df['payment_amount'] != 0].shape[0] != 0 else 0
        lead_df.loc[i, 'CV разговор/оплата'] = temp_df[temp_df['status_amo'].isin(was_conversation_status)].shape[0]/temp_df[temp_df['payment_amount'] != 0].shape[0]\
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

  for col in columns:
    if col not in ['Средний чек', '% расходов на трафик (доля от Расход общий)', '% расходов на ОП (доля от Расход общий)']:
      try:
        lead_df.loc[7, col] = lead_df.loc[5, col] + lead_df.loc[6, col]
      except TypeError as e:
        lead_df.loc[7, col] = '?'
    elif col == 'Средний чек':
      lead_df.loc[7, col] = df[df['target_class'].isin([5,6])]['payment_amount'].sum() / df[df['payment_amount'] != 0].shape[0] \
                            if df[df['payment_amount'] != 0].shape[0] != 0 else 0
    elif col == '% расходов на трафик (доля от Расход общий)':
      lead_df.loc[7, col] = round(lead_df.loc[7, 'Бюджет']/lead_df.loc[7, 'Расход общий (ОП+бюджет)'] * 100, 2)
    elif col == '% расходов на ОП (доля от Расход общий)':
      lead_df.loc[7, col] = round(lead_df.loc[7, 'Расход на ОП']/lead_df.loc[7, 'Расход общий (ОП+бюджет)'] * 100, 2)
  lead_df.rename(index={7 :'5-6'}, inplace = True)
  return {'Статистика лиды' :lead_df}

data = get_leads_data()
print('get data')
leads_ta_stats = get_leads_ta_stats(data)
print('calculated leads ta stats')
with open('results/leads_ta_stats.pkl', 'wb') as f:
  pkl.dump(leads_ta_stats, f)
