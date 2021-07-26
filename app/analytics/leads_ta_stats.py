from app.database import get_target_audience

import numpy as np
import pandas as pd

def get_leads_ta_stats(df):
  status = pd.read_csv('status.csv')
  status.fillna(0, inplace=True)

  countries, ages, jobs, earnings, trainings, times = get_target_audience()

  payment_status = status[status['Оплатил'] != 0]['Статус'].unique().tolist() # Статусы с этапом оплатил
  zoom_in_status = status[status['Проведен zoom'] != 0]['Статус'].unique().tolist() # Статусы с этапом проведен зум
  ring_up_status = status[status['Дозвон'] != 0]['Статус'].unique().tolist() # Статусы с этапом дозвон
  in_progress_status = status[status['В работе'] != 0]['Статус'].unique().tolist() # Статусы с этапом в работе
  zoom_assigned_status = status[status['Назначен zoom'] != 0]['Статус'].unique().tolist() # Статусы с этапом назначен зум

  df.insert(2, 'ca_num', 0)
  for i in range(df.shape[0]):
      if df.loc[i, 'quiz_answers1'] in countries:
          df.loc[i, 'ca_num'] += 1
      if df.loc[i, 'quiz_answers2'] in ages:
          df.loc[i, 'ca_num'] += 1
      if df.loc[i, 'quiz_answers3'] in jobs:
          df.loc[i, 'ca_num'] += 1
      if df.loc[i, 'quiz_answers4'] in earnings:
          df.loc[i, 'ca_num'] += 1
      if df.loc[i, 'quiz_answers5'] in trainings:
          df.loc[i, 'ca_num'] += 1
      if df.loc[i, 'quiz_answers6'] in times:
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
        lead_df.loc[i, 'ROMI'] = round((temp_df['payment_amount'].sum() - temp_df['channel_expense'].sum()) / temp_df['channel_expense'].sum() * 100, 2)
        # lead_df.loc[i, 'ROI'] = '?'
        lead_df.loc[i, 'Маржа'] = temp_df['payment_amount'].sum() - temp_df['channel_expense'].sum()
        lead_df.loc[i, 'Цена проведенного ZOOM'] = round(0 if done_zoom == 0 else expenses / done_zoom, 2)
        lead_df.loc[i, 'Оборот на ZOOM'] = round(0 if done_zoom == 0 else turnover / done_zoom, 2)
        lead_df.loc[i, 'CV в счет'] = round(temp_df[temp_df['payment_amount'] != 0].shape[0] / temp_df.shape[0], 2)
        lead_df.loc[i, 'CPO'] = round(temp_df['channel_expense'].sum() / temp_df[temp_df['payment_amount'] != 0].shape[0], 2) if temp_df[temp_df['payment_amount'] != 0].shape[0] != 0 else 0
        lead_df.loc[i, 'Доля лидов'] = round(temp_df.shape[0] / df.shape[0] * 100, 2)
        lead_df.loc[i, 'Доля канала в бюджете'] = round(temp_df['channel_expense'].sum() / df['channel_expense'].sum() * 100, 2)
        lead_df.loc[i, 'Доля канала в обороте'] = round(temp_df['payment_amount'].sum() / df['payment_amount'].sum() * 100, 2)
        lead_df.loc[i, 'CV в продажу'] = '?'
        lead_df.loc[i, 'Оборот на лида/анкету'] = round(temp_df['payment_amount'].sum() / temp_df.shape[0], 2)
        lead_df.loc[i, '% лидов/анкет (доля от общего)'] = round(temp_df.shape[0] / df.shape[0] * 100, 2)
        lead_df.loc[i, '% Оборот (доля от общего)'] = round(temp_df['payment_amount'].sum() / df['payment_amount'].sum() * 100, 2)
        lead_df.loc[i, '% Оплат (доля от общего)'] = '?' # (temp_df[temp_df['payment_amount'] != 0].shape[0] / df[df['payment_amount'] != 0].shape[0] * 100, 2)
        lead_df.loc[i, '% Затрат (доля от общего)'] = round(temp_df['channel_expense'].sum() / df['channel_expense'].sum() * 100, 2)
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
