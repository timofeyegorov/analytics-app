from app.database import get_target_audience, get_leads_data

import pandas as pd
import numpy as np
import pickle as pkl
import os
from config import RESULTS_FOLDER

# Функция подсчета показателей по подкатегориям каждой категории и лендингам
def calculate_turnover(df):
  df.reset_index(inplace=True, drop=True)

  with open(os.path.join(RESULTS_FOLDER, 'target_audience.pkl'), 'rb') as f:
    target_audience = pkl.load(f)

  # target_audience = get_target_audience()
  for i in range(df.shape[0]):
      df.loc[i, 'traffic_channel'] = df.loc[i, 'traffic_channel'].split('?')[0]

  # Создаем списки с уникальными значениями по ленгдингам
  landings = df['traffic_channel'].unique().tolist()

  def turnover_in(column_name):
    flag = True # Если считаем показатели по категориям
    if column_name == 'traffic_channel':
      flag = False # Если считаем показатели по лендингам

    # Создаем список подкатегорий выбранной категории (с элементом сортирвки, когда сначала идут ЦА подкатегории)
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

    # Индекс кол-ва ЦА подкатегорий текущей категории, которые есть в таблице
    targ_idx = len(df[df[column_name].isin(target_audience)][column_name].unique().tolist())
    # Создаем заготовку из нулей под результирующую таблицу
    data_turnover = np.zeros((len(subcategories)+2, 7)).astype('int') if flag else np.zeros((len(subcategories), 7)).astype('int')
    columns_turnover = ['Сегмент', 'Анкет', 'Оборот', 'Оборот на анкету', 'Оплат', 'CV', 'Чек']
    df_turnover = pd.DataFrame(data_turnover, columns=columns_turnover)

    # Проходим по каждой подкатегории
    for i in range(len(subcategories)):

      df_turnover.iloc[i,0] = subcategories[i] # Заполняем столбец Сегмент

      df_turnover.iloc[i,1] = df[df[column_name]==subcategories[i]].shape[0] # Заполняем столбец Анкет

      if df[df[column_name]==subcategories[i]].shape[0] !=0 : # Заполняем столбцы Оборот, Оборот на анкету и CV
        df_turnover.iloc[i,2] = int(round(df[df[column_name]==subcategories[i]]['payment_amount'].sum(),0))
        df_turnover.iloc[i,3] = df[df[column_name]==subcategories[i]]['payment_amount'].sum()\
                                // df[df[column_name]==subcategories[i]].shape[0]
        df_turnover.iloc[i,5] = round(df[(df[column_name]==subcategories[i]) & (df['payment_amount']!=0)].shape[0]\
                                /df[df[column_name]==subcategories[i]].shape[0]*100, 1)

      # Заполняем столбец Оплат
      df_turnover.iloc[i,4] = df[(df[column_name]==subcategories[i]) & (df['payment_amount']!=0)].shape[0]

      if df_turnover.iloc[i,5] != 0: # Заполняем столбец Чек
        df_turnover.iloc[i,6] = int(round(df[df[column_name]==subcategories[i]]['payment_amount'].sum()/df[(df[column_name]==subcategories[i]) & (df['payment_amount']!=0)].shape[0], 0))
      else:
        df_turnover.iloc[i,6] = 0
    if flag:
      df_turnover.iloc[i+1,0] = 'ЦА'
      # if targ_idx != 0:

      df_turnover.iloc[i+1,1] = df_turnover.iloc[:targ_idx,1].sum()
      df_turnover.iloc[i+1,2] = df_turnover.iloc[:targ_idx,2].sum()

      if df_turnover.iloc[i+1,1] != 0:
        df_turnover.iloc[i+1,3] = int(round(df_turnover.iloc[i+1,2]/df_turnover.iloc[i+1,1], 0))
      else:
        df_turnover.iloc[i+1,3] = 0

      df_turnover.iloc[i+1,4] = df_turnover.iloc[:targ_idx,4].sum()
      df_turnover.iloc[i+1,5] = round(df_turnover.iloc[i+1,4]/df_turnover.iloc[i+1,1]*100, 1)
      if df_turnover.iloc[i+1,4] != 0:
        df_turnover.iloc[i+1,6] = int(round(df_turnover.iloc[:targ_idx,2].sum()/df_turnover.iloc[:targ_idx,4].sum(), 0))
      else:
        df_turnover.iloc[i+1,6] = 0

      df_turnover.iloc[i+2,0] = 'не ЦА'
      df_turnover.iloc[i+2,1] = df_turnover.iloc[:len(subcategories),1].sum() - df_turnover.iloc[:targ_idx,1].sum()
      df_turnover.iloc[i+2,2] = df_turnover.iloc[:len(subcategories),2].sum() - df_turnover.iloc[:targ_idx,2].sum()
      if df_turnover.iloc[i+2,1] != 0:
        df_turnover.iloc[i+2,3] = int(round(df_turnover.iloc[i+2,2]/df_turnover.iloc[i+2,1], 0))
      else:
        df_turnover.iloc[i+2,3] = 0

      df_turnover.iloc[i+2,4] = df_turnover.iloc[:len(subcategories),4].sum() - df_turnover.iloc[:targ_idx,4].sum()

      if df_turnover.iloc[i+2,1] != 0:
        df_turnover.iloc[i+2,5] = round(df_turnover.iloc[i+2,4]/df_turnover.iloc[i+2,1]*100, 1)
      else:
        df_turnover.iloc[i+2,5] = 0

      if df_turnover.iloc[i+2,4] != 0:
        df_turnover.iloc[i+2,6] = int(round(df_turnover.iloc[i+2,2]/df_turnover.iloc[i+2,4], 0))
      else:
        df_turnover.iloc[i+2,6] = 0

      # Делаем новую заготовку под вторую таблицу
      data_turnover = np.zeros((2, 4)).astype('int')
      columns_turnover = ['Сегмент',	'% Анкет',	'% Оборот',	'% Оплат']
      df_turnover_ca = pd.DataFrame(data_turnover, columns=columns_turnover)

      df_turnover_ca.iloc[0,0] = 'ЦА'
      if df_turnover.iloc[-2:,1].sum() != 0:
        df_turnover_ca.iloc[0,1] = round(df_turnover.iloc[i+1,1]/df_turnover.iloc[-2:,1].sum(),2)*100
      else:
        df_turnover_ca.iloc[0,1] = 0
      if df.loc[:,'payment_amount'].sum() != 0:
        df_turnover_ca.iloc[0,2] = round(df_turnover.iloc[i+1,2]/df.loc[:,'payment_amount'].sum()*100,2)
      else:
        df_turnover_ca.iloc[0,2] = 0
      if df[df['payment_amount'] != 0].shape[0] != 0:
        df_turnover_ca.iloc[0,3] = round(df_turnover.iloc[i+1,4]/df[df['payment_amount'] != 0].shape[0]*100,2)
      else:
        df_turnover_ca.iloc[0,3] = 0

      df_turnover_ca.iloc[1,0] = 'не ЦА'
      if df_turnover.iloc[-2:,1].sum() != 0:
        df_turnover_ca.iloc[1,1] = round(df_turnover.iloc[i+2,1]/df_turnover.iloc[-2:,1].sum(),2)*100
      else:
        df_turnover_ca.iloc[1,1] = 0
      if df.loc[:,'payment_amount'].sum() != 0:
        df_turnover_ca.iloc[1,2] = round(df_turnover.iloc[i+2,2]/df.loc[:,'payment_amount'].sum()*100,2)
      else:
        df_turnover_ca.iloc[1,2] = 0
      if df[df['payment_amount'] != 0].shape[0] != 0:
        df_turnover_ca.iloc[1,3] = round(df_turnover.iloc[i+2,4]/df[df['payment_amount'] != 0].shape[0]*100,2)
      else:
        df_turnover_ca.iloc[1,3] = 0

      if df_turnover['Оборот на анкету'].iloc[-1] != 0:
        mnozhitel = round(df_turnover['Оборот на анкету'].iloc[-2]/df_turnover['Оборот на анкету'].iloc[-1],1)
      else:
        mnozhitel = 0

      return [df_turnover, df_turnover_ca, mnozhitel]
    else:
      return df_turnover

  countries_df = turnover_in('quiz_answers1')
  ages_df = turnover_in('quiz_answers2')
  jobs_df = turnover_in('quiz_answers3')
  earnings_df = turnover_in('quiz_answers4')
  trainings_df = turnover_in('quiz_answers5')
  times_df = turnover_in('quiz_answers6')
  landings_df = turnover_in('traffic_channel')


  data_ta = np.zeros((8, 2)).astype('int')
  df_ta = pd.DataFrame(data_ta, columns=['Абс', '%'])

  for i in range(7):
    df_ta.loc[i, 'Абс'] = df[df['target_class'] == i].shape[0]
    df_ta.loc[i, '%'] = int(round(df[df['target_class'] == i].shape[0]/df.shape[0]*100, 0))

    df_ta.loc[7, 'Абс'] = df_ta.loc[5:6, 'Абс'].sum()

    df_ta.loc[7, '%'] = df_ta.loc[5:6, '%'].sum()


  df_ta.rename(index={7: '5-6'}, inplace=True)

  def get_channels_groups(df):
    columns = ['Аудитория', 'Анкет', '% Анкет', 'Оборот', '% Оборот', 'Оборот на анкету', 'Оплат', '% Оплат', 'CV',
               'Чек']
    # data = np.zeros((11, len(columns)))
    output_df = pd.DataFrame(columns=columns)

    for el in [[0], [1], [2], [3], [4], [5], [6], [5, 6], [4, 5, 6], [0, 1, 2, 3, 4], [0, 1, 2, 3, 4, 5, 6]]:
      temp_df_ankets_ca = df[df['target_class'].isin(el)]

      # try:
      new_row = {
        'Аудитория': 'ЦА ' + str(el),
        'Анкет': temp_df_ankets_ca.shape[0],
        '% Анкет': round(temp_df_ankets_ca.shape[0] / df.shape[0] * 100 \
                           if df.shape[0] != 0 else 0, 1),

        'Оборот': round(temp_df_ankets_ca['payment_amount'].sum(), 1),
        '% Оборот': round(temp_df_ankets_ca['payment_amount'].sum() / df['payment_amount'].sum() * 100 \
                            if df['payment_amount'].sum() != 0 else 0, 1),

        'Оборот на анкету': round(temp_df_ankets_ca['payment_amount'].sum() / temp_df_ankets_ca.shape[0], 1) \
          if temp_df_ankets_ca.shape[0] != 0 else 0,

        'Оплат': temp_df_ankets_ca[temp_df_ankets_ca['payment_amount'] != 0].shape[0],

        '% Оплат': round(
          temp_df_ankets_ca[temp_df_ankets_ca['payment_amount'] != 0].shape[0] / df[df['payment_amount'] != 0].shape[
            0] * 100 \
            if df[df['payment_amount'] != 0].shape[0] != 0 else 0, 1),

        'CV': round(
          temp_df_ankets_ca[temp_df_ankets_ca['payment_amount'] != 0].shape[0] / temp_df_ankets_ca.shape[0] * 100 \
            if temp_df_ankets_ca.shape[0] != 0 else 0, 1),

        'Чек': round(temp_df_ankets_ca['payment_amount'].sum() /
               temp_df_ankets_ca[temp_df_ankets_ca['payment_amount'] != 0].shape[0], 1) \
          if temp_df_ankets_ca[temp_df_ankets_ca['payment_amount'] != 0].shape[0] != 0 else 0
      }
      output_df = output_df.append(new_row, ignore_index=True)
      # except ZeroDivisionError:
      #   new_row = {'Аудитория': 'ЦА (5-6)'}
      #   output_df = output_df.append(new_row, ignore_index=True)
    return output_df

  turnover_ta = get_channels_groups(df)
  return {'Страна':countries_df,
          'Возраст':ages_df,
          'Работа':jobs_df,
          'Доход':earnings_df,
          'Обучение':trainings_df,
          'Время':times_df,
          'traffic_channel':landings_df,
          'Оборот на ЦА': turnover_ta,
          'ЦА': df_ta}


if __name__=='__main__':
  with open(os.path.join((RESULTS_FOLDER), 'leads.pkl'), 'rb') as f:
    data = pkl.load(f)

    res, ta = calculate_turnover(data[:2000])
    print(type(res))
    print(len(res))
    for item in res.values():
      # print(item)
      print(type(item[0]), type(item[1]), type(item[2]))
    print()