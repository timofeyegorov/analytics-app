from app.database import get_accounts, get_leads_data, get_target_audience

import pandas as pd
from urllib import parse
import pickle as pkl
import numpy as np
import os
from config import RESULTS_FOLDER, DATA_FOLDER

def calculate_segments_stats(df):
  df.reset_index(inplace=True, drop=True)
  # target_audience = get_target_audience()

  with open(os.path.join(RESULTS_FOLDER, 'target_audience.pkl'), 'rb') as f:
    target_audience = pkl.load(f)

  with open(os.path.join(RESULTS_FOLDER, 'statuses.pkl'), 'rb') as f:
    status = pkl.load(f)
  status.fillna(0, inplace=True)

  payment_status = status[status['Оплатил'] == ' +']['Статус АМО'].unique().tolist() # Статусы с этапом "оплатил"
  conversation_status = status[status['Был разговор'] == ' +']['Статус АМО'].unique().tolist() # Статусы с этапом "был разговор"
  progress_status = status[status['В работе'] == ' +']['Статус АМО'].unique().tolist() # Статусы с этапом "в работе"


  # try:
  #   df.insert(19, 'trafficologist', 'Неизвестно')                # Добавляем столбец trafficologist для записи имени трафиколога
  #   df.insert(20, 'account', 'Неизвестно 1')                       # Добавляем столбец account для записи аккаунта трафиколога
  #   df.insert(21, 'target_class', 0)
  # except ValueError:
  #   pass
  #
  # for i in range(df.shape[0]):
  #   target_class = 0
  #   if df.loc[i, 'quiz_answers1'] in target_audience:
  #     target_class += 1
  #   if df.loc[i, 'quiz_answers2'] in target_audience:
  #     target_class += 1
  #   if df.loc[i, 'quiz_answers3'] in target_audience:
  #     target_class += 1
  #   if df.loc[i, 'quiz_answers4'] in target_audience:
  #     target_class += 1
  #   if df.loc[i, 'quiz_answers5'] in target_audience:
  #     target_class += 1
  #   if df.loc[i, 'quiz_answers6'] in target_audience:
  #     target_class += 1
  #
  #   df.loc[i, 'target_class'] = target_class

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
        df_category.loc[i, '% в работе'] = round(temp_df[temp_df['status_amo'].isin(progress_status)].shape[0] / temp_df.shape[0] * 100, 1)
        df_category.loc[i, '% дозвонов'] = round(temp_df[temp_df['status_amo'].isin(conversation_status)].shape[0] / temp_df.shape[0] * 100, 1)
        df_category.loc[i, '% офферов'] = round(temp_df[temp_df['status_amo'] == 'Сделан оффер из Теплые продажи'].shape[0] / temp_df.shape[0] * 100, 1)
        df_category.loc[i, '% счетов'] = round(temp_df[temp_df['status_amo'] == 'Выставлен счет из Теплые продажи'].shape[0] / temp_df.shape[0] * 100, 1)
        df_category.loc[i, 'Цена лида'] = round(temp_df[temp_df['channel_expense'] != 0]['channel_expense'].sum() / temp_df.shape[0], 1)

        df_category.loc[i, '% ЦА 5-6'] = round(temp_df[temp_df['target_class'].isin([5,6])].shape[0] / temp_df.shape[0] * 100, 1)
        df_category.loc[i, '% ЦА 5'] = round(temp_df[temp_df['target_class'] == 5].shape[0] / temp_df.shape[0] * 100, 1)
        df_category.loc[i, '% ЦА 6'] = round(temp_df[temp_df['target_class'] == 6].shape[0] / temp_df.shape[0] * 100, 1)
        df_category.loc[i, '% ЦА 4-5-6'] = round(temp_df[temp_df['target_class'].isin([4, 5,6])].shape[0] / temp_df.shape[0] * 100, 1)
        df_category.loc[i, '% ЦА 4'] = round(temp_df[temp_df['target_class'] == 4].shape[0] / temp_df.shape[0] * 100, 1)
        df_category.loc[i, '% не ЦА'] = round(temp_df[temp_df['target_class'] == 0].shape[0] / temp_df.shape[0] * 100, 1)
        df_category.loc[i, '% ЦА 3'] = round(temp_df[temp_df['target_class'] == 3].shape[0] / temp_df.shape[0] * 100, 1)
        df_category.loc[i, '% ЦА 2'] = round(temp_df[temp_df['target_class'] == 2].shape[0] / temp_df.shape[0] * 100, 1)
        df_category.loc[i, '% ЦА 1'] = round(temp_df[temp_df['target_class'] == 1].shape[0] / temp_df.shape[0] * 100, 1)
        df_category.loc[i, 'Цена ЦА 5-6'] = temp_df[temp_df['target_class'].isin([5,6])]['channel_expense'].sum() / temp_df[temp_df['target_class'].isin([5,6])].shape[0] if temp_df[temp_df['target_class'].isin([5,6])].shape[0] != 0 else float('nan')
        df_category.loc[i, 'Цена ЦА 4-5-6'] = temp_df[temp_df['target_class'].isin([4,5,6])]['channel_expense'].sum() / temp_df[temp_df['target_class'].isin([4,5,6])].shape[0] if temp_df[temp_df['target_class'].isin([4,5,6])].shape[0] != 0 else float('nan')

        df_category.loc[i, '% продаж'] = round(temp_df[temp_df['status_amo'].isin(payment_status)].shape[0] / temp_df.shape[0] * 100, 1)
        df_category.loc[i, 'Средний чек'] = temp_df['payment_amount'].sum() / temp_df[temp_df['payment_amount'] != 0].shape[0] if temp_df[temp_df['payment_amount'] != 0].shape[0] != 0 else 0
        df_category.loc[i, 'Оборот'] = temp_df['payment_amount'].sum()
        df_category.loc[i, 'Расход на ОП'] = (float(temp_df['payment_amount'].sum()) * 0.06 + 150 * float(temp_df[temp_df['payment_amount'] != 0].shape[0]) + 27 * float(temp_df[temp_df['payment_amount'] != 0].shape[0]) + 20 * float(temp_df[temp_df['payment_amount'] != 0].shape[0])) * 1.4
        df_category.loc[i, 'Расход общий (ОП+бюджет)'] = float(df_category.loc[i, 'Бюджет']) + float(df_category.loc[i, 'Расход на ОП'])
        df_category.loc[i, '% расходов на трафик (доля от Расход общий)'] = round(float(df_category.loc[i, 'Бюджет'])/float(df_category.loc[i, 'Расход общий (ОП+бюджет)'] * 100), 1) if float(df_category.loc[i, 'Расход общий (ОП+бюджет)']) != 0 else float('nan')
        df_category.loc[i, '% расходов на ОП (доля от Расход общий)'] = round(df_category.loc[i, 'Расход на ОП']/df_category.loc[i, 'Расход общий (ОП+бюджет)'] * 100, 1)
        df_category.loc[i, 'ROI'] = round((temp_df['payment_amount'].sum() - temp_df['channel_expense'].sum()) / temp_df['channel_expense'].sum() * 100, 1) if temp_df['channel_expense'].sum() != 0 else 0
        df_category.loc[i, 'Маржа'] = temp_df['payment_amount'].sum() - temp_df['channel_expense'].sum()
        df_category.loc[i, 'Цена Разговора'] = temp_df['channel_expense'].sum() / temp_df[temp_df['status_amo'].isin(conversation_status)].shape[0] \
                                           if temp_df[temp_df['status_amo'].isin(conversation_status)].shape[0] != 0 else 0
        df_category.loc[i, 'Цена Оффера'] = temp_df['channel_expense'].sum() / temp_df[temp_df['status_amo'] == 'Сделан оффер из Теплые продажи'].shape[0] \
                                        if temp_df[temp_df['channel_expense'] == 'Сделан оффер из Теплые продажи'].shape[0] != 0 else 0 
        df_category.loc[i, 'Цена Счета'] = temp_df['channel_expense'].sum() / temp_df[temp_df['status_amo'] == 'Выставлен счет из Теплые продажи'].shape[0] \
                                       if temp_df[temp_df['status_amo'] == 'Выставлен счет из Теплые продажи'].shape[0] != 0 else 0


        df_category.loc[i, 'Оборот на лида (на обработанного лида)'] = float(df_category.loc[i, 'Оборот']) / float(temp_df[temp_df['status_amo'] == 'Обработанная заявка из Теплые продажи'].shape[0]) if float(temp_df[temp_df['status_amo'] == 'Обработанная заявка из Теплые продажи'].shape[0]) != 0 else float('nan')
        df_category.loc[i, 'Оборот на разговор'] = float(df_category.loc[i, 'Оборот']) / float(temp_df[temp_df['status_amo'].isin(conversation_status)].shape[0]) if float(temp_df[temp_df['status_amo'].isin(conversation_status)].shape[0]) != 0 else float('nan')
        df_category.loc[i, 'CV обр.лид/оплата'] = temp_df[temp_df['status_amo'] == 'Обработанная заявка из Теплые продажи'].shape[0]/temp_df[temp_df['payment_amount'] != 0].shape[0]\
                                              if temp_df[temp_df['payment_amount'] != 0].shape[0] != 0 else 0
        df_category.loc[i, 'CV разговор/оплата'] = temp_df[temp_df['status_amo'].isin(conversation_status)].shape[0]/temp_df[temp_df['payment_amount'] != 0].shape[0]\
                                                if temp_df[temp_df['payment_amount'] != 0].shape[0] != 0 else 0
        # output_df.loc[i, 'CPO'] = round(temp_df[temp_df['payment_amount'] != 0].sum() / temp_df[temp_df['payment_amount'] != 0].shape[0], 2)
        # output_df.loc[i, '% лидов (доля от общего)'] = round(temp_df.shape[0]/result.shape[0]*100, 2)
        # output_df.loc[i, '% Оборот (доля от общего)'] = round(temp_df['payment_amount'].sum()/result['payment_amount'].sum(), 2)
        # output_df.loc[i, '% Оплат (доля от общего)'] = round(temp_df[temp_df['payment_amount'].isin(payment_status)].shape[0]/result[result['payment_amount'].isin(payment_status)].shape[0], 2)
        # output_df.loc[i, '% Затрат (доля от общего)'] = round(output_df.loc[i, 'Бюджет'] + output_df.loc[i, 'Расход на ОП']/\
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

