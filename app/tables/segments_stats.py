from app.database import get_accounts, get_leads_data, get_target_audience

import pandas as pd
from urllib import parse
import pickle as pkl
import numpy as np
import os
from config import RESULTS_FOLDER, DATA_FOLDER

def calculate_segments_stats(df):
  df.reset_index(inplace=True, drop=True)

  with open(os.path.join(RESULTS_FOLDER, 'target_audience.pkl'), 'rb') as f:
    target_audience = pkl.load(f)

  with open(os.path.join(RESULTS_FOLDER, 'statuses.pkl'), 'rb') as f:
    status = pkl.load(f)
  status.fillna(0, inplace=True)

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

  columns = ['Кол-во лидов', '% в работе', '% дозвонов', '% офферов', '% счетов', '% продаж', 'Цена лида',
             '% ЦА 5-6', '% ЦА 5', '% ЦА 6', '% ЦА 4-5-6', '% ЦА 4', '% не ЦА', '% ЦА 3', '% ЦА 2', '% ЦА 1',
             'Цена ЦА 5-6', 'Цена ЦА 4-5-6',
             'Средний чек', 'Оборот',
             'Бюджет', 'Расход на ОП', 'Расход общий (ОП+бюджет)',
             '% расходов на трафик (доля от Расход общий)', '% расходов на ОП (доля от Расход общий)',
             'ROI ?', 'ROMI', 'ROSI', 'ROI', 'Маржа', 'Маржа %', 'Цена Разговора', 'Цена Оффера', 'Цена Счета',
             'Оборот на лида (на обработанного лида)', 'Оборот на разговор', 'CV обр.лид/оплата',
             'CV разговор/оплата', 'CPO', '% лидов (доля от общего)', '% Оборот (доля от общего)',
             '% Оплат (доля от общего)', '% Затрат (доля от общего)'
             ]
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
    flag = False
    if column_name == 'Сегменты':
        flag = True
        # Создаем заготовки результирующих датасетов из нулей
        data_category = np.zeros((6, len(columns))).astype('int')
        output_df = pd.DataFrame(data_category, columns=columns) # Датасет для процентного кол-ва лидов
        output_df.insert(0, column_name, 0) # Столбец названия подкатегорий указанной категории
        subcategories = df.columns[2:8]
        segments_names = {'quiz_answers1': 'Страны', 'quiz_answers2': 'Возраст', 'quiz_answers3': 'Профессия',
                          'quiz_answers4': 'Доход', 'quiz_answers5': 'Обучение', 'quiz_answers6': 'Время'}

    else:
        # Создаем заготовки результирующих датасетов из нулей
        data_category = np.zeros((len(df[column_name].unique()), len(columns))).astype('int')
        output_df = pd.DataFrame(data_category, columns=columns) # Датасет для процентного кол-ва лидов
        output_df.insert(0, column_name, 0) # Столбец названия подкатегорий указанной категории


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
      if flag:
        output_df.loc[i, column_name] = segments_names[subcategory_name]
        temp_df = df[df[subcategory_name].isin(target_audience)]
      else:
        output_df.loc[i, column_name] = subcategory_name
        temp_df = df[df[column_name] == subcategory_name]
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

        output_df.loc[i, '% ЦА 5-6'] = round(
            temp_df[temp_df['target_class'].isin([5, 6])].shape[0] / temp_df.shape[0] * 100, 1)
        output_df.loc[i, '% ЦА 5'] = round(
            temp_df[temp_df['target_class'] == 5].shape[0] / temp_df.shape[0] * 100, 1)
        output_df.loc[i, '% ЦА 6'] = round(
            temp_df[temp_df['target_class'] == 6].shape[0] / temp_df.shape[0] * 100, 1)
        output_df.loc[i, '% ЦА 4-5-6'] = round(
            temp_df[temp_df['target_class'].isin([4, 5, 6])].shape[0] / temp_df.shape[0] * 100, 1)
        output_df.loc[i, '% ЦА 4'] = round(
            temp_df[temp_df['target_class'] == 4].shape[0] / temp_df.shape[0] * 100, 1)
        output_df.loc[i, '% не ЦА'] = round(
            temp_df[temp_df['target_class'] == 0].shape[0] / temp_df.shape[0] * 100, 1)
        output_df.loc[i, '% ЦА 3'] = round(
            temp_df[temp_df['target_class'] == 3].shape[0] / temp_df.shape[0] * 100, 1)
        output_df.loc[i, '% ЦА 2'] = round(
            temp_df[temp_df['target_class'] == 2].shape[0] / temp_df.shape[0] * 100, 1)
        output_df.loc[i, '% ЦА 1'] = round(
            temp_df[temp_df['target_class'] == 1].shape[0] / temp_df.shape[0] * 100, 1)

        output_df.loc[i, 'Цена ЦА 5-6'] = round(temp_df[temp_df['target_class'].isin([5, 6])]['channel_expense'].sum() / \
                                          temp_df[temp_df['target_class'].isin([5, 6])].shape[0], 1) if \
                                          temp_df[temp_df['target_class'].isin([5, 6])].shape[0] != 0 else 0
        output_df.loc[i, 'Цена ЦА 4-5-6'] = round(temp_df[temp_df['target_class'].isin([4, 5, 6])][
                                                'channel_expense'].sum() / \
                                            temp_df[temp_df['target_class'].isin([4, 5, 6])].shape[0], 1) if \
                                            temp_df[temp_df['target_class'].isin([4, 5, 6])].shape[0] != 0 else 0

        output_df.loc[i, 'Цена лида'] = round(temp_df['channel_expense'].sum() / temp_df.shape[0], 1)
        output_df.loc[i, 'Средний чек'] = round(temp_df['payment_amount'].sum() \
                                                / temp_df[temp_df['payment_amount'] != 0].shape[0], 1) \
          if temp_df[temp_df['payment_amount'] != 0].shape[0] != 0 else 0
        output_df.loc[i, 'Оборот'] = round(float(temp_df['payment_amount'].sum()), 1)
        output_df.loc[i, 'Бюджет'] = round(float(temp_df['channel_expense'].sum()), 1)
        output_df.loc[i, 'Расход на ОП'] = round(float(output_df.loc[i, 'Оборот']) * 0.06
                                                 + 150 * float(temp_df[temp_df['payment_amount'] != 0].shape[0])
                                                 + 27 * float(temp_df[temp_df['payment_amount'] != 0].shape[0])
                                                 + 20 * float(temp_df[temp_df['payment_amount'] != 0].shape[0]) * 1.4,
                                                 1)

        output_df.loc[i, 'Расход общий (ОП+бюджет)'] = float(output_df.loc[i, 'Бюджет']) \
                                                       + float(output_df.loc[i, 'Расход на ОП'])

        output_df.loc[i, '% расходов на трафик (доля от Расход общий)'] = \
          round(float(output_df.loc[i, 'Бюджет']) / output_df.loc[i, 'Расход общий (ОП+бюджет)'] * 100, 1) \
            if output_df.loc[i, 'Расход общий (ОП+бюджет)'] != 0 else 0

        output_df.loc[i, '% расходов на ОП (доля от Расход общий)'] = \
          round(output_df.loc[i, 'Расход на ОП'] / output_df.loc[i, 'Расход общий (ОП+бюджет)'] * 100, 1) \
            if output_df.loc[i, 'Расход общий (ОП+бюджет)'] != 0 else 0

        output_df.loc[i, 'ROI ?'] = round((float(temp_df['payment_amount'].sum())
                                           - float(temp_df['channel_expense'].sum()))
                                          / float(temp_df['channel_expense'].sum()) * 100, 1) \
          if temp_df['channel_expense'].sum() != 0 else 0

        output_df.loc[i, 'ROMI'] = round((output_df.loc[i, 'Оборот'] / output_df.loc[i, 'Бюджет']) - 1, 1)
        output_df.loc[i, 'ROSI'] = round((output_df.loc[i, 'Оборот'] / output_df.loc[i, 'Расход на ОП']) - 1, 1) \
          if output_df.loc[i, 'Расход на ОП'] != 0 else -1
        output_df.loc[i, 'ROI'] = round((output_df.loc[i, 'Оборот'] / output_df.loc[i, 'Расход общий (ОП+бюджет)']) - 1,
                                        1) \
          if output_df.loc[i, 'Расход общий (ОП+бюджет)'] != 0 else -1

        output_df.loc[i, 'Маржа'] = float(temp_df['payment_amount'].sum()) \
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

        output_df.loc[i, 'Оборот на лида (на обработанного лида)'] = round(float(output_df.loc[i, 'Оборот']) / \
                                                                           finished_leads, 1) \
          if finished_leads != 0 else 0

        output_df.loc[i, 'Оборот на разговор'] = round(float(output_df.loc[i, 'Оборот']) / \
                                                       temp_df[temp_df['status_amo'].isin(conversation_status)].shape[
                                                         0], 1) \
          if temp_df[temp_df['status_amo'].isin(conversation_status)].shape[0] != 0 else 0

        output_df.loc[i, 'CV обр.лид/оплата'] = round(finished_leads \
                                                      / temp_df[temp_df['payment_amount'] != 0].shape[0], 1) \
          if temp_df[temp_df['payment_amount'] != 0].shape[0] != 0 else 0

        output_df.loc[i, 'CV разговор/оплата'] = \
          round(temp_df[temp_df['status_amo'].isin(conversation_status)].shape[0] \
                / temp_df[temp_df['payment_amount'] != 0].shape[0], 1) \
            if temp_df[temp_df['payment_amount'] != 0].shape[0] != 0 else 0

        output_df.loc[i, 'CPO'] = round(output_df.loc[i, 'Расход общий (ОП+бюджет)'] \
                                        / temp_df[temp_df['status_amo'].isin(payment_status)].shape[0], 1) \
          if temp_df[temp_df['status_amo'].isin(payment_status)].shape[0] != 0 else 0

        output_df.loc[i, '% лидов (доля от общего)'] = round(temp_df.shape[0] / df.shape[0] * 100, 1)
        output_df.loc[i, '% Оборот (доля от общего)'] = round(float(temp_df['payment_amount'].sum()) \
                                                              / float(df['payment_amount'].sum()), 1) \
          if df['payment_amount'].sum() != 0 else 0
        output_df.loc[i, '% Оплат (доля от общего)'] = \
          round(temp_df[temp_df['status_amo'].isin(payment_status)].shape[0] \
                / df[df['status_amo'].isin(payment_status)].shape[0], 1) \
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

    return output_df

  df_countries = channels_table(df = df, column_name = df.columns[2])
  df_ages = channels_table(df = df, column_name = df.columns[3])
  df_jobs = channels_table(df = df, column_name = df.columns[4])
  df_earnings = channels_table(df = df, column_name = df.columns[5])
  df_trainings = channels_table(df = df, column_name = df.columns[6])
  df_times = channels_table(df = df, column_name = df.columns[7])
  df_segments = channels_table(df=df, column_name='Сегменты')

  return {'Страны, абсолютные значения': df_countries,
          'Возраст, абсолютные значения': df_ages,
          'Профессия, абсолютные значения': df_jobs,
          'Заработок, абсолютные значения': df_earnings,
          'Обучение, абсолютные значения': df_trainings,
          'Время, абсолютные значения': df_times,
          'Сводная': df_segments
          }

