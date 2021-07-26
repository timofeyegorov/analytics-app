import numpy as np
import pandas as pd
from collections import defaultdict
from database import *
from urllib import parse
import matplotlib.pyplot as plt
import re


def get_clusters(df):
  with open('filters/default.pkl', 'rb') as f:
    countries, ages, jobs, earnings, trainings, times = pkl.load(f)

  countries_unique = list(df[df['quiz_answers1'].isin(countries)]['quiz_answers1'].unique()) \
                  + list(df[~df['quiz_answers1'].isin(countries)]['quiz_answers1'].unique())
  ages_unique = list(df[df['quiz_answers2'].isin(ages)]['quiz_answers2'].unique()) \
                  + list(df[~df['quiz_answers2'].isin(ages)]['quiz_answers2'].unique())
  jobs_unique = list(df[df['quiz_answers3'].isin(jobs)]['quiz_answers3'].unique()) \
                  + list(df[~df['quiz_answers3'].isin(jobs)]['quiz_answers3'].unique())
  earnings_unique = list(df[df['quiz_answers4'].isin(earnings)]['quiz_answers4'].unique()) \
                  + list(df[~df['quiz_answers4'].isin(earnings)]['quiz_answers4'].unique())
  trainings_unique = list(df[df['quiz_answers5'].isin(trainings)]['quiz_answers5'].unique()) \
                  + list(df[~df['quiz_answers5'].isin(trainings)]['quiz_answers5'].unique())
  times_unique = list(df[df['quiz_answers6'].isin(times)]['quiz_answers6'].unique()) \
                  + list(df[~df['quiz_answers6'].isin(times)]['quiz_answers6'].unique())

  def getParameterCityVect(arg):
    outClass = countries_unique.index(arg)
    return list(np.eye(len(countries_unique))[outClass].astype('int'))

  def getParameterAgeVect(arg):
    outClass = ages_unique.index(arg)
    return list(np.eye(len(ages_unique))[outClass].astype('int'))

  def getParameterJobVect(arg):
    outClass = jobs_unique.index(arg)
    return list(np.eye(len(jobs_unique))[outClass].astype('int'))

  def getParameterEarningsVect(arg):
    outClass = earnings_unique.index(arg)
    return list(np.eye(len(earnings_unique))[outClass].astype('int'))

  def getParameterTrainingVect(arg):
    outClass = trainings_unique.index(arg)
    return list(np.eye(len(trainings_unique))[outClass].astype('int'))

  def getParameterTimeVect(arg):
    outClass = times_unique.index(arg)
    return list(np.eye(len(times_unique))[outClass].astype('int'))

  def getAllParameters(val):
    city = getParameterCityVect(val[2])             
    age = getParameterAgeVect(val[3])             
    job = getParameterJobVect(val[4])         
    earning = getParameterEarningsVect(val[5])             
    training = getParameterTrainingVect(val[6]) 
    times = getParameterTimeVect(val[7])   

    out = []
    out += city
    out += age
    out += job
    out += earning
    out += training
    out += times
    return out

  def get01Data(values):
    xTrain = []                   # Создаем пустой xTrain

    for val in values:            # Пробегаем по всем записям базы
        x = getAllParameters(val) # Получаем полный набор данных о текущей записи val
        xTrain.append(x)          # Добавляем полученные данные в xTrain

    xTrain = np.array(xTrain)     # Переводим в numpy
    return xTrain

  countries_end = len(countries)

  ages_start = len(countries_unique)
  ages_end = len(countries_unique) + len(ages)

  jobs_start = len(countries_unique) + len(ages_unique)
  jobs_end = len(countries_unique) + len(ages_unique) + len(jobs)

  earnings_start = len(countries_unique) + len(ages_unique) + len(jobs_unique)
  earnings_end = len(countries_unique) + len(ages_unique) + len(jobs_unique) + len(earnings)

  trainings_start = len(countries_unique) + len(ages_unique) + len(jobs_unique) + len(earnings_unique)
  trainings_end = len(countries_unique) + len(ages_unique) + len(jobs_unique) + len(earnings_unique) + len(trainings)

  times_start = len(countries_unique) + len(ages_unique) + len(jobs_unique) + len(earnings_unique) + len(trainings_unique)
  times_end = len(countries_unique) + len(ages_unique) + len(jobs_unique) + len(earnings_unique) + len(trainings_unique) + len(times)

  def addLabels(xTrain01):
    xTrain01 = xTrain01.tolist()

    for i in range(len(xTrain01)):
      category = 0
      if sum(xTrain01[i][:countries_end]) == 1:
        category += 1
      if sum(xTrain01[i][ages_start:ages_end]) == 1:
        category += 1
      if sum(xTrain01[i][jobs_start:jobs_end]) == 1:
        category += 1
      if sum(xTrain01[i][earnings_start:earnings_end]) == 1:
        category += 1
      if sum(xTrain01[i][trainings_start:trainings_end]) == 1:
        category += 1
      if sum(xTrain01[i][times_start:times_end]) == 1:
        category += 1

      category_2 = 0
      if xTrain01[i][trainings_start] == 1:
        category_2 = 1
      if xTrain01[i][trainings_start + 1] == 1:
        category_2 = 2
      if xTrain01[i][trainings_start + 2] == 1:
        category_2 = 3
      

      xTrain01[i].append(category)  
      xTrain01[i].append(category_2)
    xTrain01 = np.array(xTrain01)
    return xTrain01

  # Функция для вставки строки в фрейм данных
  def Insert_row(row_number, df, row_value):
      start_upper = 0 # Начальное значение верхней половины
      end_upper = row_number # Конечное значение верхней половины
      start_lower = row_number # Начальное значение нижней половины
      end_lower = df.shape[0] # Конечное значение нижней половины
      upper_half = [*range(start_upper, end_upper, 1)] # Создать список индекса upper_half
      lower_half = [*range(start_lower, end_lower, 1)] # Создать список индекса lower_half
      lower_half = [x.__add__(1) for x in lower_half] # Увеличить значение нижней половины на 1
      index_ = upper_half + lower_half # Объединить два списка
      df.index = index_ # Обновить индекс данных
      df.loc[row_number] = row_value # Вставить строку в конце
      df = df.sort_index() # Сортировать метки индекса
      return df

  all_categories = countries_unique +\
                  ages_unique +\
                  jobs_unique +\
                  earnings_unique +\
                  trainings_unique +\
                  times_unique

  all_categories_len = len(countries_unique +\
                          ages_unique +\
                          jobs_unique +\
                          earnings_unique +\
                          trainings_unique +\
                          times_unique)

  def clusterDataFrame(x):
    if xTrain01[xTrain01[:, all_categories_len]==x].shape[0] != 0:
      mX = np.mean(xTrain01[xTrain01[:, all_categories_len]==x], axis=0)  # Считаем среднее значение по кластеру
      sX = np.sum(xTrain01[xTrain01[:, all_categories_len]==x], axis=0)  # Считаем сумму значение по кластеру
      mAll = np.mean(xTrain01, axis=0) # Считаем среднее значение по базе
      sAll = np.sum(xTrain01, axis=0) # Считаем сумму значение по базе

      df_data = np.zeros((all_categories_len,6))
      cluster_columns = ['Сегмент', 'Процент в кластере', 'Процент в базе', 'Процент кластер/база', 'Кол-во в кластере', 'Кол-во в базе']
      df = pd.DataFrame(df_data, columns = cluster_columns)

      for i in range(all_categories_len):
        # try:
          df.iloc[i,0] = all_categories[i]
          df.iloc[i,1] = int(round(100*mX[i]))
          df.iloc[i,2] = int(round(100*mAll[i]))
          if (mAll[i]*100 >= 1) & (mX[i]*100 >= 1):
            df.iloc[i,3] = int(round(mX[i]/mAll[i]*100))
          else:
            df.iloc[i,3] = 0  
          df.iloc[i,4] = int(sX[i])
          df.iloc[i,5] = int(sAll[i])
        # except ValueError:  
        #   if np.isnan(mX[i]) == True:
        #       mX[i] = 0
        #   df.iloc[i,0] = temp_list[i]
        #   df.iloc[i,1] = int(round(100*mX[i]))
        #   df.iloc[i,2] = int(round(100*mAll[i]))
        #   if mAll[i] != 0:
        #     df.iloc[i,3] = int(round(100*mX[i]/100*mAll[i])*100)
        #   else:
        #     df.iloc[i,3] = 0  
        #   df.iloc[i,4] = int(sX[i])
        #   df.iloc[i,5] = int(sAll[i])
        
      df = Insert_row(0, df, ["Страна", '-', '-','-','-','-'])
      df = Insert_row(ages_start+1, df, ["Возраст", '-', '-','-','-','-'])
      df = Insert_row(jobs_start+2, df, ["Профессия", '-', '-','-','-','-'])
      df = Insert_row(earnings_start+3, df, ["Доход", '-', '-','-','-','-'])
      df = Insert_row(trainings_start+4, df, ["Обучение", '-', '-','-','-','-'])
      df = Insert_row(times_start+5, df, ["Время", '-', '-','-','-','-'])  

      return df
    else:
      return None

  def clusterDataFrame_education(x):
    mX = np.mean(xTrain01[xTrain01[:, all_categories_len+1]==x], axis=0)  # Считаем среднее значение по кластеру
    sX = np.sum(xTrain01[xTrain01[:, all_categories_len+1]==x], axis=0)  # Считаем сумму значение по кластеру
    mAll = np.mean(xTrain01, axis=0) # Считаем среднее значение по базе
    sAll = np.sum(xTrain01, axis=0) # Считаем сумму значение по базе

    df_data = np.zeros((all_categories_len,6))
    cluster_columns = ['Сегмент', 'Процент в кластере', 'Процент в базе', 'Процент кластер/база', 'Кол-во в кластере', 'Кол-во в базе']
    df = pd.DataFrame(df_data, columns = cluster_columns)

    for i in range(all_categories_len):
      df.iloc[i,0] = all_categories[i]
      df.iloc[i,1] = int(round(100*mX[i]))
      df.iloc[i,2] = int(round(100*mAll[i]))
      if mAll[i] != 0:
        df.iloc[i,3] = int(round(100*mX[i]/100*mAll[i])*100)
      else:
        df.iloc[i,3] = 0
      df.iloc[i,4] = int(sX[i])
      df.iloc[i,5] = int(sAll[i])
      
    df = Insert_row(0, df, ["Страна", '-', '-','-','-','-'])
    df = Insert_row(ages_start+1, df, ["Возраст", '-', '-','-','-','-'])
    df = Insert_row(jobs_start+2, df, ["Профессия", '-', '-','-','-','-'])
    df = Insert_row(earnings_start+3, df, ["Доход", '-', '-','-','-','-'])
    df = Insert_row(trainings_start+4, df, ["Обучение", '-', '-','-','-','-'])
    df = Insert_row(times_start+5, df, ["Время", '-', '-','-','-','-'])  

    return df

  xTrain01 = get01Data(df.values) # Создаем обучающую выборку по данным из базы
  xTrain01 = addLabels(xTrain01)               # Добавляем метки кластера

  res_dict = {}
  for i in range(7):
    num = i

    cluster_ = clusterDataFrame(num)
    # print(f'\033[1m{num} попаданий в ЦА\033[0m')
    # print()
    # print("Размер кластера:", xTrain01[xTrain01[:, all_categories_len]==num].shape[0]) # Выведем количество элементов в кластере
    # display(cluster_)

    res_dict.update({f'{num} попаданий в ЦА': {'Датасет': cluster_, 'Размер кластера': xTrain01[xTrain01[:, all_categories_len]==num].shape[0]}})
  return res_dict

# def get_segments(df, ta_filter):
  # df.to_csv('table.csv', index=False)
def get_segments(df, ta_filter):
  with open('filters/' + ta_filter['pickle'], 'rb') as f:
    countries, ages, jobs, earnings, trainings, times = pkl.load(f)
  traff_data = get_accounts()
  # Список подкатегорий по каждой категории в попадание которых лид считаеся целевым
  # В готовых отчетах делаем сортировку так, чтобы сначала были показатели по ЦА подкатегориям, потом остальные
  try:
    df.insert(19, 'trafficologist', 'Неизвестно')                # Добавляем столбец trafficologist для записи имени трафиколога
    df.insert(20, 'account', 'Неизвестно 1')                       # Добавляем столбец account для записи аккаунта трафиколога
    df.insert(21, 'target_class', 0)  
  except ValueError:
    pass

  # Анализируем ссылки каждого лида на то, какой трафиколог привел этого лида
  links_list = [] # Сохраняем в список ссылки, не содержащие метки аккаунтов (в таком случае неизвестно, кто привел лида)
  for el in list(traff_data['label']): # Проходимся по всем метка которые есть
    for i in range(df.shape[0]): # Проходим по всему датасету
      try: # Пробуем проверить, есть ли элемент в ссылке
        if el in df.loc[i, 'traffic_channel']: # Если элемент (метка) есть
          df.loc[i, 'trafficologist'] = traff_data[traff_data['label'] == el]['name'].values[0] # Заносим имя трафиколога по в ячейку по значению метки
          df.loc[i, 'account'] = traff_data[traff_data['label'] == el]['title'].values[0] # Заносим кабинет трафиколога по в ячейку по значению метки
      except TypeError: # Если в ячейке нет ссылки, а проставлен 0
        links_list.append(df.loc[i, 'traffic_channel'])

  for i in range(df.shape[0]):
    target_class = 0
    if df.loc[i, 'quiz_answers1'] in countries:
      target_class += 1
    if df.loc[i, 'quiz_answers2'] in ages:
      target_class += 1
    if df.loc[i, 'quiz_answers3'] in jobs:
      target_class += 1
    if df.loc[i, 'quiz_answers4'] in earnings:
      target_class += 1         
    if df.loc[i, 'quiz_answers5'] in trainings:
      target_class += 1
    if df.loc[i, 'quiz_answers6'] in times:
      target_class += 1

    df.loc[i, 'target_class'] = target_class

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

  # Функция формирования двух датасетов с количеством лидов и процентным соотношением
  # по выбранной категории с разбивкой по трафикологам и их кабинетам

  def channels_table(df, category, column_name):
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
    data_category = np.zeros((len(df[column_name].unique())+1, len(created_columns))).astype('int')
    df_category = pd.DataFrame(data_category, columns=created_columns).copy() # Датасет для процентного кол-ва лидов
    df_category.insert(0, 'Все', 0) # Столбец для подсчета всех лидов (сумма по всем таргетологам)
    df_category.insert(0, column_name, 0) # Столбец названия подкатегорий указанной категории

    df_category_val = pd.DataFrame(data_category, columns=created_columns).copy()  # Датасет для абсолютного кол-ва лидов
    df_category_val.insert(0, 'Все', 0) # Столбец для подсчета всех лидов (сумма по всем таргетологам)
    df_category_val.insert(0, column_name, 0) # Столбец названия подкатегорий указанной категории

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
      subcategories = df[df[column_name].isin(category)][column_name].unique().tolist()\
                  + df[~df[column_name].isin(category)][column_name].unique().tolist()

    for idx, subcategory_name in enumerate(subcategories):
      df_category_val.iloc[idx, 0] = subcategory_name 
      df_category_val.iloc[idx, 1] = df[df[column_name] == subcategory_name].shape[0]

      df_category.iloc[idx, 0] = subcategory_name
      df_category.iloc[idx, 1] = int(round((df[df[column_name] == subcategory_name].shape[0]/df.shape[0])*100, 0))

      # Проходим в цикле по каждому таргетологу и его кабинету
      for traff_name in created_columns:
        # df_category_val.loc[idx, traff_name] = 0
        df_category_val.loc[idx, traff_name] = df[(df[column_name] == subcategory_name) & ((df['trafficologist'] == traff_name) | (df['account'] == traff_name))].shape[0]
        # df_category.loc[idx, traff_name] = 1
        df_category.loc[idx, traff_name] = round((df_category_val.loc[idx, traff_name]/df[(df['trafficologist'] == traff_name) | (df['account'] == traff_name)].shape[0])*100, 0).astype('int')

    df_category_val.loc[idx+1, column_name] = 'ЦА'
    df_category.loc[idx+1, column_name] = 'ЦА'

    # По каждой колонке считаем кол-во лидов попадающих в ЦА
    for i in range(1, df_category_val.shape[1]):
      df_category_val.iloc[idx+1, i] = df_category_val[df_category_val[column_name].isin(category)].iloc[:,i].sum()
      df_category.iloc[idx+1, i] = df_category[df_category[column_name].isin(category)].iloc[:,i].sum() 

    return df_category_val, df_category

  # Функция формирования двух датасетов по ЦА
  def target_audience_channels_table(df):

    # Формируем заготовки под результирующие датасеты из нулей
    data_ta = np.zeros((8, len(created_columns))).astype('int')
    df_ta = pd.DataFrame(data_ta, columns=created_columns)
    df_ta_val = pd.DataFrame(data_ta, columns=created_columns)

    df_ta_val.insert(0, 'Все', 0)
    df_ta.insert(0, 'Все', 0)

    # Проходим по всем количествам попаданий в ЦА (от 0 до 6)
    for i in range(7):
      df_ta_val.loc[i, 'Все'] = df[df['target_class'] == i].shape[0]
      df_ta.loc[i, 'Все'] = int(round(df[df['target_class'] == i].shape[0]/df.shape[0]*100, 0))

      df_ta_val.loc[7, 'Все'] = df_ta_val.loc[5:6, 'Все'].sum()
      df_ta.loc[7, 'Все'] = df_ta.loc[5:6, 'Все'].sum()

    # Проходим по всем трафикологам и их кабинетам
    for traff_name in created_columns:
      for t_class in range(7): # Проходим по всем количествам попаданий в ЦА (от 0 до 6)
        df_ta_val.loc[t_class, traff_name] = df[(df['target_class'] == t_class) & ((df['trafficologist'] == traff_name) | (df['account'] == traff_name))].shape[0]
        df_ta.loc[t_class, traff_name] = round(df_ta_val.loc[t_class, traff_name]/(df[(df['trafficologist'] == traff_name) | (df['account'] == traff_name)].shape[0])*100, 0).astype('int')

      df_ta_val.loc[7, traff_name] = df_ta_val.loc[5:6, traff_name].sum()
      df_ta.loc[7, traff_name] = df_ta.loc[5:6, traff_name].sum()

    df_ta_val.rename(index={7: '5-6'}, inplace=True) 
    df_ta.rename(index={7: '5-6'}, inplace=True) 
    return df_ta_val, df_ta

  # Функция формирования датасетов по ЦА
  def target_consolidated(df):

    # Формируем заготовки под результирующие датасеты из нулей
    data_segment = np.zeros((6, len(created_columns))).astype('int')

    df_segment = pd.DataFrame(data_segment, columns=created_columns)
    df_segment.insert(0, 'Все', 0)
    df_segment.insert(0, 'Сегмент', 0)

    df_segment_val = pd.DataFrame(data_segment, columns=created_columns)
    df_segment_val.insert(0, 'Все', 0)
    df_segment_val.insert(0, 'Сегмент', 0)

    categories = [countries, ages, jobs, earnings, trainings, times] # Список категорий, где каждая переменная - это список подкатегорий

    for idx, column_category in enumerate(zip(list(df.columns.values[2:8]),categories)):
      df_segment_val.loc[idx, 'Сегмент'] = column_category[0]
      df_segment_val.loc[idx, 'Все'] = df[df[column_category[0]].isin(column_category[1])].shape[0]

      df_segment.loc[idx, 'Сегмент'] = column_category[0]
      df_segment.loc[idx, 'Все'] = round(df[df[column_category[0]].isin(column_category[1])].shape[0]/df.shape[0]*100, 0)

      for traff_name in created_columns:
        df_segment_val.loc[idx, traff_name] = df[df[column_category[0]].isin(column_category[1]) & ((df['trafficologist']==traff_name) | (df['account']==traff_name))].shape[0]
        df_segment.loc[idx, traff_name] = round(df[df[column_category[0]].isin(column_category[1]) & ((df['trafficologist']==traff_name) | (df['account']==traff_name))].shape[0]/\
                                          df[(df['trafficologist']==traff_name) | (df['account']==traff_name)].shape[0]*100, 0)

    return df_segment_val, df_segment

  df_countries_val, df_countries = channels_table(df = df, category = countries, column_name = df.columns[2])
  df_ages_val, df_ages = channels_table(df = df, category = ages, column_name = df.columns[3])
  df_jobs_val, df_jobs = channels_table(df = df, category = jobs, column_name = df.columns[4])
  df_earnings_val, df_earnings = channels_table(df = df, category = earnings, column_name = df.columns[5])
  df_trainings_val, df_trainings = channels_table(df = df, category = trainings, column_name = df.columns[6])
  df_times_val, df_times = channels_table(df = df, category = times, column_name = df.columns[7])
  df_target_audience_val, df_target_audience = target_audience_channels_table(df)
  df_segment_val, df_segment = target_consolidated(df)



  return {'Страны, абсолютные значения': df_countries_val, 'Страны, относительные значения': df_countries,
          'Возраст, абсолютные значения': df_ages_val, 'Возраст, относительные значения': df_ages,
          'Профессия, абсолютные значения': df_jobs_val, 'Профессия, относительные значения': df_jobs,
          'Заработок, абсолютные значения': df_earnings_val, 'Заработок, относительные значения': df_earnings,
          'Обучение, абсолютные значения': df_trainings_val, 'Обучение, относительные значения': df_trainings,
          'Время, абсолютные значения': df_times_val, 'Время, относительные значения': df_times,
          'Попадание в ЦА, абсолютные значения': df_target_audience_val, 'Попадание в ЦА, относительные значения': df_target_audience,
          'Попадание в ЦА по категориям, абсолютные значения': df_segment_val, 'Попадание в ЦА по категориям, относительные значения': df_segment}

# Функция подсчета показателей по подкатегориям каждой категории и лендингам
def get_turnover(df, ta_filter):
  datasets_dict = {}

  traff_data = get_accounts()
  with open('filters/' + ta_filter['pickle'], 'rb') as f:
    countries, ages, jobs, earnings, trainings, times = pkl.load(f)

  for i in range(df.shape[0]):
    df.loc[i, 'traffic_channel'] = df.loc[i, 'traffic_channel'].split('?')[0]

  landings = df['traffic_channel'].unique().tolist()
  # Создаем список с названием категорий (каждая категория - это список подкатегорий)
  categories = [countries, ages, jobs, earnings, trainings, times]

  for i in range(df.shape[0]):
    target_class = 0
    if df.loc[i, 'quiz_answers1'] in countries:
      target_class += 1
    if df.loc[i, 'quiz_answers2'] in ages:
      target_class += 1
    if df.loc[i, 'quiz_answers3'] in jobs:
      target_class += 1
    if df.loc[i, 'quiz_answers4'] in earnings:
      target_class += 1         
    if df.loc[i, 'quiz_answers5'] in trainings:
      target_class += 1
    if df.loc[i, 'quiz_answers6'] in times:
      target_class += 1
    df.loc[i, 'target_class'] = target_class

  def turnover_in(column_name, category):
    flag = True # Если считаем показатели по категориям
    if category not in categories:
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
      subcategories = df[df[column_name].isin(category)][column_name].unique().tolist()\
                  + df[~df[column_name].isin(category)][column_name].unique().tolist()

    # Индекс кол-ва ЦА подкатегорий текущей категории, которые есть в таблице
    targ_idx = len(df[df[column_name].isin(category)][column_name].unique().tolist())
    # Создаем заготовку из нулей под результирующую таблицу
    data_turnover = np.zeros((len(subcategories)+2, 7)).astype('int') if flag else np.zeros((len(subcategories), 7)).astype('int')
    columns_turnover = ['Сегмент',  'Анкет',  'Оборот', 'Оборот на анкету', 'Оплат',  'CV', 'Чек']
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
      # else:
      #   df_turnover.iloc[i+1,1] = 0
      #   df_turnover.iloc[i+1,2] = 0
      #   df_turnover.iloc[i+1,3] = 0
      #   df_turnover.iloc[i+1,4] = 0
      #   df_turnover.iloc[i+1,5] = 0
      #   df_turnover.iloc[i+1,6] = 0

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
      columns_turnover = ['Сегмент',  '% Анкет',  '% Оборот', '% Оплат']
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


      if df_turnover.iloc[-1,3] != 0:
        mnozhitel = round(df_turnover.iloc[-2,3]/df_turnover.iloc[-1,3],1)  
      else:
        mnozhitel = 0  

      return [df_turnover, df_turnover_ca, mnozhitel]
    else: 
      return df_turnover

  countries_df = turnover_in('quiz_answers1', countries)
  ages_df = turnover_in('quiz_answers2', ages)
  jobs_df = turnover_in('quiz_answers3', jobs)
  earnings_df = turnover_in('quiz_answers4', earnings)
  trainings_df = turnover_in('quiz_answers5', trainings)
  times_df = turnover_in('quiz_answers6', times)
  landings_df = turnover_in('traffic_channel', landings)
  

  data_ta = np.zeros((8, 2)).astype('int')
  df_ta = pd.DataFrame(data_ta, columns=['Абс', '%'])

  for i in range(7):
    df_ta.loc[i, 'Абс'] = df[df['target_class'] == i].shape[0]
    df_ta.loc[i, '%'] = int(round(df[df['target_class'] == i].shape[0]/df.shape[0]*100, 0))

    df_ta.loc[7, 'Абс'] = df_ta.loc[5:6, 'Абс'].sum()

    df_ta.loc[7, '%'] = df_ta.loc[5:6, '%'].sum()


  df_ta.rename(index={7: '5-6'}, inplace=True) 

  return {'quiz_answers1':countries_df,
          'quiz_answers2':ages_df,
          'quiz_answers3':jobs_df,
          'quiz_answers4':earnings_df,
          'quiz_answers5':trainings_df,
          'quiz_answers6':times_df}, {'traffic_channel': landings_df}, {'ca': df_ta}

def get_traffic_sources(df):
    result = dict()

    traficologist_data = get_accounts()

    def utm_replace(line):
        return line.replace('utm_source=', '')

    traficologist_data['label'] = traficologist_data['label'].apply(utm_replace)

    df_drop = df.drop(df.columns[[0, 10, 11, 12, 18, 19, 21, 22]], axis=1)

    def url2acc(df):
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
        if temp.shape [0] != 1:
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

    def dupl_count(df):
        count = []
        per = []
        filt_data = df_drop[df_drop['Трафиколог'] == df.values[0]]
        #     df['Доля лидов'] = round(traff_table[traff_table['Трафиколог'] == df.values[0]]['Количество лидов'].sum() / sum(traff_table['Количество лидов'])*100, 2)
        dif = round(filt_data[filt_data['is_double'] == 'yes'].shape[0] / filt_data.shape[0] * 100, 2)
        df['Количество дублей'] = filt_data[filt_data['is_double'] == 'yes'].shape[0]
        df['% дублей'] = round(dif, 2)
        df['% в работе'] = round(filt_data[filt_data['status_amo'].isin(status_df[~status_df['В работе'].isna()]['Статус'].unique().tolist())].shape[0] / filt_data.shape[0] * 100, 2)
        df['% дозвонов'] = round(filt_data[filt_data['status_amo'].isin(status_df[~status_df['Дозвон'].isna()]['Статус'].unique().tolist())].shape[0] / filt_data.shape[0] * 100, 2)
        df['% назначеных'] = round(filt_data[filt_data['status_amo'].isin(status_df[~status_df['Назначен zoom'].isna()]['Статус'].unique().tolist())].shape[0] / filt_data.shape[0] * 100, 2)
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
        # df['CPO'] = round(filt_data['payment_amount'].mean() / filt_data[filt_data['payment_amount'] != 0].shape[0], 2)
        df['Доля канала в бюджете'] = round(df['Бюджет'] / filt_data['channel_expense'].sum(), 2)
        
        df['Доля канала в обороте'] = round(df['Оборот'] / filt_data['payment_amount'].sum(), 2) if filt_data['payment_amount'].sum() != 0 else 0
        df['Оборот на лида/анкету'] = round(df['Оборот'] / df['Количество лидов'], 2)
        df['% лидов/анкет (доля от общего)'] = round(
            traff_table[traff_table['Трафиколог'] == df.values[0]]['Количество лидов'].sum() / sum(
                traff_table['Количество лидов']) * 100, 2)
        df['% Оборот  (доля от общего)'] = round(df['Оборот'] / df_drop['payment_amount'].sum(), 2)
        df['% Оплат (доля от общего)'] = round(df['Бюджет'] / df_drop['channel_expense'].sum(), 2)
        df['% Затрат (доля от общего)'] = round((df['Бюджет'] + df['Расход на ОП']) / df_drop['channel_expense'].sum(),
                                                2)
        df['Расход общий (ОП+бюджет)'] = df['Бюджет'] + df['Расход на ОП']
        df['% расходов на трафик (доля от Расход общий)'] = round(df['Бюджет'] / (df['Бюджет'] + df['Расход на ОП']), 2)
        df['% расходов на ОП (доля от Расход общий)'] = round(df['Расход на ОП'] / (df['Бюджет'] + df['Расход на ОП']),
                                                              2)

        return df

    traff_table1 = traff_table.apply(dupl_count, axis=1)
    for i in range(11, 20):
        traff_table1[traff_table1.columns[i]] = round(
            traff_table1[traff_table1.columns[i]] / traff_table1[traff_table1.columns[4]], 2)
    # traff_table1.to_excel('analitic.xlsx')
    result['traff_table'] = traff_table1
    return result

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


def get_leads_ta_stats(df):
  account, trafficologist = get_trafficologist_data()

  result = {}
  df_drop = df.drop(df.columns[[0, 10, 11, 12, 17, 18, 19, 21, 22]], axis=1)

  def url2acc_owner(url):
    query = parse.parse_qs(parse.urlparse(url).query)
    if 'utm_source' in query.keys():
      if query['utm_source'][0] in trafficologist.keys():
        return trafficologist[query['utm_source'][0]]
      elif 'rs' in query.keys():
        rs = query['rs'][0].split('_')[0]
        if rs in trafficologist.keys():
          return trafficologist[rs]
      return query['utm_source'][0]
    else:
      if 'rs' in query.keys():
        if query['rs'][0].split('_')[0] in trafficologist.keys():
          return trafficologist[query['rs'][0].split('_')[0]]
        return query['rs'][0].split('_')[0]
      return 'traffic not found'

  def url2account(url):
    query = parse.parse_qs(parse.urlparse(url).query)
    if 'utm_source' in query.keys():
      if query['utm_source'][0] in account.keys():
        return account[query['utm_source'][0]]
      elif 'rs' in query.keys():
        rs = query['rs'][0].split('_')[0]
        if rs in account.keys():
          return account[rs]
      return query['utm_source'][0]
    else:
      if 'rs' in query.keys():
        if query['rs'][0].split('_')[0] in account.keys():
          return account[query['rs'][0].split('_')[0]]
        return query['rs'][0].split('_')[0]
      return 'traffic not found'

  df_drop['acc_owner'] = df_drop['traffic_channel'].apply(url2acc_owner)
  df_drop['account'] = df_drop['traffic_channel'].apply(url2account)
  df_drop['status_amo'] = df_drop['status_amo'].apply(str.lower)
  result['ca_table'] = df_drop
  return result

def get_leads_ta_stats(df):
  status = pd.read_csv('status.csv')
  status.fillna(0, inplace=True)

  with open('filters/default.pkl', 'rb') as f:
    countries, ages, jobs, earnings, trainings, times = pkl.load(f)

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


def get_landings(tabl, ta_filter):
  with open('filters/' + ta_filter['pickle'], 'rb') as f:
    countries, ages, jobs, earnings, trainings, times = pkl.load(f)
  CAlist = [countries, ages, jobs, earnings, trainings, times]

  def go(result):
    for i in result.index:
      if result.loc[i, result.columns[5]] == 'Да, если это поможет мне в реализации проекта.':
        result.loc[i, result.columns[5]] = 'Да, проект'
      if result.loc[i, result.columns[5]] == 'Да, если я точно найду работу после обучения.':
        result.loc[i, result.columns[5]] = 'Да, работа'
      if result.loc[i, result.columns[5]] == 'Нет. Хочу получить только бесплатные материалы.':
        result.loc[i, result.columns[5]] = 'Нет' 
      if result.loc[i, result.columns[3]] == 'Профессии связанные с числами (аналитик, бухгалтер, инженер и т.п.)':
        result.loc[i, result.columns[3]] = 'Связано с числами'
      if result.loc[i, result.columns[3]] == 'Гуманитарий (общение с людьми, искусство, медицина и т.д.)':
        result.loc[i, result.columns[3]] = 'Гуманитарий'
      if result.loc[i, result.columns[3]] == 'IT сфера (разработчик, тестировщик, администратор и т.п.)':
        result.loc[i, result.columns[3]] = 'IT сфера'
      result.loc[i, 'quiz_answers1'] = result.loc[i, 'quiz_answers1'].strip()
    return result


  def prepare(listCA, df):
    df = go(df)
    all_dict = {'alias':{},
                'key': {},
                'base': defaultdict(lambda : {'indx': list(),
                                'notCA': list(),
                                'data': defaultdict(list)}),
                'ca': defaultdict(list)}
    alias = all_dict['alias']
    key = all_dict['key']
    summ = all_dict['base']
    CA_dict = all_dict['ca']
    keys = ['Страна', 'Возраст', 'Профессия', 'Доход', 'Обучение', 'Время']
    for i in range(1, 7):
      alias[keys[i-1]] = df.columns[i+1]
    for i in range(6):
      key[keys[i]] = listCA[i]
    df['r2'] = [str(s).split('?')[0] for s in df['traffic_channel']]
    r1all = df['r2'].unique().tolist()
    r1all.insert(0, 'Все')
    all_dict['r1all'] = r1all
    for i in key.keys():
      all = df[alias[i]].unique().tolist()
      notcaAges = list(set(all) - set(key[i]))
      sortall = key[i] + notcaAges
      summ[i]['indx'] = sortall
      summ[i]['notCA'] = notcaAges
      data = summ[i]['data']
      inx = summ[i]['indx']
      data['ЦА'] = [0]*len(r1all)
      for j in inx:
        for num, url in enumerate(r1all[1:]):
          amount = df[(df[alias[i]] == j)&(df['r2'] == url)].shape[0]
          data[j].append(amount)
          if j in key[i]:
            data['ЦА'][num+1] += amount
        data[j].insert(0, sum(data[j]))
      data['ЦА'][0] = sum(data['ЦА'][1:])
    for url in r1all[1:]:
      st = df[df['r2']==url]
      st_col = st.shape[0]
      flag = True
      for ke in list(key.keys())[:5]:
        st = st[st[alias[ke]].isin(key[ke])]
        st_new = st.shape[0]
        st_col = st_col - st_new
        if flag:
          CA_dict[url].append(st_col)
          flag = False
        CA_dict[url].append(st_new)
        st_col = st_new
    return all_dict

  def color_ca(s):
      is_mos = s.index == 'ЦА'
      return ['color: red; text-align: center' if v else 'color: black; text-align: center' for v in is_mos]

  def landing(key, table, absolut=False):
    work = table['base'][key]
    df = pd.DataFrame(work['data'])
    df = df.T
    df.columns = table['r1all']
    newind = table['key'][key] + ['ЦА'] + work['notCA']
    df = df.reindex(newind)
    if absolut:
      for i in range(df.shape[0]):
        for j in range(1, df.shape[1]):
          df.iloc[i, j] = round((df.iloc[i, j] / df.iloc[i, 0]) * 100, 2)
      df.fillna(0, inplace=True)
      for i in range(df.shape[0]):
        for j in range(1, df.shape[1]):
          df.iloc[i, j] = str(df.iloc[i, j]) + '%'
      am = df['Все'].sum() - df.loc['ЦА', 'Все']
      df['Все'] = (100*(df['Все']/am)).round(2).astype(str) + '%' 
    return df #.style.apply(color_ca)

  def foCA(basa, table, absolut=False):
    df = pd.DataFrame(basa['ca'], index=['0', '1', '2', '3', '4', '5-6'])
    if absolut:
      for col in df.columns:
        s = table[table['r2'] == col].shape[0]
        for idx in df.index:
          df.loc[idx, col] = round(100*(df.loc[idx, col] / s), 2)
          df.loc[idx, col] = str(df.loc[idx, col]) + '%'
    return df

  def CA(basa, table, absolut=False):
    new = {}
    bas = basa['base']
    for key in bas.keys():
      new[key] = bas[key]['data']['ЦА']
    df = pd.DataFrame(new, index=basa['r1all'])
    df = df.T
    if absolut:
        for col in df.columns:
          s = table.shape[0]
          for idx in df.index:
            df.loc[idx, col] = round(100*(df.loc[idx, col] / s), 2)
            df.loc[idx, col] = str(df.loc[idx, col]) + '%'
    return df



  dd = prepare(df=tabl, listCA=CAlist)
  result = {}
  for key in [*list(dd['key'].keys()), 'Попадание в ЦА', "ЦА по категориям"]:
    for ab in [True, False]:
      s = str(key) + ' в процентах' if ab else key
      if key == 'Попадание в ЦА':
        result[s] = foCA(dd, table=tabl, absolut=ab).T
      elif key == 'ЦА по категориям':
        result[s] = CA(dd, table=tabl, absolut=ab).T
      else:
        result[s] = landing(key, table=dd, absolut=ab).T
  return result