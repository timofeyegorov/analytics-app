import numpy as np
import pandas as pd
from database import *

def get_segments(df):
  # Создаем словарь, где ключи - метки аккаунтов, значения - владельцы аккаунтов
  account, trafficologist = get_trafficologist_data() 

  # Список подкатегорий по каждой категории в попадание которых лид считаеся целевым
  # В готовых отчетах делаем сортировку так, чтобы сначала были показатели по ЦА подкатегориям, потом остальные
  countries = ['Россия', 'Европа', 'Северная Америка', 'Южная Корея', 'Киев', 'Минск', 'Нур-Султан', 'Израиль', 'ОАЭ', 'Латинская Америка', 'Юго-Восточная Азия', 'Австралия']
  ages = ['24 - 30', '26 - 30', '31 - 40', '41 - 50', '51 - 60']
  jobs = ['IT сфера', 'Связано с числами', 'Гуманитарий',
        'Предприниматель, руководитель',
        'Преподаватель, учитель']
  earnings = ['до 30 000 руб.',
              'до 30 000 руб. / до $400',
              'до 60 000 руб.',
              'до 100 000 руб.',
              'более 100 000 руб.',
              'до 60 000 руб. / до $800',
              'до 100 000 руб. / до $1400',
              'более 100 000 руб. / более $1400']
  trainings = ['Да, работа', 'Да, проект']
  times = ['до 5 часов в неделю', 'до 10 часов в неделю', 'более 10 часов в неделю', 'Неизвестно']



  df.insert(19, 'trafficologist', 'Неизвестно')                # Добавляем столбец trafficologist для записи имени трафиколога
  df.insert(20, 'account', 'Неизвестно 1')                       # Добавляем столбец account для записи аккаунта трафиколога
  df.insert(21, 'target_class', 0)  

  links_list = [] # Сохраняем в список ссылки, не содержащие метки аккаунтов
  for el in list(trafficologist.keys()):
    for i in df.index:
      try:
        if el in df.loc[i, 'traffic_channel']:
          df.loc[i, 'trafficologist'] = trafficologist[el]
          df.loc[i, 'account'] = account[el]
      except TypeError:
        links_list.append(df.loc[i, 'traffic_channel'])

  for i in df.index:
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
  # Объединяем массивы уникальных значений в колонках trafficologist и account
  # Оставляем только уникальные значения, переводи массив в список и сортируем
  created_columns = np.unique(np.concatenate((df['trafficologist'].unique(), df['account'].unique()))).tolist()
  created_columns.sort() # Это список таргетологов и их кабинетов


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
    data_category = np.zeros((len(category + df[~df[column_name].isin(category)][column_name].unique().tolist())+1, len(created_columns))).astype('int')

    df_category = pd.DataFrame(data_category, columns=created_columns) # Датасет для процентного кол-ва лидов
    df_category.insert(0, 'Все', 0) # Столбец для подсчета всех лидов (сумма по всем таргетологам)
    df_category.insert(0, column_name, 0) # Столбец названия подкатегорий указанной категории

    df_category_val = pd.DataFrame(data_category, columns=created_columns)  # Датасет для абсолютного кол-ва лидов
    df_category_val.insert(0, 'Все', 0) # Столбец для подсчета всех лидов (сумма по всем таргетологам)
    df_category_val.insert(0, column_name, 0) # Столбец названия подкатегорий указанной категории

    # Проходим в цикле по каждой подкатегории выбранной категории
    # В скобках после enumerate формируется список для прохода сначала по подкатегориям куда попадает ЦА
    for idx, country_name in enumerate(category + df[~df[column_name].isin(category)][column_name].unique().tolist()):
      df_category_val.iloc[idx, 0] = country_name 
      df_category_val.iloc[idx, 1] = df[df[column_name] == country_name].shape[0]

      df_category.iloc[idx, 0] = country_name
      df_category.iloc[idx, 1] = int(round((df[df[column_name] == country_name].shape[0]/df.shape[0])*100, 0))

      # Проходим в цикле по каждому таргетологу и его кабинету
      for traff_name in created_columns:
        df_category_val.loc[idx, traff_name] = df[(df[column_name] == country_name) & ((df['trafficologist'] == traff_name) | (df['account'] == traff_name))].shape[0]
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

def get_clusters(df):
  # Список подкатегорий по каждой категории в попадание которых лид считаеся целевым
  # В готовых отчетах делаем сортировку так, чтобы сначала были показатели по ЦА подкатегориям, потом остальные
  countries = ['Россия', 'Европа', 'Северная Америка', 'Южная Корея', 'Киев', 'Минск', 'Нур-Султан', 'Израиль', 'ОАЭ', 'Латинская Америка', 'Юго-Восточная Азия', 'Австралия']
  ages = ['24 - 30', '26 - 30', '31 - 40', '41 - 50', '51 - 60']
  jobs = ['IT сфера', 'Связано с числами', 'Гуманитарий',
        'Предприниматель, руководитель',
        'Преподаватель, учитель']
  earnings = ['до 30 000 руб.',
              'до 30 000 руб. / до $400',
              'до 60 000 руб.',
              'до 100 000 руб.',
              'более 100 000 руб.',
              'до 60 000 руб. / до $800',
              'до 100 000 руб. / до $1400',
              'более 100 000 руб. / более $1400']
  trainings = ['Да, работа', 'Да, проект']
  times = ['до 5 часов в неделю', 'до 10 часов в неделю', 'более 10 часов в неделю', 'Неизвестно']


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