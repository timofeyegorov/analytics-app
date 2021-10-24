from app.database import get_target_audience, get_leads_data
import os
import numpy as np
import pandas as pd
import pickle as pkl
from config import DATA_FOLDER

def calculate_clusters(df):
  df.reset_index(inplace=True, drop=True)
  # target_audience = get_target_audience()
  with open(os.path.join(DATA_FOLDER, 'target_audience.pkl'), 'rb') as f:
    target_audience = pkl.load(f)

  countries_unique = list(df[df['quiz_answers1'].isin(target_audience)]['quiz_answers1'].unique()) \
                  + list(df[~df['quiz_answers1'].isin(target_audience)]['quiz_answers1'].unique())
  ages_unique = list(df[df['quiz_answers2'].isin(target_audience)]['quiz_answers2'].unique()) \
                  + list(df[~df['quiz_answers2'].isin(target_audience)]['quiz_answers2'].unique())
  jobs_unique = list(df[df['quiz_answers3'].isin(target_audience)]['quiz_answers3'].unique()) \
                  + list(df[~df['quiz_answers3'].isin(target_audience)]['quiz_answers3'].unique())
  earnings_unique = list(df[df['quiz_answers4'].isin(target_audience)]['quiz_answers4'].unique()) \
                  + list(df[~df['quiz_answers4'].isin(target_audience)]['quiz_answers4'].unique())
  trainings_unique = list(df[df['quiz_answers5'].isin(target_audience)]['quiz_answers5'].unique()) \
                  + list(df[~df['quiz_answers5'].isin(target_audience)]['quiz_answers5'].unique())
  times_unique = list(df[df['quiz_answers6'].isin(target_audience)]['quiz_answers6'].unique()) \
                  + list(df[~df['quiz_answers6'].isin(target_audience)]['quiz_answers6'].unique())

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


  countries_end = len(list(df[df['quiz_answers1'].isin(target_audience)]['quiz_answers1'].unique()))

  ages_start = len(countries_unique)
  ages_end = len(countries_unique) + len(list(df[df['quiz_answers2'].isin(target_audience)]['quiz_answers2'].unique()))

  jobs_start = len(countries_unique) + len(ages_unique)
  jobs_end = len(countries_unique) + len(ages_unique) + len(list(df[df['quiz_answers3'].isin(target_audience)]['quiz_answers3'].unique()))

  earnings_start = len(countries_unique) + len(ages_unique) + len(jobs_unique)
  earnings_end = len(countries_unique) + len(ages_unique) + len(jobs_unique) + len(list(df[df['quiz_answers4'].isin(target_audience)]['quiz_answers4'].unique()))

  trainings_start = len(countries_unique) + len(ages_unique) + len(jobs_unique) + len(earnings_unique)
  trainings_end = len(countries_unique) + len(ages_unique) + len(jobs_unique) + len(earnings_unique) + len(list(df[df['quiz_answers5'].isin(target_audience)]['quiz_answers5'].unique()))

  times_start = len(countries_unique) + len(ages_unique) + len(jobs_unique) + len(earnings_unique) + len(trainings_unique)
  times_end = len(countries_unique) + len(ages_unique) + len(jobs_unique) + len(earnings_unique) + len(trainings_unique) + len(list(df[df['quiz_answers6'].isin(target_audience)]['quiz_answers6'].unique()))

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

# data = get_leads_data()
# print('get data')
# clusters = get_clusters(data)
# print('calculated clusters')
# with open('results/clusters.pkl', 'wb') as f:
#   pkl.dump(clusters, f)