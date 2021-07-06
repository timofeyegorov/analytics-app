import numpy as np
import pandas as pd
from database import *

# def get_segments(df):

#   # Создаем словарь, где ключи - метки аккаунтов, значения - владельцы аккаунтов
#   trafficologist = {'facebook6': 'Юля', 'facebook3': 'Юля', 'facebook7': 'Юля', 'facebook8': 'Юля', 'facebook10': 'Юля', 'facebook14': 'Юля', 'facebook46': 'Юля', 'facebook47': 'Юля',
#                     'facebook15': 'Ксения', 'facebook16': 'Ксения', 'facebook32': 'Ксения', 'facebook28': 'Ксения',
#                     'facebook19': 'Евгений', 'facebook18': 'Евгений', 'facebook38': 'Евгений',
#                     'facebook24': 'Михаил', 'facebook25': 'Михаил',
#                     'facebook23': 'Яна', 'facebook26': 'Яна',
#                     'facebook30': 'Антон',
#                     'facebook22': 'Демура',
#                     'facebook34': 'УИИ',
#                     'facebook37': 'Валерий', 'facebook23': 'Валерий', 'facebook26': 'Валерий',
#                     'facebook39': 'Артем', 'facebook40': 'Артем',
#                     'facebook17': 'Алина', 'facebook29': 'Алина', 'facebook35': 'Алина', 'facebook36': 'Алина', 'facebook33': 'Алина', 'facebook52': 'Алина', 'facebook51': 'Алина',
#                     'direct1': 'Юля Директ',
#                     'direct11': 'ArrowMedia Директ',
#                     'direct27': 'Директ 2',
#                     'facebook48': 'Zufar',
#                     'facebook49': 'Максим С',
#                     'facebook50': 'Luxury Promo',
#                     'facebook53': 'Resource Traget',
#                     'utm_source=tproger': 'tproger',
#                     'utm_source=itlecture': 'itlecture',
#                     'utm_source=progeri': 'progeri',
#                     'utm_source=secrets_of_programmist': 'secrets_of_programmist',
#                     'utm_source=web_dev_humor': 'web_dev_humor',
#                     'utm_source=itlibr': 'itlibr',
#                     'utm_source=litra_it': 'litra_it',
#                     'utm_source=tipaproit': 'tipaproit',
#                     'utm_source=itneedit': 'itneedit',
#                     'utm_source=bookpython': 'bookpython',
#                     'utm_source=proglb': 'proglb',
#                     'utm_source=python2day': 'python2day',
#                     'utm_source=yaprogrammer': 'yaprogrammer',
#                     'utm_source=js_test': 'js_test',
#                     'utm_source=thehaking': 'thehaking',
#                     'utm_source=python+test': 'python+test',
#                     'utm_source=tgverstka': 'tgverstka',
#                     'utm_source=apple_secret': 'apple_secret',
#                     'utm_source=lullisteacher': 'lullisteacher',
#                     'utm_source=godnotech': 'godnotech',
#                     'utm_source=programmistkas': 'programmistkas',
#                     'utm_source=python_academy': 'python_academy',
#                     'utm_source=u_programmer': 'u_programmer',
#                     'utm_source=campcode': 'campcode',
#                     'utm_source=neuralmachine': 'neuralmachine',
#                     'utm_source=pythonaa': 'pythonaa',
#                     'utm_source=memsdigital': 'memsdigital',
#                     'utm_source=reddit' : 'reddit',
#                     'utm_source=FreshProductManager' : 'FreshProductManager',
#                     'utm_source=directorsclub' : 'directorsclub',
#                     'utm_source=GIT' : 'GIT',
#                     'utm_source=Grantium' : 'Grantium',
#                     'utm_source=business&analytics' : 'business&analytics',
#                     'utm_source=actual_management' : 'actual_management',
#                     'utm_source=science&tech' : 'science&tech',
#                     'vk55': 'ВК Ален',
#                     'vk4': 'ВК Юля',
#                     'google54': 'Google ads Лидсфера',
#                     'utm_source=salozen': 'Дзен'
#                     }

#                     # Создаем словарь, где ключи - метки аккаунтов, значения - названия аккаунтов
#   account = {'facebook6': 'Юля 1', 'facebook3': 'Юля 2', 'facebook7': 'Юля 3', 'facebook8': 'Юля 4', 'facebook10': 'Юля 5', 'facebook14': 'Юля 7', 'facebook46': 'Юля каб.1 тест', 'facebook47': 'Юля каб.2 тест',
#             'facebook15': 'Ксения 1', 'facebook16': 'Ксения 2', 'facebook32': 'Ксения 3', 'facebook28': 'Ксения 4',
#             'facebook19': 'Евгений 1', 'facebook18': 'Евгений 2', 'facebook38': 'Евгений 3',
#             'facebook24': 'Михаил 1', 'facebook25': 'Михаил 2',
#             'facebook23': 'Яна 1', 'facebook26': 'Яна 2',
#             'facebook30': 'Антон 1',
#             'facebook22': 'Демура 1',
#             'facebook34': 'УИИ 1',
#             'facebook37': 'Валерий 1', 'facebook23': 'Валерий 2', 'facebook26': 'Валерий 3',
#             'facebook39': 'Артем 1', 'facebook40': 'Артем 2',
#             'facebook17': 'Алина 1', 'facebook29': 'Алина 2', 'facebook35': 'Алина 3', 'facebook36': 'Алина 4', 'facebook33': 'Алина 5', 'facebook52': 'Алина 6', 'facebook51': 'Алина 7',
#             'direct1': 'Юля Директ',
#             'direct11': 'ArrowMedia Директ',
#             'direct27': 'Директ 2',
#             'facebook48': 'Zufar 1',
#             'facebook49': 'Максим С 1',
#             'facebook50': 'Luxury Promo 1',  
#             'facebook53': 'Resource Traget 1',        
#             'utm_source=tproger': 'tproger 1',
#             'utm_source=itlecture': 'itlecture 1',
#             'utm_source=progeri': 'progeri 1',
#             'utm_source=secrets_of_programmist': 'secrets_of_programmist 1',
#             'utm_source=web_dev_humor': 'web_dev_humor 1',
#             'utm_source=itlibr': 'itlibr 1',
#             'utm_source=litra_it': 'litra_it 1',
#             'utm_source=tipaproit': 'tipaproit 1',
#             'utm_source=itneedit': 'itneedit 1',
#             'utm_source=bookpython': 'bookpython 1',
#             'utm_source=proglb': 'proglb 1',
#             'utm_source=python2day': 'python2day 1',
#             'utm_source=yaprogrammer': 'yaprogrammer 1',
#             'utm_source=js_test': 'js_test 1',
#             'utm_source=thehaking': 'thehaking 1',
#             'utm_source=python+test': 'python+test 1',
#             'utm_source=tgverstka': 'tgverstka 1',
#             'utm_source=apple_secret': 'apple_secret 1',
#             'utm_source=lullisteacher': 'lullisteacher 1',
#             'utm_source=godnotech': 'godnotech 1',
#             'utm_source=programmistkas': 'programmistkas 1',
#             'utm_source=python_academy': 'python_academy 1',
#             'utm_source=u_programmer': 'u_programmer 1',
#             'utm_source=campcode': 'campcode 1',
#             'utm_source=neuralmachine': 'neuralmachine 1',
#             'utm_source=pythonaa': 'pythonaa 1',
#             'utm_source=memsdigital': 'memsdigital 1',  
#             'utm_source=reddit' : 'reddit 1',
#             'utm_source=FreshProductManager' : 'FreshProductManager 1',
#             'utm_source=directorsclub' : 'directorsclub 1',
#             'utm_source=GIT' : 'GIT 1',
#             'utm_source=Grantium' : 'Grantium 1',
#             'utm_source=business&analytics' : 'business&analytics 1',
#             'utm_source=actual_management' : 'actual_management 1',
#             'utm_source=science&tech' : 'science&tech 1',
#             'vk55': 'ВК Ален 1',
#             'vk4': 'ВК Юля 1',
#             'google54': 'Google ads Лидсфера 1',
#             'utm_source=salozen': 'Дзен'
#               }



#   # Добавляем названия столбцов, которые нужно удалить при выводе статистики по кабинетам
#   delete_list = ['Демура 1', 'УИИ 1', 'Неизвестно 1', 'Антон 1', 'Zufar 1', 'Максим С 1', 'Luxury Promo 1',
#                 'Resource Traget 1',
#                 'tproger 1',
#                 'itlecture 1',
#                 'progeri 1',
#                 'secrets_of_programmist 1',
#                 'web_dev_humor 1',\
#                 'itlibr 1',
#                 'litra_it 1',
#                 'tipaproit 1',
#                 'itneedit 1',
#                 'bookpython 1',
#                 'proglb 1',
#                 'python2day 1',
#                 'yaprogrammer 1',
#                 'js_test 1',
#                 'thehaking 1',
#                 'python+test 1',
#                 'tgverstka 1',
#                 'apple_secret 1',
#                 'lullisteacher 1',
#                 'godnotech 1',
#                 'programmistkas 1',
#                 'python_academy 1',
#                 'u_programmer 1',
#                 'campcode 1',
#                 'neuralmachine 1',
#                 'pythonaa 1',
#                 'memsdigital 1',
#                 'reddit 1',
#                 'FreshProductManager 1',
#                 'directorsclub 1',
#                 'GIT 1',
#                 'Grantium 1',
#                 'business&analytics 1',
#                 'actual_management 1',
#                 'science&tech 1',
#                 'ВК Ален 1',
#                 'ВК Юля 1',
#                 'Google ads Лидсфера 1']

#   # Список категорий по приоритетности или возрастанию
#   countries = ['Россия', 'Европа', 'Северная Америка', 'Южная Корея', 'Киев', 'Минск', 'Нур-Султан', 'Израиль', 'ОАЭ', 'Латинская Америка', 'Юго-Восточная Азия', 'Австралия',  'СНГ', 'Другое']
#   ages = ['до 18 лет', '18 - 23', '18 - 25', '24 - 30', '26 - 30', '31 - 40', '41 - 50', '51 - 60', '60 +']
#   jobs = ['IT сфера', 'Связано с числами', 'Гуманитарий',
#         'Предприниматель, руководитель',
#         'Преподаватель, учитель', 'Школьник, студент', 'Пенсионер', 'Декрет', ]
#   earnings = ['0 руб.', '0руб.', '0 руб./ $0',
#               'до 30 000 руб.',
#               'до 30 000 руб. / до $400',
#               'до 60 000 руб.',
#               'до 100 000 руб.',
#               'более 100 000 руб.',
#               'до 60 000 руб. / до $800',
#               'до 100 000 руб. / до $1400',
#               'более 100 000 руб. / более $1400']
#   trainings = ['Да, работа', 'Да, проект', 'Нет']
#   times = ['до 5 часов в неделю', 'до 10 часов в неделю', 'более 10 часов в неделю', 'Неизвестно']

#   # Данные ЦА (целевой аудитории) по каждой категории - индексы ЦА
#   start_countries, end_countries = 0, 12
#   start_ages, end_ages = 3, 8
#   start_jobs, end_jobs = 0, 5
#   start_earnings, end_earnings = 3, 11
#   start_trainings, end_trainings = 0, 2
#   start_times, end_times = 0, 4



#   df.insert(19, 'trafficologist', 'Неизвестно')                # Добавляем столбец trafficologist для записи имени трафиколога
#   df.insert(20, 'account', 'Неизвестно 1')                       # Добавляем столбец account для записи аккаунта трафиколога
#   df.insert(21, 'target_class', 0)  

#   links_list = [] # Сохраняем в список ссылки, не содержащие метки аккаунтов
#   for el in list(trafficologist.keys()):
#     for i in range(df.shape[0]):
#       try:
#         if el in df.loc[i, 'traffic_channel']:
#           df.loc[i, 'trafficologist'] = trafficologist[el]
#           df.loc[i, 'account'] = account[el]
#       except TypeError:
#         links_list.append(df.loc[i, 'traffic_channel'])

#   for i in range(df.shape[0]):
#     target_class = 0
#     if df.loc[i, 'quiz_answers1'] in countries[start_countries:end_countries]:
#       target_class += 1
#     if df.loc[i, 'quiz_answers2'] in ages[start_ages:end_ages]:
#       target_class += 1
#     if df.loc[i, 'quiz_answers3'] in jobs[start_jobs:end_jobs]:
#       target_class += 1
#     if df.loc[i, 'quiz_answers4'] in earnings[start_earnings:end_earnings]:
#       target_class += 1         
#     if df.loc[i, 'quiz_answers5'] in trainings[start_trainings:end_trainings]:
#       target_class += 1
#     if df.loc[i, 'quiz_answers6'] in times[start_times:end_times]:
#       target_class += 1

#     df.loc[i, 'target_class'] = target_class

#   # display(df)



#   def DataSetCheking(df):
#     countries_errors = []
#     ages_errors = []
#     jobs_errors = []
#     earnings_errors = []
#     trainings_errors = []
#     times_errors = []

#     for i in range(df.shape[0]):
#       if df['Страна'].values[i] not in countries:
#         countries_errors.append(df['Страна'].values[i])
#       if df['Возраст'].values[i] not in ages:
#         ages_errors.append(df['Возраст'].values[i])
#       if df['Профессия'].values[i] not in jobs:
#         jobs_errors.append(df['Профессия'].values[i])
#       if df['Доход'].values[i] not in earnings:
#         earnings_errors.append(df['Доход'].values[i])
#       if df['Обучение'].values[i] not in trainings:
#         trainings_errors.append(df['Обучение'].values[i])
#       if df['Время'].values[i] not in times:
#         times_errors.append(df['Время'].values[i])
#     print('Ошибки в колонке "Страна": \033[91m', np.unique(countries_errors))
#     print()
#     print('\033[0mОшибки в колонке "Возраст": \033[91m', np.unique(ages_errors))
#     print()
#     print('\033[0mОшибки в колонке "Профессия": \033[91m', np.unique(jobs_errors))
#     print()
#     print('\033[0mОшибки в колонке "Доход": \033[91m', np.unique(earnings_errors))
#     print()
#     print('\033[0mОшибки в колонке "Обучение": \033[91m', np.unique(trainings_errors))
#     print()
#     print('\033[0mОшибки в колонке "Время": \033[91m', np.unique(times_errors))

#       # Функция формирования двух датасетов по выбранной категории и соотвествующему столбцу в которой эти категории находятся
#   def channels_table(df, category, column_name):
#     '''
#       Функция формирования двух датасетов по выбранной категории и соотвествующему столбцу в которой эти категории находятся.
#       Вход:
#           category - список подкатегорий по выбранной категории
#           column_name - название колонки в датасете, которая содержит названия подкатегорий
#       Выход:
#           df_category - датасет с разбивкой заявок трафикологов по категориям в %
#           df_category_val - датасет с разбивкой заявок трафикологов по категориям в штуках
#     '''
#     # Задаем начальный и конечный индексы для ЦА из списка
#     if category == countries: start, end = start_countries, end_countries
#     if category == ages: start, end = start_ages, end_ages
#     if category == jobs: start, end = start_jobs, end_jobs
#     if category == earnings: start, end = start_earnings, end_earnings
#     if category == trainings: start, end = start_trainings, end_trainings
#     if category == times: start, end = start_times, end_times

#     data_category = np.zeros((len(category)+1, len(created_columns))).astype('int')
#     df_category = pd.DataFrame(data_category, columns=created_columns)
#     df_category.insert(0, 'Все', 0)
#     df_category.insert(0, column_name, 0)

#     df_category_val = pd.DataFrame(data_category, columns=created_columns)
#     df_category_val.insert(0, 'Все', 0)
#     df_category_val.insert(0, column_name, 0)

#     for idx, country_name in enumerate(category):
#       df_category_val.iloc[idx, 0] = country_name
#       df_category_val.iloc[idx, 1] = df[df[column_name] == country_name].shape[0]

#       df_category.iloc[idx, 0] = country_name
#       df_category.iloc[idx, 1] = int(round((df[df[column_name] == country_name].shape[0]/df.shape[0])*100, 0))

#       for traff_name in df_category_val.columns.values[2:]:
#         df_category_val.loc[idx, traff_name] = df[(df[column_name] == country_name) & ((df['trafficologist'] == traff_name) | (df['account'] == traff_name))].shape[0]

#         df_category.loc[idx, traff_name] = round((df_category_val.loc[idx, traff_name]/df[(df['trafficologist'] == traff_name) | (df['account'] == traff_name)].shape[0])*100, 0).astype('int')

#     df_category_val.loc[idx+1, column_name] = 'ЦА'
#     df_category.loc[idx+1, column_name] = 'ЦА'

#     for i in range(1, df_category_val.shape[1]):
#       df_category_val.iloc[idx+1, i] = df_category_val.iloc[start:end, i].sum()  
#       df_category.iloc[idx+1, i] = df_category.iloc[start:end, i].sum()  

#     df_category_val.drop(columns=delete_columns,inplace=True)
#     df_category.drop(columns=delete_columns,inplace=True)

#     return df_category_val, df_category


#     DataSetCheking(df)

#     # Объединяем массивы уникальных значений в колонках trafficologist и account
#   # Оставляем только уникальные значения, переводи массив в список и сортируем
#   created_columns = np.unique(np.concatenate((df['trafficologist'].unique(), df['account'].unique()))).tolist()
#   created_columns.sort()

#   delete_columns = []
#   for el in created_columns:
#     if el in delete_list:
#       delete_columns.append(el)

#   df_countries_val, df_countries = channels_table(df = df, category = countries, column_name = df.columns[2])
#   df_ages_val, df_ages = channels_table(df = df, category = ages, column_name = df.columns[3])
#   df_jobs_val, df_jobs = channels_table(df = df, category = jobs, column_name = df.columns[4])
#   df_earnings_val, df_earnings = channels_table(df = df, category = earnings, column_name = df.columns[5])
#   df_trainings_val, df_trainings = channels_table(df = df, category = trainings, column_name = df.columns[6])
#   df_times_val, df_times = channels_table(df = df, category = times, column_name = df.columns[7])
#   return df_countries_val, df_countries, df_ages_val, df_ages, df_jobs_val, df_jobs, df_earnings_val, df_earnings, df_trainings_val, df_trainings, df_times_val, df_times


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