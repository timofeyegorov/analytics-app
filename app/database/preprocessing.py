from config import RESULTS_FOLDER, DATA_FOLDER
import pickle as pkl
import pandas as pd
import os

def preprocess_dataframe(df):
	'''
		Preprocess leads dataframe changing erroneous values and adding following columns:
		'trafficologist' - trafficologist name
        'account' - trafficologist account name
        'target_class' - the number of hits of the lead in the target audience
	:return: prepared leads dataframe
	'''
	# В категории (столбце) Сколько_вам_лет меняем значение подкатегорий
	df.loc[df['quiz_answers2']=='23 - 30', 'quiz_answers2'] = '24 - 30'
	df.loc[df['quiz_answers2']=='25 - 30', 'quiz_answers2'] = '26 - 30'
	df.loc[df['quiz_answers2']=='30 - 40', 'quiz_answers2'] = '31 - 40'
	df.loc[df['quiz_answers2']=='40 - 50', 'quiz_answers2'] = '41 - 50'
	df.loc[df['quiz_answers2']=='50 - 60', 'quiz_answers2'] = '51 - 60'

	# В категории (столбце) Ваш_средний_доход_в_месяц меняем значение подкатегорий
	df.loc[df['quiz_answers4']=='0 руб./ $0', 'quiz_answers4'] = '0 руб.'
	df.loc[df['quiz_answers4']=='0руб.', 'quiz_answers4'] = '0 руб.'
	df.loc[df['quiz_answers4']=='более 100 000 руб. / более $1400', 'quiz_answers4'] = 'более 100 000 руб.'
	df.loc[df['quiz_answers4']=='до 100 000 руб. / до $1400', 'quiz_answers4'] = 'до 100 000 руб.'
	df.loc[df['quiz_answers4']=='до 30 000 руб. / до $400', 'quiz_answers4'] = 'до 30 000 руб.'
	df.loc[df['quiz_answers4']=='до 60 000 руб. / до $800', 'quiz_answers4'] = 'до 60 000 руб.'

	# Меняем формулировки в столбце "Обучение" на сокращенные (для более удобной визуализции)
	df.loc[df['quiz_answers5'] == 'Да, если это поможет мне в реализации проекта.', 'quiz_answers5'] = 'Да, проект'
	df.loc[df['quiz_answers5'] == 'Да, если я точно найду работу после обучения.', 'quiz_answers5'] = 'Да, работа'
	df.loc[df['quiz_answers5'] == 'Нет. Хочу получить только бесплатные материалы.', 'quiz_answers5'] = 'Нет'        

	# Меняем формулировки в столбце "Профессия" на сокращенные (для более удобной визуализции)
	df.loc[df['quiz_answers3'] == 'Профессии связанные с числами (аналитик, бухгалтер, инженер и т.п.)', 'quiz_answers3'] = 'Связано с числами'
	df.loc[df['quiz_answers3'] == 'Гуманитарий (общение с людьми, искусство, медицина и т.д.)', 'quiz_answers3'] = 'Гуманитарий'
	df.loc[df['quiz_answers3'] == 'IT сфера (разработчик, тестировщик, администратор и т.п.)', 'quiz_answers3'] = 'IT сфера'

	df.insert(19, 'trafficologist', 'Неизвестно')  # Добавляем столбец trafficologist для записи имени трафиколога
	df.insert(20, 'account', 'Неизвестно 1')  # Добавляем столбец account для записи аккаунта трафиколога
	df.insert(21, 'target_class', 0)

	with open(os.path.join(DATA_FOLDER, 'trafficologists.pkl'), 'rb') as f:
		traff_data = pkl.load(f)
	# Анализируем ссылки каждого лида на то, какой трафиколог привел этого лида
	links_list = []  # Сохраняем в список ссылки, не содержащие метки аккаунтов (в таком случае неизвестно, кто привел лида)
	for el in list(traff_data['label']):  # Проходимся по всем метка которые есть
		for i in range(df.shape[0]):  # Проходим по всему датасету
			try:  # Пробуем проверить, есть ли элемент в ссылке
				if el in df.loc[i, 'traffic_channel']:  # Если элемент (метка) есть
					df.loc[i, 'trafficologist'] = traff_data[traff_data['label'] == el]['name'].values[
						0]  # Заносим имя трафиколога по в ячейку по значению метки
					df.loc[i, 'account'] = traff_data[traff_data['label'] == el]['title'].values[
						0]  # Заносим кабинет трафиколога по в ячейку по значению метки
			except TypeError:  # Если в ячейке нет ссылки, а проставлен 0
				links_list.append(df.loc[i, 'traffic_channel'])

	# Добавляем в датасет данные по количеству попаданий лидов в целевую аудиторию
	with open(os.path.join(DATA_FOLDER, 'target_audience.pkl'), 'rb') as f:
		target_audience = pkl.load(f)

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

	return df

if __name__ == '__main__':
	df = pd.read_csv(os.path.join(RESULTS_FOLDER, 'leads.csv'))
	df = preprocess_dataframe(df)
	print(df)



