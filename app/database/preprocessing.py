import numpy as np

def preprocess_dataframe(df):
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
	return df

change_dict = {'23 - 30': '24 - 30', '25 - 30': '26 - 30', '30 - 40': '31 - 40', '40 - 50': '41 - 50', '50 - 60': 				'51 - 60',
              '0 руб./ $0': '0 руб.', '0руб.': '0 руб.', 'более 100 000 руб. / более $1400': 'более 100 000 руб.',
              'до 100 000 руб. / до $1400': 'до 100 000 руб.', 'Ваш_средний_доход_в_месяц': 'до 100 000 руб.', 'до 30 000 руб. / до $400': 'до 30 000 руб.',
              'до 60 000 руб. / до $800': 'до 60 000 руб.', 
              'Да, если это поможет мне в реализации проекта.': 'Да, проект',
              'Да, если я точно найду работу после обучения.': 'Да, работа',
              'Нет. Хочу получить только бесплатные материалы.': 'Нет',   
              'Профессии связанные с числами (аналитик, бухгалтер, инженер и т.п.)': 'Связано с числами',
              'Гуманитарий (общение с людьми, искусство, медицина и т.д.)': 'Гуманитарий',
              'IT сфера (разработчик, тестировщик, администратор и т.п.)': 'IT сфера'}


def preprocess_target_audience(target_audience):
	for idx, el in enumerate(target_audience):
	  if el in change_dict.keys():
	    target_audience[idx] = change_dict[el]

	indexes = np.unique(target_audience, return_index=True)[1]
	change_target = [target_audience[index] for index in sorted(indexes)]
	target_audience = change_target
	return target_audience

