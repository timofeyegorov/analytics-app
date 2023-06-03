import numpy as np
import pandas as pd

from typing import Dict

from flask import current_app


def get_parameter_city_vect(arg, countries_unique):
    out_class = countries_unique.index(arg)
    return list(np.eye(len(countries_unique))[out_class].astype("int"))


def get_parameter_age_vect(arg, ages_unique):
    out_class = ages_unique.index(arg)
    return list(np.eye(len(ages_unique))[out_class].astype("int"))


def get_parameter_job_vect(arg, jobs_unique):
    out_class = jobs_unique.index(arg)
    return list(np.eye(len(jobs_unique))[out_class].astype("int"))


def get_parameter_earnings_vect(arg, earnings_unique):
    out_class = earnings_unique.index(arg)
    return list(np.eye(len(earnings_unique))[out_class].astype("int"))


def get_parameter_training_vect(arg, trainings_unique):
    out_class = trainings_unique.index(arg)
    return list(np.eye(len(trainings_unique))[out_class].astype("int"))


def get_parameter_time_vect(arg, times_unique):
    out_class = times_unique.index(arg)
    return list(np.eye(len(times_unique))[out_class].astype("int"))


def get_all_parameters(
    val,
    countries_unique,
    ages_unique,
    jobs_unique,
    earnings_unique,
    trainings_unique,
    times_unique,
):
    city = get_parameter_city_vect(val[2], countries_unique)
    age = get_parameter_age_vect(val[3], ages_unique)
    job = get_parameter_job_vect(val[4], jobs_unique)
    earning = get_parameter_earnings_vect(val[5], earnings_unique)
    training = get_parameter_training_vect(val[6], trainings_unique)
    times = get_parameter_time_vect(val[7], times_unique)

    out = []
    out += city
    out += age
    out += job
    out += earning
    out += training
    out += times
    return out


def get01_data(
    values,
    countries_unique,
    ages_unique,
    jobs_unique,
    earnings_unique,
    trainings_unique,
    times_unique,
):
    x_train = []  # Создаем пустой x_train

    for val in values:  # Пробегаем по всем записям базы
        x = get_all_parameters(
            val,
            countries_unique,
            ages_unique,
            jobs_unique,
            earnings_unique,
            trainings_unique,
            times_unique,
        )  # Получаем полный набор данных о текущей записи val
        x_train.append(x)  # Добавляем полученные данные в x_train

    x_train = np.array(x_train)  # Переводим в numpy
    return x_train


def calculate(df: pd.DataFrame) -> pd.DataFrame:
    pickle_app = current_app.ext("pickle")

    target_audience = pickle_app.load("target_audience")

    df.reset_index(inplace=True, drop=True)
    countries_unique = list(
        df[df["quiz_answers1"].isin(target_audience)]["quiz_answers1"].unique()
    ) + list(df[~df["quiz_answers1"].isin(target_audience)]["quiz_answers1"].unique())
    ages_unique = list(
        df[df["quiz_answers2"].isin(target_audience)]["quiz_answers2"].unique()
    ) + list(df[~df["quiz_answers2"].isin(target_audience)]["quiz_answers2"].unique())
    jobs_unique = list(
        df[df["quiz_answers3"].isin(target_audience)]["quiz_answers3"].unique()
    ) + list(df[~df["quiz_answers3"].isin(target_audience)]["quiz_answers3"].unique())
    earnings_unique = list(
        df[df["quiz_answers4"].isin(target_audience)]["quiz_answers4"].unique()
    ) + list(df[~df["quiz_answers4"].isin(target_audience)]["quiz_answers4"].unique())
    trainings_unique = list(
        df[df["quiz_answers5"].isin(target_audience)]["quiz_answers5"].unique()
    ) + list(df[~df["quiz_answers5"].isin(target_audience)]["quiz_answers5"].unique())
    times_unique = list(
        df[df["quiz_answers6"].isin(target_audience)]["quiz_answers6"].unique()
    ) + list(df[~df["quiz_answers6"].isin(target_audience)]["quiz_answers6"].unique())

    countries_end = len(
        list(df[df["quiz_answers1"].isin(target_audience)]["quiz_answers1"].unique())
    )

    ages_start = len(countries_unique)
    ages_end = len(countries_unique) + len(
        list(df[df["quiz_answers2"].isin(target_audience)]["quiz_answers2"].unique())
    )

    jobs_start = len(countries_unique) + len(ages_unique)
    jobs_end = (
        len(countries_unique)
        + len(ages_unique)
        + len(
            list(
                df[df["quiz_answers3"].isin(target_audience)]["quiz_answers3"].unique()
            )
        )
    )

    earnings_start = len(countries_unique) + len(ages_unique) + len(jobs_unique)
    earnings_end = (
        len(countries_unique)
        + len(ages_unique)
        + len(jobs_unique)
        + len(
            list(
                df[df["quiz_answers4"].isin(target_audience)]["quiz_answers4"].unique()
            )
        )
    )

    trainings_start = (
        len(countries_unique)
        + len(ages_unique)
        + len(jobs_unique)
        + len(earnings_unique)
    )
    trainings_end = (
        len(countries_unique)
        + len(ages_unique)
        + len(jobs_unique)
        + len(earnings_unique)
        + len(
            list(
                df[df["quiz_answers5"].isin(target_audience)]["quiz_answers5"].unique()
            )
        )
    )

    times_start = (
        len(countries_unique)
        + len(ages_unique)
        + len(jobs_unique)
        + len(earnings_unique)
        + len(trainings_unique)
    )
    times_end = (
        len(countries_unique)
        + len(ages_unique)
        + len(jobs_unique)
        + len(earnings_unique)
        + len(trainings_unique)
        + len(
            list(
                df[df["quiz_answers6"].isin(target_audience)]["quiz_answers6"].unique()
            )
        )
    )

    def add_labels(x_train01):
        x_train01 = x_train01.tolist()

        for i in range(len(x_train01)):
            category = 0
            if sum(x_train01[i][:countries_end]) == 1:
                category += 1
            if sum(x_train01[i][ages_start:ages_end]) == 1:
                category += 1
            if sum(x_train01[i][jobs_start:jobs_end]) == 1:
                category += 1
            if sum(x_train01[i][earnings_start:earnings_end]) == 1:
                category += 1
            if sum(x_train01[i][trainings_start:trainings_end]) == 1:
                category += 1
            if sum(x_train01[i][times_start:times_end]) == 1:
                category += 1

            category_2 = 0
            if x_train01[i][trainings_start] == 1:
                category_2 = 1
            if x_train01[i][trainings_start + 1] == 1:
                category_2 = 2
            if x_train01[i][trainings_start + 2] == 1:
                category_2 = 3

            x_train01[i].append(category)
            x_train01[i].append(category_2)
        x_train01 = np.array(x_train01)
        return x_train01

    # Функция для вставки строки в фрейм данных
    def insert_row(row_number, df, row_value):
        start_upper = 0  # Начальное значение верхней половины
        end_upper = row_number  # Конечное значение верхней половины
        start_lower = row_number  # Начальное значение нижней половины
        end_lower = df.shape[0]  # Конечное значение нижней половины
        upper_half = [
            *range(start_upper, end_upper, 1)
        ]  # Создать список индекса upper_half
        lower_half = [
            *range(start_lower, end_lower, 1)
        ]  # Создать список индекса lower_half
        lower_half = [
            x.__add__(1) for x in lower_half
        ]  # Увеличить значение нижней половины на 1
        index_ = upper_half + lower_half  # Объединить два списка
        df.index = index_  # Обновить индекс данных
        df.loc[row_number] = row_value  # Вставить строку в конце
        df = df.sort_index()  # Сортировать метки индекса
        return df

    all_categories = (
        countries_unique
        + ages_unique
        + jobs_unique
        + earnings_unique
        + trainings_unique
        + times_unique
    )

    all_categories_len = len(
        countries_unique
        + ages_unique
        + jobs_unique
        + earnings_unique
        + trainings_unique
        + times_unique
    )

    def cluster_dataframe(x):
        if x_train01[x_train01[:, all_categories_len] == x].shape[0] != 0:
            m_x = np.mean(
                x_train01[x_train01[:, all_categories_len] == x], axis=0
            )  # Считаем среднее значение по кластеру
            s_x = np.sum(
                x_train01[x_train01[:, all_categories_len] == x], axis=0
            )  # Считаем сумму значение по кластеру
            m_all = np.mean(x_train01, axis=0)  # Считаем среднее значение по базе
            s_all = np.sum(x_train01, axis=0)  # Считаем сумму значение по базе

            df_data = np.zeros((all_categories_len, 6))
            cluster_columns = [
                "Сегмент",
                "Процент в кластере",
                "Процент в базе",
                "Процент кластер/база",
                "Кол-во в кластере",
                "Кол-во в базе",
            ]
            df = pd.DataFrame(df_data, columns=cluster_columns)

            for i in range(all_categories_len):
                # try:
                df.iloc[i, 0] = all_categories[i]
                df.iloc[i, 1] = int(round(100 * m_x[i]))
                df.iloc[i, 2] = int(round(100 * m_all[i]))
                if (m_all[i] * 100 >= 1) & (m_x[i] * 100 >= 1):
                    df.iloc[i, 3] = int(round(m_x[i] / m_all[i] * 100))
                else:
                    df.iloc[i, 3] = 0
                df.iloc[i, 4] = int(s_x[i])
                df.iloc[i, 5] = int(s_all[i])
            # except ValueError:
            #   if np.isnan(m_x[i]) == True:
            #       m_x[i] = 0
            #   df.iloc[i,0] = temp_list[i]
            #   df.iloc[i,1] = int(round(100*m_x[i]))
            #   df.iloc[i,2] = int(round(100*m_all[i]))
            #   if m_all[i] != 0:
            #     df.iloc[i,3] = int(round(100*m_x[i]/100*m_all[i])*100)
            #   else:
            #     df.iloc[i,3] = 0
            #   df.iloc[i,4] = int(s_x[i])
            #   df.iloc[i,5] = int(s_all[i])

            df = insert_row(0, df, ["Страна", "-", "-", "-", "-", "-"])
            df = insert_row(ages_start + 1, df, ["Возраст", "-", "-", "-", "-", "-"])
            df = insert_row(jobs_start + 2, df, ["Профессия", "-", "-", "-", "-", "-"])
            df = insert_row(earnings_start + 3, df, ["Доход", "-", "-", "-", "-", "-"])
            df = insert_row(
                trainings_start + 4, df, ["Обучение", "-", "-", "-", "-", "-"]
            )
            df = insert_row(times_start + 5, df, ["Время", "-", "-", "-", "-", "-"])

            return df
        else:
            return None

    # def cluster_dataframe_education(x):
    #     m_x = np.mean(
    #         x_train01[x_train01[:, all_categories_len + 1] == x], axis=0
    #     )  # Считаем среднее значение по кластеру
    #     s_x = np.sum(
    #         x_train01[x_train01[:, all_categories_len + 1] == x], axis=0
    #     )  # Считаем сумму значение по кластеру
    #     m_all = np.mean(x_train01, axis=0)  # Считаем среднее значение по базе
    #     s_all = np.sum(x_train01, axis=0)  # Считаем сумму значение по базе
    #
    #     df_data = np.zeros((all_categories_len, 6))
    #     cluster_columns = [
    #         "Сегмент",
    #         "Процент в кластере",
    #         "Процент в базе",
    #         "Процент кластер/база",
    #         "Кол-во в кластере",
    #         "Кол-во в базе",
    #     ]
    #     df = pd.DataFrame(df_data, columns=cluster_columns)
    #
    #     for i in range(all_categories_len):
    #         df.iloc[i, 0] = all_categories[i]
    #         df.iloc[i, 1] = int(round(100 * m_x[i]))
    #         df.iloc[i, 2] = int(round(100 * m_all[i]))
    #         if m_all[i] != 0:
    #             df.iloc[i, 3] = int(round(100 * m_x[i] / 100 * m_all[i]) * 100)
    #         else:
    #             df.iloc[i, 3] = 0
    #         df.iloc[i, 4] = int(s_x[i])
    #         df.iloc[i, 5] = int(s_all[i])
    #
    #     df = insert_row(0, df, ["Страна", "-", "-", "-", "-", "-"])
    #     df = insert_row(ages_start + 1, df, ["Возраст", "-", "-", "-", "-", "-"])
    #     df = insert_row(jobs_start + 2, df, ["Профессия", "-", "-", "-", "-", "-"])
    #     df = insert_row(earnings_start + 3, df, ["Доход", "-", "-", "-", "-", "-"])
    #     df = insert_row(trainings_start + 4, df, ["Обучение", "-", "-", "-", "-", "-"])
    #     df = insert_row(times_start + 5, df, ["Время", "-", "-", "-", "-", "-"])
    #
    #     return df

    x_train01 = get01_data(
        df.values,
        countries_unique,
        ages_unique,
        jobs_unique,
        earnings_unique,
        trainings_unique,
        times_unique,
    )  # Создаем обучающую выборку по данным из базы
    x_train01 = add_labels(x_train01)  # Добавляем метки кластера

    res_dict = {}
    for i in range(7):
        num = i

        cluster_ = cluster_dataframe(num)
        # print(f'\033[1m{num} попаданий в ЦА\033[0m')
        # print()
        # print("Размер кластера:", x_train01[x_train01[:, all_categories_len]==num].shape[0]) # Выведем количество элементов в кластере
        # display(cluster_)

        res_dict.update(
            {
                f"{num} попаданий в ЦА": {
                    "Датасет": cluster_,
                    "Размер кластера": x_train01[
                        x_train01[:, all_categories_len] == num
                    ].shape[0],
                }
            }
        )
    return res_dict
