import os
import numpy as np
import pandas as pd
import pickle as pkl
from config import RESULTS_FOLDER


def calculate_segments(df):
    with open(os.path.join(RESULTS_FOLDER, "target_audience.pkl"), "rb") as f:
        target_audience = pkl.load(f)

    with open(os.path.join(RESULTS_FOLDER, "trafficologists.pkl"), "rb") as f:
        traff_data = pkl.load(f)
    with open(os.path.join(RESULTS_FOLDER, "crops_list.pkl"), "rb") as f:
        crops_data = pkl.load(f)

    # Получаем массив трафикологов в отфильтрованном датасете
    filtered_trafficologists = df["trafficologist"].unique()
    # Создаем список с трафикологами и названиями их кабинетов - это будут заголовки колонок результирующей таблицы
    created_columns = []

    for name in filtered_trafficologists:
        created_columns.append(name)
        # Проверяем, что у трафиколога более 1 кабинета, чтобы не дублировать инфу, если кабинет только один
        if traff_data[traff_data["name"] == name]["title"].shape[0] > 1:
            created_columns.extend(
                list(df[df["trafficologist"] == name]["account"].unique())
            )
    created_columns.sort()

    # Функция формирования двух датасетов с количеством лидов и процентным соотношением
    # по выбранной категории с разбивкой по трафикологам и их кабинетам

    def channels_table(df, column_name):
        """
        Функция формирования двух датасетов по выбранной категории
        Вход:
            df - обрабатываемый датасет
            category - список подкатегорий по выбранной категории
            column_name - название колонки в датасете, которая содержит названия подкатегорий
        Выход:
            df_category - датасет с разбивкой заявок трафикологов по подкатегориям в %
            df_category_val - датасет с разбивкой заявок трафикологов по подкатегориям в штуках
        """
        # Создаем заготовки результирующих датасетов из нулей
        data_category = np.zeros(
            (len(df[column_name].unique()) + 1, len(created_columns))
        ).astype("int")
        template_df = pd.DataFrame(data_category, columns=created_columns)
        df_category = template_df.copy()  # Датасет для процентного кол-ва лидов
        df_category.insert(
            0, "Все", 0
        )  # Столбец для подсчета всех лидов (сумма по всем таргетологам)
        df_category.insert(
            0, column_name, 0
        )  # Столбец названия подкатегорий указанной категории

        df_category_val = template_df.copy()  # Датасет для абсолютного кол-ва лидов
        df_category_val.insert(
            0, "Все", 0
        )  # Столбец для подсчета всех лидов (сумма по всем таргетологам)
        df_category_val.insert(
            0, column_name, 0
        )  # Столбец названия подкатегорий указанной категории
        # Проходим в цикле по каждой подкатегории выбранной категории
        # Формируем список для прохода сначала по подкатегориям куда попадает ЦА
        if column_name == df.columns[3]:
            subcategories = list(df[column_name].unique())
            subcategories.sort()
            try:
                subcategories.pop(subcategories.index("до 18 лет"))
                subcategories.insert(0, "до 18 лет")
            except ValueError:
                pass
        elif column_name == df.columns[5]:
            temp_list = [
                "0 руб.",
                "до 30 000 руб.",
                "до 60 000 руб.",
                "до 100 000 руб.",
                "более 100 000 руб.",
            ]
            subcategories = (
                temp_list
                + df[~df[column_name].isin(temp_list)][column_name].unique().tolist()
            )
            subcategories_temp = subcategories.copy()
            for el in subcategories_temp:
                if el not in df[column_name].unique():
                    subcategories.remove(el)
        elif column_name == df.columns[7]:
            temp_list = [
                "до 5 часов в неделю",
                "до 10 часов в неделю",
                "более 10 часов в неделю",
            ]
            subcategories = (
                temp_list
                + df[~df[column_name].isin(temp_list)][column_name].unique().tolist()
            )
            subcategories_temp = subcategories.copy()
            for el in subcategories_temp:
                if el not in df[column_name].unique():
                    subcategories.remove(el)
        else:
            subcategories = (
                df[df[column_name].isin(target_audience)][column_name].unique().tolist()
                + df[~df[column_name].isin(target_audience)][column_name]
                .unique()
                .tolist()
            )

        for idx, subcategory_name in enumerate(subcategories):
            df_category_val.iloc[idx, 0] = subcategory_name
            df_category_val.iloc[idx, 1] = df[
                df[column_name] == subcategory_name
            ].shape[0]

            df_category.iloc[idx, 0] = subcategory_name
            df_category.iloc[idx, 1] = int(
                round(
                    (df[df[column_name] == subcategory_name].shape[0] / df.shape[0])
                    * 100,
                    0,
                )
            )

            # Проходим в цикле по каждому таргетологу и его кабинету
            for traff_name in created_columns:
                df_category_val.loc[idx, traff_name] = df[
                    (df[column_name] == subcategory_name)
                    & (
                        (df["trafficologist"] == traff_name)
                        | (df["account"] == traff_name)
                    )
                ].shape[0]
                print(df_category_val.loc[idx, traff_name])
                print(
                    df_category_val.loc[idx, traff_name]
                    / (
                        df[
                            (df["trafficologist"] == traff_name)
                            | (df["account"] == traff_name)
                        ].shape[0]
                    )
                    * 100
                )
                df_category.loc[idx, traff_name] = (
                    (
                        df_category_val.loc[idx, traff_name]
                        / (
                            df[
                                (df["trafficologist"] == traff_name)
                                | (df["account"] == traff_name)
                            ].shape[0]
                        )
                        * 100
                    )
                    .round()
                    .astype(int)
                )

        df_category_val.loc[idx + 1, column_name] = "ЦА"
        df_category.loc[idx + 1, column_name] = "ЦА"

        # По каждой колонке считаем кол-во лидов попадающих в ЦА
        for i in range(1, df_category_val.shape[1]):
            df_category_val.iloc[idx + 1, i] = (
                df_category_val[df_category_val[column_name].isin(target_audience)]
                .iloc[:, i]
                .sum()
            )
            df_category.iloc[idx + 1, i] = (
                df_category[df_category[column_name].isin(target_audience)]
                .iloc[:, i]
                .sum()
            )

        return df_category_val, df_category

    # Функция формирования двух датасетов по ЦА
    def target_audience_channels_table(df):

        # Формируем заготовки под результирующие датасеты из нулей
        data_ta = np.zeros((8, len(created_columns))).astype("int")
        template_df = pd.DataFrame(data_ta, columns=created_columns)
        df_ta = template_df.copy()
        df_ta_val = template_df.copy()

        df_ta_val.insert(0, "Все", 0)
        df_ta.insert(0, "Все", 0)

        # Проходим по всем количествам попаданий в ЦА (от 0 до 6)
        for i in range(7):
            df_ta_val.loc[i, "Все"] = df[df["target_class"] == i].shape[0]
            df_ta.loc[i, "Все"] = int(
                round(df[df["target_class"] == i].shape[0] / df.shape[0] * 100, 0)
            )

            df_ta_val.loc[7, "Все"] = df_ta_val.loc[5:6, "Все"].sum()
            df_ta.loc[7, "Все"] = df_ta.loc[5:6, "Все"].sum()

        # Проходим по всем трафикологам и их кабинетам
        for traff_name in created_columns:
            for t_class in range(
                7
            ):  # Проходим по всем количествам попаданий в ЦА (от 0 до 6)
                df_ta_val.loc[t_class, traff_name] = df[
                    (df["target_class"] == t_class)
                    & (
                        (df["trafficologist"] == traff_name)
                        | (df["account"] == traff_name)
                    )
                ].shape[0]
                df_ta.loc[t_class, traff_name] = round(
                    df_ta_val.loc[t_class, traff_name]
                    / (
                        df[
                            (df["trafficologist"] == traff_name)
                            | (df["account"] == traff_name)
                        ].shape[0]
                    )
                    * 100,
                    0,
                ).astype("int")

            df_ta_val.loc[7, traff_name] = df_ta_val.loc[5:6, traff_name].sum()
            df_ta.loc[7, traff_name] = df_ta.loc[5:6, traff_name].sum()

        df_ta_val.rename(index={7: "5-6"}, inplace=True)
        df_ta.rename(index={7: "5-6"}, inplace=True)
        return df_ta_val, df_ta

    # Функция формирования датасетов по ЦА
    def target_consolidated(df):

        # Формируем заготовки под результирующие датасеты из нулей
        data_segment = np.zeros((6, len(created_columns))).astype("int")
        template_df = pd.DataFrame(data_segment, columns=created_columns)
        df_segment = template_df.copy()
        df_segment.insert(0, "Все", 0)
        df_segment.insert(0, "Сегмент", 0)

        df_segment_val = template_df.copy()
        df_segment_val.insert(0, "Все", 0)
        df_segment_val.insert(0, "Сегмент", 0)

        categories_names = {
            df.columns.values[2]: "Страны",
            df.columns.values[3]: "Возраст",
            df.columns.values[4]: "Профессия",
            df.columns.values[5]: "Заработок",
            df.columns.values[6]: "Обучение",
            df.columns.values[7]: "Время",
        }
        for idx, column_category in enumerate(df.columns.values[2:8]):
            df_segment_val.loc[idx, "Сегмент"] = categories_names[column_category]
            df_segment_val.loc[idx, "Все"] = df[
                df[column_category].isin(target_audience)
            ].shape[0]

            df_segment.loc[idx, "Сегмент"] = categories_names[column_category]
            df_segment.loc[idx, "Все"] = round(
                df[df[column_category].isin(target_audience)].shape[0]
                / df.shape[0]
                * 100,
                0,
            )

            for traff_name in created_columns:
                df_segment_val.loc[idx, traff_name] = df[
                    df[column_category].isin(target_audience)
                    & (
                        (df["trafficologist"] == traff_name)
                        | (df["account"] == traff_name)
                    )
                ].shape[0]
                df_segment.loc[idx, traff_name] = round(
                    df[
                        df[column_category].isin(target_audience)
                        & (
                            (df["trafficologist"] == traff_name)
                            | (df["account"] == traff_name)
                        )
                    ].shape[0]
                    / df[
                        (df["trafficologist"] == traff_name)
                        | (df["account"] == traff_name)
                    ].shape[0]
                    * 100,
                    0,
                )

        return df_segment_val, df_segment

    df_countries_val, df_countries = channels_table(df=df, column_name=df.columns[2])
    df_ages_val, df_ages = channels_table(df=df, column_name=df.columns[3])
    df_jobs_val, df_jobs = channels_table(df=df, column_name=df.columns[4])
    df_earnings_val, df_earnings = channels_table(df=df, column_name=df.columns[5])
    df_trainings_val, df_trainings = channels_table(df=df, column_name=df.columns[6])
    df_times_val, df_times = channels_table(df=df, column_name=df.columns[7])
    df_target_audience_val, df_target_audience = target_audience_channels_table(df)
    df_segment_val, df_segment = target_consolidated(df)

    df_countries_val = df_countries_val.rename(columns={"quiz_answers1": "Страна"})
    df_countries = df_countries.rename(columns={"quiz_answers1": "Страна"})

    df_ages_val = df_ages_val.rename(columns={"quiz_answers2": "Возраст"})
    df_ages = df_ages.rename(columns={"quiz_answers2": "Возраст"})

    df_jobs_val = df_jobs_val.rename(columns={"quiz_answers3": "Профессия"})
    df_jobs = df_jobs.rename(columns={"quiz_answers3": "Профессия"})

    df_earnings_val = df_earnings_val.rename(columns={"quiz_answers4": "Заработок"})
    df_earnings = df_earnings.rename(columns={"quiz_answers4": "Заработок"})

    df_trainings_val = df_trainings_val.rename(columns={"quiz_answers5": "Обучение"})
    df_trainings = df_trainings.rename(columns={"quiz_answers5": "Обучение"})

    df_times_val = df_times_val.rename(columns={"quiz_answers6": "Время"})
    df_times = df_times.rename(columns={"quiz_answers6": "Время"})

    return {
        "Страны, абсолютные значения": df_countries_val,
        "Страны, относительные значения": df_countries,
        "Возраст, абсолютные значения": df_ages_val,
        "Возраст, относительные значения": df_ages,
        "Профессия, абсолютные значения": df_jobs_val,
        "Профессия, относительные значения": df_jobs,
        "Заработок, абсолютные значения": df_earnings_val,
        "Заработок, относительные значения": df_earnings,
        "Обучение, абсолютные значения": df_trainings_val,
        "Обучение, относительные значения": df_trainings,
        "Время, абсолютные значения": df_times_val,
        "Время, относительные значения": df_times,
        "Попадание в ЦА, абсолютные значения": df_target_audience_val,
        "Попадание в ЦА, относительные значения": df_target_audience,
        "Попадание в ЦА по категориям, абсолютные значения": df_segment_val,
        "Попадание в ЦА по категориям, относительные значения": df_segment,
    }


if __name__ == "__main__":
    path = RESULTS_FOLDER
    with open(os.path.join(path, "leads.pkl"), "rb") as f:
        df = pkl.load(f)
    df = df[:1000]
    res = calculate_segments(df)
    print(res)
