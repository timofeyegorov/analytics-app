import os
import numpy as np
import pandas as pd
import pickle as pkl
from config import RESULTS_FOLDER


def additional_table(df):
    with open(os.path.join(RESULTS_FOLDER, "target_audience.pkl"), "rb") as f:
        target_audience = pkl.load(f)
    column_names = ["quiz_answers2", "quiz_answers3", "quiz_answers4", "quiz_answers5"]
    subcategory_names = {
        "quiz_answers2": "Возраст",
        "quiz_answers3": "Профессия",
        "quiz_answers4": "Доход",
        "quiz_answers5": "Обучение",
    }
    output = {}
    for column_name in column_names:
        result = [
            [
                subcategory_names[column_name] + ", Подкатегория",
                subcategory_names[column_name] + ", Абс. знач.",
                subcategory_names[column_name] + ", Процент",
            ]
        ]
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
        for subcategory in subcategories:
            temp_vals = []
            temp_vals.append(subcategory)
            temp_vals.append(df[df[column_name] == subcategory].shape[0])
            temp_vals.append(
                str(
                    int(
                        round(
                            df[df[column_name] == subcategory].shape[0]
                            / df.shape[0]
                            * 100,
                            0,
                        )
                    )
                )
                + " %"
            )
            result.append(temp_vals)
        output.update({column_name: pd.DataFrame(result)})
        # output.update({column_name: result})
    res = pd.concat(
        [
            output["quiz_answers2"],
            output["quiz_answers3"],
            output["quiz_answers4"],
            output["quiz_answers5"],
        ],
        axis=1,
    )
    res.columns = res.iloc[0, :].values
    res = res.iloc[1:, :]
    res.reset_index(drop=True, inplace=True)
    res.fillna("", inplace=True)
    # create tuples from MultiIndex
    a = res.columns.str.split(", ", expand=True).values
    # print(a)

    # swap values in NaN and replace NAN to ''
    res.columns = pd.MultiIndex.from_tuples(
        [("", x[0]) if pd.isnull(x[1]) else x for x in a]
    )
    return res


def calculate_channels_summary_detailed(df, utm_source, source, utm_2, utm_2_value):
    if (utm_source == "") & (source == "") & (utm_2 == ""):
        if utm_2_value != "все лиды":
            signature = "Канал=" + utm_2_value
            output_df = df[df["trafficologist"] == utm_2_value]
        else:
            signature = utm_2_value
            output_df = df
    elif utm_source != "":
        if utm_2_value != "все лиды":
            if utm_2 != "":
                signature = "utm_source=" + utm_source + "&" + utm_2 + "=" + utm_2_value
                output_df = df[
                    (df["utm_source"] == utm_source) & (df[utm_2] == utm_2_value)
                ]
            else:
                signature = "utm_source=" + utm_source + "(канал)" + utm_2_value
                output_df = df[df["utm_source"] == utm_source]
        else:
            signature = "Все лиды по utm_source=" + utm_source + "(канал)" + utm_2_value
            output_df = df[df["utm_source"] == utm_source]
    elif (utm_source == "") & (source != ""):
        if utm_2_value != "все лиды":
            if utm_2 != "":
                signature = "Канал=" + source + "&" + utm_2 + "=" + utm_2_value
                output_df = df[(df["account"] == source) & (df[utm_2] == utm_2_value)]
            else:
                signature = "Канал=" + utm_2_value
                output_df = df[df["account"] == utm_2_value]
        else:
            signature = "Все лиды по каналу=" + source
            output_df = df[df["account"] == source]
    elif (utm_source == "") & (source == "") & (utm_2 != ""):
        if utm_2_value != "все лиды":
            signature = utm_2 + "=" + utm_2_value
            output_df = df[(df[utm_2] == utm_2_value)]
        else:
            signature = utm_2_value
            output_df = df
    else:
        signature = "Неизвестная фильтрация"
        output_df = df
    additional_df = additional_table(output_df)
    output_df = output_df[
        [
            "created_at",
            "turnover_on_lead",
            "quiz_answers1",
            "quiz_answers2",
            "quiz_answers3",
            "quiz_answers4",
            "quiz_answers5",
        ]
    ]
    output_df.reset_index(drop=True, inplace=True)
    return {
        "1": {"Лиды в разбивке по: " + signature: output_df},
        "2": {"Доп таблица": additional_df},
    }
