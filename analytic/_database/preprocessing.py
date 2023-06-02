from config import RESULTS_FOLDER
import pickle as pkl
import numpy as np
import pandas as pd
import os
import json
from urllib.parse import urlparse, parse_qs
from datetime import datetime
from dateutil import parser
from app.dags.vk import reader as vk_reader


def preprocess_dataframe(df):
    """
        Preprocess leads dataframe changing erroneous values and adding following columns:
        'trafficologist' - trafficologist name
        'account' - trafficologist account name
        'target_class' - the number of hits of the lead in the target audience
    :return: prepared leads dataframe
    """

    df = df[
        (
            df["quiz_answers1"].isin(
                [
                    "Россия",
                    "Израиль",
                    "Юго-Восточная Азия",
                    "Латинская Америка",
                    "Северная Америка",
                    "СНГ",
                    "Европа",
                    "Другое",
                    "Южная Корея",
                    "ОАЭ",
                    "Австралия",
                ]
            )
        )
        & (
            df["quiz_answers2"].isin(
                [
                    "23 - 30",
                    "25 - 30",
                    "30 - 40",
                    "40 - 50",
                    "50 - 60",
                    "41 - 50",
                    "31 - 40",
                    "26 - 30",
                    "до 18 лет",
                    "18 - 25",
                    "51 - 60",
                    "18 - 23",
                    "60 +",
                    "24 - 30",
                ]
            )
        )
        & (
            df["quiz_answers3"].isin(
                [
                    "Гуманитарий (общение с людьми, искусство, медицина и т.д.)",
                    "IT сфера (разработчик, тестировщик, администратор и т.п.)",
                    "Школьник, студент",
                    "Другое",
                    "Предприниматель, руководитель",
                    "Пенсионер",
                    "Профессии связанные с числами (аналитик, бухгалтер, инженер и т.п.)",
                    "Декрет",
                    "Преподаватель, учитель",
                ]
            )
        )
        & (
            df["quiz_answers4"].isin(
                [
                    "до 30 000 руб.",
                    "более 100 000 руб.",
                    "до 100 000 руб.",
                    "0 руб.",
                    "до 60 000 руб.",
                    "более 100 000 руб. / более $1400",
                    "до 60 000 руб. / до $800",
                    "0руб.",
                    "0 руб./ $0",
                    "до 100 000 руб. / до $1400",
                    "до 30 000 руб. / до $400",
                ]
            )
        )
        & (
            df["quiz_answers5"].isin(
                [
                    "Да, если я точно найду работу после обучения.",
                    "Нет. Хочу получить только бесплатные материалы.",
                    "Да, если это поможет мне в реализации проекта.",
                ]
            )
        )
        & (
            df["quiz_answers6"].isin(
                [
                    "до 10 часов в неделю",
                    "до 5 часов в неделю",
                    "более 10 часов в неделю",
                ]
            )
        )
    ]
    df.reset_index(drop=True, inplace=True)

    # В категории (столбце) Сколько_вам_лет меняем значение подкатегорий
    df.loc[df["quiz_answers2"] == "23 - 30", "quiz_answers2"] = "24 - 30"
    df.loc[df["quiz_answers2"] == "25 - 30", "quiz_answers2"] = "26 - 30"
    df.loc[df["quiz_answers2"] == "30 - 40", "quiz_answers2"] = "31 - 40"
    df.loc[df["quiz_answers2"] == "40 - 50", "quiz_answers2"] = "41 - 50"
    df.loc[df["quiz_answers2"] == "50 - 60", "quiz_answers2"] = "51 - 60"

    # В категории (столбце) Ваш_средний_доход_в_месяц меняем значение подкатегорий
    df.loc[df["quiz_answers4"] == "0 руб./ $0", "quiz_answers4"] = "0 руб."
    df.loc[df["quiz_answers4"] == "0руб.", "quiz_answers4"] = "0 руб."
    df.loc[
        df["quiz_answers4"] == "более 100 000 руб. / более $1400", "quiz_answers4"
    ] = "более 100 000 руб."
    df.loc[
        df["quiz_answers4"] == "до 100 000 руб. / до $1400", "quiz_answers4"
    ] = "до 100 000 руб."
    df.loc[
        df["quiz_answers4"] == "до 30 000 руб. / до $400", "quiz_answers4"
    ] = "до 30 000 руб."
    df.loc[
        df["quiz_answers4"] == "до 60 000 руб. / до $800", "quiz_answers4"
    ] = "до 60 000 руб."

    # Меняем формулировки в столбце "Обучение" на сокращенные (для более удобной визуализции)
    df.loc[
        df["quiz_answers5"] == "Да, если это поможет мне в реализации проекта.",
        "quiz_answers5",
    ] = "Да, проект"
    df.loc[
        df["quiz_answers5"] == "Да, если я точно найду работу после обучения.",
        "quiz_answers5",
    ] = "Да, работа"
    df.loc[
        df["quiz_answers5"] == "Нет. Хочу получить только бесплатные материалы.",
        "quiz_answers5",
    ] = "Нет"

    # Меняем формулировки в столбце "Профессия" на сокращенные (для более удобной визуализции)
    df.loc[
        df["quiz_answers3"]
        == "Профессии связанные с числами (аналитик, бухгалтер, инженер и т.п.)",
        "quiz_answers3",
    ] = "Связано с числами"
    df.loc[
        df["quiz_answers3"]
        == "Гуманитарий (общение с людьми, искусство, медицина и т.д.)",
        "quiz_answers3",
    ] = "Гуманитарий"
    df.loc[
        df["quiz_answers3"]
        == "IT сфера (разработчик, тестировщик, администратор и т.п.)",
        "quiz_answers3",
    ] = "IT сфера"

    df.insert(
        19, "trafficologist", "Неизвестно"
    )  # Добавляем столбец trafficologist для записи имени трафиколога
    # 'Неизвестно' - канал с utm-меткой по которой не определяется трафиколог
    df.insert(
        20, "account", "Неизвестно 1"
    )  # Добавляем столбец account для записи аккаунта трафиколога
    df.insert(21, "target_class", 0)

    with open(os.path.join(RESULTS_FOLDER, "trafficologists.pkl"), "rb") as f:
        traff_data = pkl.load(f)
    # Анализируем ссылки каждого лида на то, какой трафиколог привел этого лида
    links_list = (
        []
    )  # Сохраняем в список ссылки, не содержащие метки аккаунтов (в таком случае неизвестно, кто привел лида)
    for el in list(traff_data["label"]):  # Проходимся по всем метка которые есть
        for i in range(df.shape[0]):  # Проходим по всему датасету
            try:  # Пробуем проверить, есть ли элемент в ссылке
                if el in df.loc[i, "traffic_channel"]:  # Если элемент (метка) есть
                    df.loc[i, "trafficologist"] = traff_data[traff_data["label"] == el][
                        "name"
                    ].values[
                        0
                    ]  # Заносим имя трафиколога по в ячейку по значению метки
                    df.loc[i, "account"] = traff_data[traff_data["label"] == el][
                        "title"
                    ].values[
                        0
                    ]  # Заносим кабинет трафиколога по в ячейку по значению метки
                else:
                    if "utm" not in df.loc[i, "traffic_channel"]:
                        df.loc[
                            i, "account"
                        ] = "Неизвестно 2"  # Канал совсем без utm-метки
            except TypeError:  # Если в ячейке нет ссылки, а проставлен 0
                links_list.append(df.loc[i, "traffic_channel"])

    with open(os.path.join(RESULTS_FOLDER, "crops_list.pkl"), "rb") as f:
        crops_data = pkl.load(f)

    for el in crops_data:  # Проходимся по всем метка которые есть
        for i in range(df.shape[0]):  # Проходим по всему датасету
            try:  # Пробуем проверить, есть ли элемент в ссылке
                if el in df.loc[i, "traffic_channel"]:  # Если элемент (метка) есть
                    df.loc[
                        i, "trafficologist"
                    ] = el  # Заносим имя трафиколога по в ячейку по значению метки
                    df.loc[
                        i, "account"
                    ] = el  # Заносим кабинет трафиколога по в ячейку по значению метки
            except TypeError:  # Если в ячейке нет ссылки, а проставлен 0
                links_list.append(df.loc[i, "traffic_channel"])

    # Добавляем в датасет данные по количеству попаданий лидов в целевую аудиторию
    with open(os.path.join(RESULTS_FOLDER, "target_audience.pkl"), "rb") as f:
        target_audience = pkl.load(f)

    for i in range(df.shape[0]):
        target_class = 0
        if df.loc[i, "quiz_answers1"] in target_audience:
            target_class += 1
        if df.loc[i, "quiz_answers2"] in target_audience:
            target_class += 1
        if df.loc[i, "quiz_answers3"] in target_audience:
            target_class += 1
        if df.loc[i, "quiz_answers4"] in target_audience:
            target_class += 1
        if df.loc[i, "quiz_answers5"] in target_audience:
            target_class += 1
        if df.loc[i, "quiz_answers6"] in target_audience:
            target_class += 1
        df.loc[i, "target_class"] = target_class

    # Добавляем значение метки в ссылке
    df[["utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term"]] = ""
    for i, link in enumerate(df["traffic_channel"]):
        parsed_url = urlparse(link)
        parsed_query = parse_qs(parsed_url.query)
        for k in parsed_query.keys():
            if k in [
                "utm_source",
                "utm_medium",
                "utm_campaign",
                "utm_content",
                "utm_term",
            ]:
                df.loc[i, k] = parsed_query[k][0]
    return df


change_dict = {
    "23 - 30": "24 - 30",
    "25 - 30": "26 - 30",
    "30 - 40": "31 - 40",
    "40 - 50": "41 - 50",
    "50 - 60": "51 - 60",
    "0 руб./ $0": "0 руб.",
    "0руб.": "0 руб.",
    "более 100 000 руб. / более $1400": "более 100 000 руб.",
    "до 100 000 руб. / до $1400": "до 100 000 руб.",
    "Ваш_средний_доход_в_месяц": "до 100 000 руб.",
    "до 30 000 руб. / до $400": "до 30 000 руб.",
    "до 60 000 руб. / до $800": "до 60 000 руб.",
    "Да, если это поможет мне в реализации проекта.": "Да, проект",
    "Да, если я точно найду работу после обучения.": "Да, работа",
    "Нет. Хочу получить только бесплатные материалы.": "Нет",
    "Профессии связанные с числами (аналитик, бухгалтер, инженер и т.п.)": "Связано с числами",
    "Гуманитарий (общение с людьми, искусство, медицина и т.д.)": "Гуманитарий",
    "IT сфера (разработчик, тестировщик, администратор и т.п.)": "IT сфера",
}


def preprocess_target_audience(target_audience):
    for idx, el in enumerate(target_audience):
        if el in change_dict.keys():
            target_audience[idx] = change_dict[el]

    indexes = np.unique(target_audience, return_index=True)[1]
    change_target = [target_audience[index] for index in sorted(indexes)]
    target_audience = change_target
    return target_audience


def calculate_crops_expenses(leads, crops):
    try:
        crops["Метка"] = (
            crops["Ссылка"]
            .str.split("?")
            .apply(lambda x: x[-1])
            .str.split("&")
            .apply(lambda x: x[-1])
        )
        crops["Бюджет"] = crops["Бюджет"].str.replace(" ", "").astype(int)
        crops = crops.groupby(["Метка"], as_index=False)["Бюджет"].sum()
        leads.created_at = pd.to_datetime(leads.created_at)
        for i, row in crops.iterrows():
            label = row["Метка"]
            cost = row["Бюджет"]

            leads_to_correct = leads[leads["traffic_channel"].str.contains(label)]
            if len(leads_to_correct) > 0:
                cost_per_lead = cost / len(leads_to_correct)
                leads.loc[
                    leads["traffic_channel"].str.contains(label), "channel_expense"
                ] = cost_per_lead
        return leads
    except:
        return leads


def calculate_trafficologists_expenses(leads, traff):
    with open(os.path.join(RESULTS_FOLDER, "expenses.json"), "r") as f:
        data = json.load(f)

    leads.created_at = pd.to_datetime(leads.created_at)

    for value in data:
        date = parser.parse(value["dateFrom"])
        for trafficologist, cost in value["items"].items():
            row = traff[traff.roistat_name == trafficologist]
            if len(row) > 0:
                label = row.iloc[0].label
                mask = (
                    leads.traffic_channel.str.contains(label)
                    & (leads.created_at.dt.day == date.day)
                    & (leads.created_at.dt.month == date.month)
                    & (leads.created_at.dt.year == date.year)
                )
                leads_to_correct = leads[mask]
                if len(leads_to_correct) > 0:
                    cost_per_lead = cost / len(leads_to_correct)
                    leads.loc[mask, "channel_expense"] = cost_per_lead
                # Если у лида нет расходов, то в таблицу записать 400 за лида
                # if len(leads_to_correct) > 0:
                #     if cost != 0:
                #         cost_per_lead = cost / len(leads_to_correct)
                #         leads.loc[mask, 'channel_expense'] = cost_per_lead
                #     else:
                #         leads.loc[mask, 'channel_expense'] = 400
    return leads


def get_turnover_on_lead(leads, ca_payment_analytic):
    leads.insert(10, "turnover_on_lead", 0)
    for i in range(ca_payment_analytic.shape[0]):
        if ca_payment_analytic.values[i][1] in [
            "IT сфера",
            "Гуманитарий",
            "Предприниматель, руководитель",
            "Связано с числами",
        ]:
            leads.loc[
                (leads["quiz_answers2"] == ca_payment_analytic.values[i][0])
                & (leads["quiz_answers3"] == ca_payment_analytic.values[i][1])
                & (leads["quiz_answers4"] == ca_payment_analytic.values[i][2])
                & (leads["quiz_answers5"] == ca_payment_analytic.values[i][3]),
                "turnover_on_lead",
            ] = ca_payment_analytic.values[i][4]
        else:
            leads.loc[
                (leads["quiz_answers2"] == ca_payment_analytic.values[i][0])
                & (
                    ~leads["quiz_answers3"].isin(
                        [
                            "IT сфера",
                            "Гуманитарий",
                            "Предприниматель, руководитель",
                            "Связано с числами",
                        ]
                    )
                )
                & (leads["quiz_answers4"] == ca_payment_analytic.values[i][2])
                & (leads["quiz_answers5"] == ca_payment_analytic.values[i][3]),
                "turnover_on_lead",
            ] = ca_payment_analytic.values[i][4]
    return leads


def get_marginality(leads):
    leads["channel_expense2"] = leads["channel_expense"]
    leads.loc[leads["channel_expense2"] == 0, "channel_expense2"] = 400 * 1.2
    return leads


if __name__ == "__main__":
    with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
        leads = pkl.load(f)
    # leads = preprocess_dataframe(leads)
    with open(os.path.join(RESULTS_FOLDER, "ca_payment_analytic.pkl"), "rb") as f:
        ca_payment_analytic = pkl.load(f)
    leads = get_turnover_on_lead(leads, ca_payment_analytic)
    with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "wb") as f:
        pkl.dump(leads, f)
    print(leads)


def recalc_expenses(leads):
    stats = vk_reader("collectStatisticsDataFrame")
    stats.campaign_id = stats.campaign_id.astype(str)
    stats.id = stats.id.astype(str)
    stats.date = stats.date.astype(str)

    leads_ids = leads.copy()
    leads_ids = leads_ids[~leads_ids.campaign_id.isna() & ~leads_ids.ad_id.isna()]
    leads_ids.campaign_id = leads_ids.campaign_id.astype(str)
    leads_ids.ad_id = leads_ids.ad_id.astype(str)
    leads_ids.created_at = (
        pd.to_datetime(leads_ids.created_at).dt.normalize().astype(str)
    )

    for index, item in leads_ids.iterrows():
        stat = stats[
            (stats.campaign_id == item.campaign_id)
            & (stats.id == item.ad_id)
            & (stats.date == item.created_at)
        ]
        if len(stat):
            leads.loc[index, "channel_expense"] = stat.iloc[0].spent

    return leads


def telegram_restructure(leads):
    def restructure(leads, name):
        leads_new = leads[leads.utm_source == name]
        leads_new.utm_campaign = leads_new.utm_source
        leads_new.utm_source = "tg"
        leads[leads.utm_source == name] = leads_new
        return leads

    items = [
        "bookpython",
        "books_java",
        "clear_python",
        "coding_on_java",
        "frontend_tasks",
        "frontend_trends",
        "fullstacker",
        "github_community",
        "habr",
        "java_job",
        "java_learning",
        "java_news",
        "javascript_job",
    ]

    for name in items:
        leads = restructure(leads, name)

    return leads
