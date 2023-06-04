import numpy as np
import pandas as pd

from typing import Dict

from flask import current_app


def calculate(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    df.reset_index(inplace=True, drop=True)

    pickle_app = current_app.app("pickle")

    traff_data = pickle_app.load("trafficologists")
    status = pickle_app.load("statuses")
    status.fillna(0, inplace=True)

    payment_status = (
        status[status["Продажа"] == " +"]["Статус АМО"].unique().tolist()
    )  # Статусы с этапом "Продажа"
    conversation_status = (
        status[status["Дозвон"] == " +"]["Статус АМО"].unique().tolist()
    )  # Статусы с этапом "Дозвон"
    progress_status = (
        status[status["В работе"] == " +"]["Статус АМО"].unique().tolist()
    )  # Статусы с этапом "В работе"
    offer_status = (
        status[status["Оффер"] == " +"]["Статус АМО"].unique().tolist()
    )  # Статусы с этапом "Оффер"
    bill_status = (
        status[status["Счет"] == " +"]["Статус АМО"].unique().tolist()
    )  # Статусы с этапом "Счет"
    done_status = (
        status[status["Обработанная заявка"] == " +"]["Статус АМО"].unique().tolist()
    )  # Статусы с этапом "Обработанная заявка"

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

    columns = [
        "Кол-во лидов",
        "% в работе",
        "% дозвонов",
        "% офферов",
        "% счетов",
        "% продаж",
        "Цена лида",
        "% ЦА 5-6",
        "% ЦА 5",
        "% ЦА 6",
        "% ЦА 4-5-6",
        "% ЦА 4",
        "% не ЦА",
        "% ЦА 3",
        "% ЦА 2",
        "% ЦА 1",
        "Цена ЦА 5-6",
        "Цена ЦА 4-5-6",
        "Средний чек",
        "Оборот",
        "Бюджет",
        "Расход на ОП",
        "Расход общий (ОП+бюджет)",
        "% расходов на трафик (доля от Расход общий)",
        "% расходов на ОП (доля от Расход общий)",
        "ROI ?",
        "ROMI",
        "ROSI",
        "ROI",
        "Маржа",
        "Маржа %",
        "Цена Разговора",
        "Цена Оффера",
        "Цена Счета",
        "Оборот на лида (на обработанного лида)",
        "Оборот на разговор",
        "CV обр.лид/оплата",
        "CV разговор/оплата",
        "CPO",
        "% лидов (доля от общего)",
        "% Оборот (доля от общего)",
        "% Оплат (доля от общего)",
        "% Затрат (доля от общего)",
    ]

    data_category = np.zeros((len(created_columns), len(columns))).astype("int")
    output_df = pd.DataFrame(
        data_category, columns=columns
    )  # Датасет для процентного кол-ва лидов
    output_df.insert(
        0, "Источник", 0
    )  # Столбец названия подкатегорий указанной категории

    for i, subcategory_name in enumerate(created_columns):
        output_df.loc[i, "Источник"] = subcategory_name
        temp_df = df[
            (df["trafficologist"] == subcategory_name)
            | (df["account"] == subcategory_name)
        ]
        finished_leads = float(
            temp_df[temp_df["status_amo"].isin(done_status)].shape[0]
        )
        if temp_df.shape[0] != 0:
            output_df.loc[i, "Кол-во лидов"] = temp_df.shape[0]

            output_df.loc[i, "% в работе"] = round(
                temp_df[temp_df["status_amo"].isin(progress_status)].shape[0]
                / temp_df.shape[0]
                * 100,
                1,
            )
            output_df.loc[i, "% дозвонов"] = round(
                temp_df[temp_df["status_amo"].isin(conversation_status)].shape[0]
                / temp_df.shape[0]
                * 100,
                1,
            )
            output_df.loc[i, "% офферов"] = round(
                temp_df[temp_df["status_amo"].isin(offer_status)].shape[0]
                / temp_df.shape[0]
                * 100,
                1,
            )
            output_df.loc[i, "% счетов"] = round(
                temp_df[temp_df["status_amo"].isin(bill_status)].shape[0]
                / temp_df.shape[0]
                * 100,
                1,
            )
            output_df.loc[i, "% продаж"] = round(
                temp_df[temp_df["status_amo"].isin(payment_status)].shape[0]
                / temp_df.shape[0]
                * 100,
                1,
            )

            output_df.loc[i, "Цена лида"] = round(
                temp_df["channel_expense"].sum() / temp_df.shape[0], 1
            )

            output_df.loc[i, "% ЦА 5-6"] = round(
                temp_df[temp_df["target_class"].isin([5, 6])].shape[0]
                / temp_df.shape[0]
                * 100,
                1,
            )
            output_df.loc[i, "% ЦА 5"] = round(
                temp_df[temp_df["target_class"] == 5].shape[0] / temp_df.shape[0] * 100,
                1,
            )
            output_df.loc[i, "% ЦА 6"] = round(
                temp_df[temp_df["target_class"] == 6].shape[0] / temp_df.shape[0] * 100,
                1,
            )
            output_df.loc[i, "% ЦА 4-5-6"] = round(
                temp_df[temp_df["target_class"].isin([4, 5, 6])].shape[0]
                / temp_df.shape[0]
                * 100,
                1,
            )
            output_df.loc[i, "% ЦА 4"] = round(
                temp_df[temp_df["target_class"] == 4].shape[0] / temp_df.shape[0] * 100,
                1,
            )
            output_df.loc[i, "% не ЦА"] = round(
                temp_df[temp_df["target_class"] == 0].shape[0] / temp_df.shape[0] * 100,
                1,
            )
            output_df.loc[i, "% ЦА 3"] = round(
                temp_df[temp_df["target_class"] == 3].shape[0] / temp_df.shape[0] * 100,
                1,
            )
            output_df.loc[i, "% ЦА 2"] = round(
                temp_df[temp_df["target_class"] == 2].shape[0] / temp_df.shape[0] * 100,
                1,
            )
            output_df.loc[i, "% ЦА 1"] = round(
                temp_df[temp_df["target_class"] == 1].shape[0] / temp_df.shape[0] * 100,
                1,
            )
            output_df.loc[i, "Цена ЦА 5-6"] = (
                temp_df[temp_df["target_class"].isin([5, 6])]["channel_expense"].sum()
                / temp_df[temp_df["target_class"].isin([5, 6])].shape[0]
                if temp_df[temp_df["target_class"].isin([5, 6])].shape[0] != 0
                else 0
            )
            output_df.loc[i, "Цена ЦА 4-5-6"] = (
                temp_df[temp_df["target_class"].isin([4, 5, 6])][
                    "channel_expense"
                ].sum()
                / temp_df[temp_df["target_class"].isin([4, 5, 6])].shape[0]
                if temp_df[temp_df["target_class"].isin([4, 5, 6])].shape[0] != 0
                else 0
            )

            output_df.loc[i, "Средний чек"] = (
                round(
                    temp_df["payment_amount"].sum()
                    / temp_df[temp_df["payment_amount"] != 0].shape[0],
                    1,
                )
                if temp_df[temp_df["payment_amount"] != 0].shape[0] != 0
                else 0
            )
            output_df.loc[i, "Оборот"] = round(
                float(temp_df["payment_amount"].sum()), 1
            )
            output_df.loc[i, "Бюджет"] = round(
                float(temp_df["channel_expense"].sum()), 1
            )
            output_df.loc[i, "Расход на ОП"] = round(
                float(output_df.loc[i, "Оборот"]) * 0.06
                + 150 * float(temp_df[temp_df["payment_amount"] != 0].shape[0])
                + 27 * float(temp_df[temp_df["payment_amount"] != 0].shape[0])
                + 20 * float(temp_df[temp_df["payment_amount"] != 0].shape[0]) * 1.4,
                1,
            )

            output_df.loc[i, "Расход общий (ОП+бюджет)"] = float(
                output_df.loc[i, "Бюджет"]
            ) + float(output_df.loc[i, "Расход на ОП"])

            output_df.loc[i, "% расходов на трафик (доля от Расход общий)"] = (
                round(
                    float(output_df.loc[i, "Бюджет"])
                    / output_df.loc[i, "Расход общий (ОП+бюджет)"]
                    * 100,
                    1,
                )
                if output_df.loc[i, "Расход общий (ОП+бюджет)"] != 0
                else 0
            )

            output_df.loc[i, "% расходов на ОП (доля от Расход общий)"] = (
                round(
                    output_df.loc[i, "Расход на ОП"]
                    / output_df.loc[i, "Расход общий (ОП+бюджет)"]
                    * 100,
                    1,
                )
                if output_df.loc[i, "Расход общий (ОП+бюджет)"] != 0
                else 0
            )

            output_df.loc[i, "ROI ?"] = (
                round(
                    (
                        float(temp_df["payment_amount"].sum())
                        - float(temp_df["channel_expense"].sum())
                    )
                    / float(temp_df["channel_expense"].sum())
                    * 100,
                    1,
                )
                if temp_df["channel_expense"].sum() != 0
                else 0
            )

            output_df.loc[i, "ROMI"] = round(
                (output_df.loc[i, "Оборот"] / output_df.loc[i, "Бюджет"]) - 1, 1
            )
            output_df.loc[i, "ROSI"] = (
                round(
                    (output_df.loc[i, "Оборот"] / output_df.loc[i, "Расход на ОП"]) - 1,
                    1,
                )
                if output_df.loc[i, "Расход на ОП"] != 0
                else -1
            )
            output_df.loc[i, "ROI"] = (
                round(
                    (
                        output_df.loc[i, "Оборот"]
                        / output_df.loc[i, "Расход общий (ОП+бюджет)"]
                    )
                    - 1,
                    1,
                )
                if output_df.loc[i, "Расход общий (ОП+бюджет)"] != 0
                else -1
            )

            output_df.loc[i, "Маржа"] = round(
                float(temp_df["payment_amount"].sum())
                - float(output_df.loc[i, "Расход общий (ОП+бюджет)"]),
                1,
            )
            output_df.loc[i, "Маржа %"] = (
                round(
                    float(output_df.loc[i, "Маржа"])
                    / float(output_df.loc[i, "Оборот"]),
                    1,
                )
                if output_df.loc[i, "Оборот"] != 0
                else 0
            )

            output_df.loc[i, "Цена Разговора"] = (
                round(
                    temp_df["channel_expense"].sum()
                    / temp_df[temp_df["status_amo"].isin(conversation_status)].shape[0],
                    1,
                )
                if temp_df[temp_df["status_amo"].isin(conversation_status)].shape[0]
                != 0
                else 0
            )

            output_df.loc[i, "Цена Оффера"] = (
                round(
                    temp_df["channel_expense"].sum()
                    / temp_df[temp_df["status_amo"].isin(offer_status)].shape[0],
                    1,
                )
                if temp_df[temp_df["status_amo"].isin(offer_status)].shape[0] != 0
                else 0
            )

            output_df.loc[i, "Цена Счета"] = (
                round(
                    temp_df["channel_expense"].sum()
                    / temp_df[temp_df["status_amo"].isin(bill_status)].shape[0],
                    1,
                )
                if temp_df[temp_df["status_amo"].isin(bill_status)].shape[0] != 0
                else 0
            )

            output_df.loc[i, "Оборот на лида (на обработанного лида)"] = (
                round(float(output_df.loc[i, "Оборот"]) / finished_leads, 1)
                if finished_leads != 0
                else 0
            )

            output_df.loc[i, "Оборот на разговор"] = (
                round(
                    float(output_df.loc[i, "Оборот"])
                    / temp_df[temp_df["status_amo"].isin(conversation_status)].shape[0],
                    1,
                )
                if temp_df[temp_df["status_amo"].isin(conversation_status)].shape[0]
                != 0
                else 0
            )

            output_df.loc[i, "CV обр.лид/оплата"] = (
                round(
                    finished_leads / temp_df[temp_df["payment_amount"] != 0].shape[0], 1
                )
                if temp_df[temp_df["payment_amount"] != 0].shape[0] != 0
                else 0
            )

            output_df.loc[i, "CV разговор/оплата"] = (
                round(
                    temp_df[temp_df["status_amo"].isin(conversation_status)].shape[0]
                    / temp_df[temp_df["payment_amount"] != 0].shape[0],
                    1,
                )
                if temp_df[temp_df["payment_amount"] != 0].shape[0] != 0
                else 0
            )

            output_df.loc[i, "CPO"] = (
                round(
                    output_df.loc[i, "Расход общий (ОП+бюджет)"]
                    / temp_df[temp_df["status_amo"].isin(payment_status)].shape[0],
                    1,
                )
                if temp_df[temp_df["status_amo"].isin(payment_status)].shape[0] != 0
                else 0
            )

            output_df.loc[i, "% лидов (доля от общего)"] = round(
                temp_df.shape[0] / df.shape[0] * 100, 1
            )
            output_df.loc[i, "% Оборот (доля от общего)"] = (
                round(
                    float(temp_df["payment_amount"].sum())
                    / float(df["payment_amount"].sum()),
                    1,
                )
                if df["payment_amount"].sum() != 0
                else 0
            )
            output_df.loc[i, "% Оплат (доля от общего)"] = (
                round(
                    temp_df[temp_df["status_amo"].isin(payment_status)].shape[0]
                    / df[df["status_amo"].isin(payment_status)].shape[0],
                    1,
                )
                if df[df["status_amo"].isin(payment_status)].shape[0] != 0
                else 0
            )

            output_df.loc[i, "% Затрат (доля от общего)"] = (
                round(
                    float(output_df.loc[i, "Расход общий (ОП+бюджет)"])
                    / (
                        float(df["channel_expense"].sum())
                        + (
                            (
                                float(df["payment_amount"].sum()) * 0.06
                                + 150 * float(df[df["payment_amount"] != 0].shape[0])
                                + 27 * float(df[df["payment_amount"] != 0].shape[0])
                                + 20 * float(df[df["payment_amount"] != 0].shape[0])
                            )
                            * 1.4
                        )
                    )
                    * 100,
                    1,
                )
                if (
                    float(df["channel_expense"].sum())
                    + (
                        (
                            float(df["payment_amount"].sum()) * 0.6
                            + 150 * float(df[df["payment_amount"] != 0].shape[0])
                            + 27 * float(df[df["payment_amount"] != 0].shape[0])
                            + 20 * float(df[df["payment_amount"] != 0].shape[0])
                        )
                        * 1.4
                    )
                    != 0
                )
                else 0
            )
        else:
            pass
    return {"Источники трафика": output_df}
