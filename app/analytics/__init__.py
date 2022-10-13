from app import app
from .table_loaders import get_clusters, get_segments, get_landings, get_turnover
from .table_loaders import get_leads_ta_stats, get_segments_stats, get_traffic_sources
from .table_loaders import (
    get_channels,
    get_channels_summary,
    get_channels_detailed,
    get_payments_accumulation,
)
from .table_loaders import get_marginality, get_audience_type, get_audience_type_percent

from app.tables import (
    calculate_clusters,
    calculate_segments,
    calculate_landings,
    calculate_traffic_sources,
)
from app.tables import (
    calculate_turnover,
    calculate_leads_ta_stats,
    calculate_segments_stats,
)
from app.tables import calculate_channels_summary, calculate_channels_detailed
from app.tables import calculate_channels_summary_detailed, additional_table
from app.tables.audience_type import (
    calculate_audience_tables_by_date,
    calculate_audience_type_result,
    calculate_audience_type_percent_result,
)
from config import config
from config import RESULTS_FOLDER

import os
import re
from transliterate import slugify
import numpy as np
from flask import render_template, request, redirect, Response
from datetime import datetime, timedelta
from urllib.parse import urlencode
import requests
import pandas as pd
import httplib2
import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials
import pickle as pkl
import json
import html

from app import views

from .pickle_load import PickleLoader


pickle_loader = PickleLoader()


@app.context_processor
def utility_processor():
    def is_tuple(value) -> bool:
        return isinstance(value, tuple)

    return dict(is_tuple=is_tuple)


@app.route("/getPlotCSV")
def getPlotCSV():
    marginality = request.args.get("value")
    if ", " in marginality:
        report, subname = marginality.split(", ")
        with open(os.path.join(RESULTS_FOLDER, report + ".pkl"), "rb") as f:
            df = pkl.load(f)
            df = df[subname]
        return Response(
            df.to_csv(),
            mimetype="text/csv",
            headers={"Content-disposition": f"attachment; filename={report}.csv"},
        )
    else:
        report = marginality
        with open(os.path.join(RESULTS_FOLDER, report + ".pkl"), "rb") as f:
            df = pkl.load(f)
        return Response(
            df.to_csv(),
            mimetype="text/csv",
            headers={"Content-disposition": f"attachment; filename={report}.csv"},
        )


def get_table_one_campaign(campaign, column_unique, table, **kwargs):
    table.created_at = pd.to_datetime(table.created_at).dt.normalize()
    table = table[table[column_unique] == campaign]

    date_end = kwargs.get("date_end")
    utm_source = kwargs.get("utm_source")
    source = kwargs.get("source")
    only_ru = kwargs.get("only_ru")

    date_end_filter = (
        datetime.strptime(date_end, "%Y-%m-%d") if date_end else datetime.now()
    )
    table = table[table.created_at >= date_end_filter - timedelta(days=30)]
    table = table[table.created_at <= date_end_filter]

    if utm_source:
        table = table[table["utm_source"] == utm_source]
    elif source:
        table = table[table["account"] == source]

    if only_ru:
        table = table[table["quiz_answers1"] == "Россия"]

    return table


@app.route("/channels_summary", methods=["GET", "POST"])
def channels_summary():
    columns_dict = {
        "Канал": "canal",
        "Лидов": "lead",
        "Оборот*": "turnover",
        "Оборот на лида": "turnover_on_lead",
        "Трафик": "traffic",
        "Остальное": "other",
        "Прибыль": "profit",
        "ROI": "roi",
        "Маржинальность": "marginality",
        "Цена лида": "lead_cost",
    }

    date_start = request.args.get("date_start", default="")
    date_end = request.args.get("date_end", default="")
    utm_source = request.args.get("utm_source", default="")  # Значение utm_source
    source = request.args.get("source", default="")  # Имя канала
    utm_2 = request.args.get("utm_2")  # Значение utm для разбивки
    only_ru = bool(request.args.get("only_ru"))
    column = request.args.get("sort")

    try:
        # Если мини-форма по кнопке в столбце возвращает значение
        utm_2_value = request.args.get("channel")[2:]
        # Загружаем текущую разбивку по лидам для повторного отображения
        with open(
            os.path.join(RESULTS_FOLDER, "current_channel_summary.pkl"), "rb"
        ) as f:
            tables = pkl.load(f)
        # Загружаем отфильтрованную ранее базу лидов для расчета
        with open(os.path.join(RESULTS_FOLDER, "current_leads.pkl"), "rb") as f:
            table = pkl.load(f)

        with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
            unique_sources = pkl.load(f)
            unique_sources.created_at = pd.to_datetime(
                unique_sources.created_at
            ).dt.normalize()
            if date_start:
                unique_sources = unique_sources[unique_sources.created_at >= date_start]
            if date_end:
                unique_sources = unique_sources[unique_sources.created_at <= date_end]
            unique_sources = unique_sources["trafficologist"].unique().tolist()

        # Загружаем значения фильтров
        # print(os.path.join(RESULTS_FOLDER, "filter_data.txt"))
        # with open(os.path.join(RESULTS_FOLDER, "filter_data.txt"), "r") as f:
        #     filter_data = json.load(f)
        # utm_source = filter_data["utms"]["utm_source"]
        # source = filter_data["utms"]["source"]
        # utm_2 = filter_data["utms"]["utm_2"]

        filter_data = {
            "filter_dates": {"date_start": date_start, "date_end": date_end},
            "utms": {
                "utm_source": utm_source,
                "source": source,
                "utm_2": utm_2 or "",
                "utm_2_value": utm_2_value,
            },
            "unique_sources": [""] + unique_sources,
            "column": column,
            "columns_dict": columns_dict,
            "only_ru": only_ru,
        }

        channels_summary_detailed = calculate_channels_summary_detailed(
            table, utm_source, source, utm_2, utm_2_value, only_ru
        )
        channels_summary_detailed_df = channels_summary_detailed["1"]
        additional_df = channels_summary_detailed["2"]
        utms2 = ["", "utm_campaign", "utm_medium", "utm_content", "utm_term"]
        return render_template(
            "channels_summary.html",
            tables=tables,
            date_start=date_start,
            date_end=date_end,
            channels_summary_detailed_df=channels_summary_detailed_df,
            additional_df=additional_df,
            enumerate=enumerate,
            utm_2_value=utm_2_value,
            utms2=utms2,
            only_ru=only_ru,
            filter_data=filter_data,
        )
    # Если жмем на общую фильтрацию
    except TypeError as _e:
        # Значение доп. кнопки - пустое - загружаем таблицу лидов
        with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
            table = pkl.load(f)

        # Список уникальных трафиколгов
        unique_sources = [""] + table["trafficologist"].unique().tolist()
        utm_2_value = ""

        # Список меток для разбивки
        utms2 = ["", "utm_campaign", "utm_medium", "utm_content", "utm_term"]

        filter_data = {
            "filter_dates": {"date_start": date_start, "date_end": date_end},
            "utms": {
                "utm_source": utm_source,
                "source": source,
                "utm_2": utm_2 or "",
                "utm_2_value": utm_2_value,
            },
            "unique_sources": unique_sources,
            "column": column,
            "columns_dict": columns_dict,
            "only_ru": only_ru,
        }

        # if date_start or date_end or utm_source or source or utm_2:
        column_unique = utm_2 or "trafficologist"

        # table.date_request = pd.to_datetime(table.date_request).dt.normalize()  # Переводим столбец sent в формат даты
        table.created_at = pd.to_datetime(table.created_at).dt.normalize()
        if date_start:
            table = table[table.created_at >= datetime.strptime(date_start, "%Y-%m-%d")]
        if date_end:
            table = table[table.created_at <= datetime.strptime(date_end, "%Y-%m-%d")]
        unique_sources = table["trafficologist"].unique()

        if utm_source:
            table = table[table["utm_source"] == utm_source]
        elif source:
            table = table[table["trafficologist"] == source]

        if only_ru:
            table = table[table["quiz_answers1"] == "Россия"]

        if len(table) == 0:
            return render_template(
                "channels_summary.html",
                filter_data=filter_data,
                utms2=utms2,
                only_ru=only_ru,
                additional_df="",
                error="Нет данных для заданного периода",
                channels_summary_detailed_df="",
            )

        # Get month data for every channel
        with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
            table_month_data = pkl.load(f)
        data_month = {}
        for campaign in table[column_unique].unique():
            data_month[campaign] = get_table_one_campaign(
                campaign,
                column_unique,
                table_month_data.copy(True),
                date_start=date_start,
                date_end=date_end,
                utm_source=utm_source,
                source=source,
                only_ru=only_ru,
            )

        tables = calculate_channels_summary(
            table, column_unique=column_unique, data_month=data_month
        )

        # Сохраняем значения полей в списке
        filter_data = {
            "filter_dates": {"date_start": date_start, "date_end": date_end},
            "utms": {
                "utm_source": utm_source,
                "source": source,
                "utm_2": utm_2 or "",
                "utm_2_value": utm_2_value,
            },
            "unique_sources": [""] + unique_sources.tolist(),
            "column": column,
            "columns_dict": columns_dict,
            "only_ru": only_ru,
        }
        with open(os.path.join(RESULTS_FOLDER, "filter_data.txt"), "w") as f:
            json.dump(filter_data, f)
        with open(os.path.join(RESULTS_FOLDER, "current_leads.pkl"), "wb") as f:
            pkl.dump(table, f)
        with open(
            os.path.join(RESULTS_FOLDER, "current_channel_summary.pkl"), "wb"
        ) as f:
            pkl.dump(tables, f)

        return render_template(
            "channels_summary.html",
            tables=tables,
            date_start=date_start,
            date_end=date_end,
            utms2=utms2,
            only_ru=only_ru,
            enumerate=enumerate,
            additional_df="",
            channels_summary_detailed_df="",
            filter_data=filter_data,
        )

        # # Список уникальных трафиколгов
        # unique_sources = [""] + table["trafficologist"].unique().tolist()
        #
        # # Сохраняем значения полей в списке
        # filter_data = {
        #     "filter_dates": {"date_start": date_start, "date_end": date_end},
        #     "utms": {
        #         "utm_source": utm_source,
        #         "source": source,
        #         "utm_2": utm_2 or "",
        #         "utm_2_value": utm_2_value,
        #     },
        #     "unique_sources": unique_sources,
        #     "column": column,
        #     "columns_dict": columns_dict,
        # }
        #
        # tables = get_channels_summary()
        # with open(os.path.join(RESULTS_FOLDER, "current_leads.pkl"), "wb") as f:
        #     pkl.dump(table, f)
        # with open(
        #     os.path.join(RESULTS_FOLDER, "current_channel_summary.pkl"), "wb"
        # ) as f:
        #     pkl.dump(tables, f)
        # return render_template(
        #     "channels_summary.html",
        #     tables=tables,
        #     utms2=utms2,
        #     enumerate=enumerate,
        #     channels_summary_detailed_df="",
        #     additional_df="",
        #     filter_data=filter_data,  # date_start=date_start, date_end=date_end
        # )


@app.route("/channels_detailed")
def channels_detailed():
    tab = request.args.get("tab")
    tables = get_channels_detailed()
    return render_template("channels_detailed.html", tables=tables, tab=tab)


app.add_url_rule("/channels", view_func=views.ChannelsView.as_view("channels"))


@app.route("/payments_accumulation")
def payments_accumulation():
    tab = request.args.get("tab")
    tables = get_payments_accumulation()
    return render_template("payments_accumulation.html", tables=tables, tab=tab)


@app.route("/marginality")
def marginality():
    tab = request.args.get("tab")
    tables = get_marginality()
    return render_template("marginality.html", tables=tables, tab=tab)


@app.route("/audience_type")
def audience_type():
    tab = request.args.get("tab")
    date_start = request.args.get("date_start")
    date_end = request.args.get("date_end")
    utm = []
    utm_value = []
    for i in range(10):
        utm_temp = request.args.get("utm_" + str(i))
        if utm_temp is None:
            utm_temp = ""

        utm_value_temp = request.args.get("utm_value_" + str(i))
        if utm_value_temp is None:
            utm_value_temp = ""

        utm.append(utm_temp)
        utm_value.append(utm_value_temp)

    utm_unique = np.unique(utm)
    utm_value_unique = np.unique(utm_value)

    if date_start or date_end or (len(utm_unique) != 1) or (len(utm_value_unique) != 1):
        with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
            table = pkl.load(f)
        # table.date_request = pd.to_datetime(table.date_request).dt.normalize()  # Переводим столбец sent в формат даты
        table.created_at = pd.to_datetime(table.created_at).dt.normalize()
        if date_start:
            table = table[table.created_at >= datetime.strptime(date_start, "%Y-%m-%d")]
        if date_end:
            table = table[table.created_at <= datetime.strptime(date_end, "%Y-%m-%d")]
        for i in range(10):
            if (utm[i] != [""]) or (utm_value[i] != [""]):
                el = utm[i] + "=" + utm_value[i]
                table = table[table["traffic_channel"].str.contains(el)]
        if len(table) == 0:
            return render_template(
                "audience_type.html", error="Нет данных для заданного периода"
            )
        table = calculate_audience_tables_by_date(table)
        tables = calculate_audience_type_result(table)
        return render_template(
            "audience_type.html",
            tables=tables,
            date_start=date_start,
            date_end=date_end,
            tab=tab
            # utm=utm, utm_value=utm_value
        )
    tables = get_audience_type()
    return render_template("audience_type.html", tables=tables, tab=tab)


@app.route("/audience_type_percent")
def audience_type_percent():
    tab = request.args.get("tab")
    date_start = request.args.get("date_start")
    date_end = request.args.get("date_end")
    utm = []
    utm_value = []
    for i in range(10):
        utm_temp = request.args.get("utm_" + str(i))
        if utm_temp is None:
            utm_temp = ""

        utm_value_temp = request.args.get("utm_value_" + str(i))
        if utm_value_temp is None:
            utm_value_temp = ""

        utm.append(utm_temp)
        utm_value.append(utm_value_temp)

    utm_unique = np.unique(utm)
    utm_value_unique = np.unique(utm_value)

    if date_start or date_end or (len(utm_unique) != 1) or (len(utm_value_unique) != 1):
        with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
            table = pkl.load(f)
        # table.date_request = pd.to_datetime(table.date_request).dt.normalize()  # Переводим столбец sent в формат даты
        table.created_at = pd.to_datetime(table.created_at).dt.normalize()
        if date_start:
            table = table[table.created_at >= datetime.strptime(date_start, "%Y-%m-%d")]
        if date_end:
            table = table[table.created_at <= datetime.strptime(date_end, "%Y-%m-%d")]
        for i in range(10):
            if (utm[i] != [""]) or (utm_value[i] != [""]):
                el = utm[i] + "=" + utm_value[i]
                table = table[table["traffic_channel"].str.contains(el)]
        if len(table) == 0:
            return render_template(
                "audience_type_percent.html", error="Нет данных для заданного периода"
            )
        table = calculate_audience_tables_by_date(table)
        tables = calculate_audience_type_percent_result(table)
        return render_template(
            "audience_type_percent.html",
            tables=tables,
            date_start=date_start,
            date_end=date_end,
            tab=tab
            # utm=utm, utm_value=utm_value
        )
    tables = get_audience_type_percent()
    return render_template("audience_type_percent.html", tables=tables, tab=tab)


@app.route("/segments")
def segments():
    date_start = request.args.get("date_start")
    date_end = request.args.get("date_end")

    if date_start or date_end:
        with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
            table = pkl.load(f)
        table.created_at = pd.to_datetime(table.created_at).dt.normalize()
        if date_start:
            table = table[table.created_at >= datetime.strptime(date_start, "%Y-%m-%d")]
        if date_end:
            table = table[table.created_at <= datetime.strptime(date_end, "%Y-%m-%d")]
        if len(table) == 0:
            return render_template(
                "segments.html", error="Нет данных для заданного периода"
            )
        tables = calculate_segments(table)
        return render_template(
            "segments.html", tables=tables, date_start=date_start, date_end=date_end
        )
    tables = get_segments()
    return render_template(
        "segments.html",
        tables=tables,  # date_start=date_start, date_end=date_end
    )


@app.route("/turnover")
def turnover():
    date_request_start = request.args.get("date_request_start")
    date_request_end = request.args.get("date_request_end")
    date_payment_start = request.args.get("date_payment_start")
    date_payment_end = request.args.get("date_payment_end")
    tab = request.args.get("tab")
    if date_request_start or date_request_end or date_payment_start or date_payment_end:
        with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
            table = pkl.load(f)
        table.created_at = pd.to_datetime(table.created_at).dt.normalize()
        table.date_payment = pd.to_datetime(table.date_payment).dt.normalize()
        if date_request_start:
            table = table[
                table.created_at >= datetime.strptime(date_request_start, "%Y-%m-%d")
            ]
        if date_request_end:
            table = table[
                table.created_at <= datetime.strptime(date_request_end, "%Y-%m-%d")
            ]
        if date_payment_start:
            table = table[
                table.date_payment >= datetime.strptime(date_payment_start, "%Y-%m-%d")
            ]
        if date_payment_end:
            table = table[
                table.date_payment <= datetime.strptime(date_payment_end, "%Y-%m-%d")
            ]
        if len(table) == 0:
            return render_template(
                "turnover.html", error="Нет данных для заданного периода"
            )
        # return render_template(
        #     'turnover.html',
        #     error='Not enough data',
        #     date_request_start=date_request_start,
        #     date_request_end=date_request_end,
        #     date_payment_start=date_payment_start,
        #     date_payment_end=date_payment_end,
        #     tab=tab
        #     )
        tables = calculate_turnover(table)
        return render_template(
            "turnover.html",
            tables=tables,
            # date_request_start=date_request_start,
            # date_request_end=date_request_end,
            # date_payment_start=date_payment_start,
            # date_payment_end=date_payment_end,
            tab=tab,
        )
    tables = get_turnover()

    return render_template(
        "turnover.html",
        tables=tables,
        # date_request_start=date_request_start,
        # date_request_end=date_request_end,
        # date_payment_start=date_payment_start,
        # date_payment_end=date_payment_end,
        tab=tab,
    )


@app.route("/clusters")
def clusters():
    date_start = request.args.get("date_start")
    date_end = request.args.get("date_end")
    tab = request.args.get("tab")
    if date_start or date_end:
        with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
            table = pkl.load(f)
        table.created_at = pd.to_datetime(table.created_at).dt.normalize()
        if date_start:
            table = table[table.created_at >= datetime.strptime(date_start, "%Y-%m-%d")]
        if date_end:
            table = table[table.created_at <= datetime.strptime(date_end, "%Y-%m-%d")]
        if len(table) == 0:
            return render_template(
                "clusters.html",
                error="Not enough data",
                date_start=date_start,
                date_end=date_end,
                tab=tab,
            )
        tables = calculate_clusters(table)
        return render_template(
            "clusters.html",
            tables=tables,
            # date_start=date_start, date_end=date_end,
            tab=tab,
        )
    tables = get_clusters()
    return render_template(
        "clusters.html",
        tables=tables,
        # date_start=date_start, date_end=date_end,
        tab=tab,
    )


@app.route("/traffic_sources")
def traffic_sources():
    date_start = request.args.get("date_start")
    date_end = request.args.get("date_end")
    tab = request.args.get("tab")
    if date_start or date_end:
        with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
            table = pkl.load(f)
        table.created_at = pd.to_datetime(table.created_at).dt.normalize()
        if date_start:
            table = table[table.created_at >= datetime.strptime(date_start, "%Y-%m-%d")]
        if date_end:
            table = table[table.created_at <= datetime.strptime(date_end, "%Y-%m-%d")]
        if len(table) == 0:
            return render_template(
                "traffic_sources.html",
                error="Not enough data",
                date_start=date_start,
                date_end=date_end,
                tab=tab,
            )
        table = calculate_traffic_sources(table)
        return render_template(
            "traffic_sources.html",
            tables=table,
            tab=tab,
            date_start=date_start,
            date_end=date_end,
        )
    tables = get_traffic_sources()
    return render_template(
        "traffic_sources.html",
        tables=tables,
        tab=tab,
        # date_start=date_start, date_end=date_end
    )


@app.route("/segments_stats")
def segments_stats():
    date_start = request.args.get("date_start")
    date_end = request.args.get("date_end")
    tab = request.args.get("tab")
    if date_start or date_end:
        with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
            table = pkl.load(f)
        table.created_at = pd.to_datetime(table.created_at).dt.normalize()
        if date_start:
            table = table[table.created_at >= datetime.strptime(date_start, "%Y-%m-%d")]
        if date_end:
            table = table[table.created_at <= datetime.strptime(date_end, "%Y-%m-%d")]
        if len(table) == 0:
            return render_template(
                "segments_stats.html",
                error="Not enough data",
                tab=tab,
                date_start=date_start,
                date_end=date_end,
            )
        tables = calculate_segments_stats(table)
        return render_template(
            "segments_stats.html",
            tables=tables,
            tab=tab,
            date_start=date_start,
            date_end=date_end,
        )
    tables = get_segments_stats()
    return render_template(
        "segments_stats.html",
        tables=tables,
        tab=tab,
        # date_start=date_start, date_end=date_end
    )


@app.route("/leads_ta_stats")
def leads_ta_stats():
    date_start = request.args.get("date_start")
    date_end = request.args.get("date_end")
    if date_start or date_end:
        with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
            table = pkl.load(f)
        table.created_at = pd.to_datetime(table.created_at).dt.normalize()
        if date_start:
            table = table[table.created_at >= datetime.strptime(date_start, "%Y-%m-%d")]
        if date_end:
            table = table[table.created_at <= datetime.strptime(date_end, "%Y-%m-%d")]
        if len(table) == 0:
            return render_template(
                "leads_ta_stats.html",
                error="Not enough data",
                date_start=date_start,
                date_end=date_end,
            )
        table = calculate_leads_ta_stats(table)
        return render_template(
            "leads_ta_stats.html", table=table, date_start=date_start, date_end=date_end
        )
    table = get_leads_ta_stats()
    return render_template("leads_ta_stats.html", table=table)


@app.route("/landings")
def landings():
    date_start = request.args.get("date_start")
    date_end = request.args.get("date_end")
    if date_start or date_end:
        with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
            table = pkl.load(f)
        table.created_at = pd.to_datetime(table.created_at).dt.normalize()
        if date_start:
            table = table[table.created_at >= datetime.strptime(date_start, "%Y-%m-%d")]
        if date_end:
            table = table[table.created_at <= datetime.strptime(date_end, "%Y-%m-%d")]
        if len(table) == 0:
            return render_template(
                "landings.html", error="Нет данных для заданного периода"
            )
        table = calculate_landings(table)
        return render_template(
            "landings.html", tables=table, date_start=date_start, date_end=date_end
        )
    tables = get_landings()
    return render_template(
        "landings.html",
        tables=tables,
        # date_start=date_start, date_end=date_end
    )


@app.route("/vacancies")
def vacancies():
    redirect_uri = "https://analytic.neural-university.ru/login/hh"
    client_id = config["hh"]["client_id"]
    params = urlencode(
        [
            ("response_type", "code"),
            ("client_id", client_id),
            ("redirect_uri", redirect_uri),
        ]
    )
    url = "https://hh.ru/oauth/authorize?" + params
    return redirect(url)


app.add_url_rule(
    "/statistics",
    view_func=views.StatisticsView.as_view("statistics"),
)
app.add_url_rule(
    "/vk/statistics",
    view_func=views.VKStatisticsView.as_view("vk_statistics"),
)
app.add_url_rule(
    "/vk/create-ad",
    view_func=views.VKCreateAdView.as_view("vk_create_ad"),
)
app.add_url_rule(
    "/vk/xlsx/ads",
    view_func=views.VKXlsxAdsView.as_view("vk_xlsx_ads"),
)
app.add_url_rule(
    "/vk/xlsx/leads",
    view_func=views.VKXlsxLeadsView.as_view("vk_xlsx_leads"),
)
app.add_url_rule(
    "/vk/xlsx",
    view_func=views.VKXlsxView.as_view("vk_xlsx"),
)
app.add_url_rule(
    "/api/vk/create-ad/dependes-fields",
    view_func=views.ApiVKCreateAdDependesFieldsView.as_view(
        "api_vk_create_ad_dependes_fields"
    ),
)
app.add_url_rule(
    "/api/vk/leads",
    view_func=views.ApiVKLeadsView.as_view("api_vk_leads"),
)
app.add_url_rule(
    "/api/vk/ads",
    view_func=views.ApiVKAdsView.as_view("api_vk_ads"),
)
app.add_url_rule(
    "/api/statistics/accounts/<provider>",
    view_func=views.StatisticsAccountsByProviderView.as_view(
        "statistics_accounts_by_provider"
    ),
)
app.add_url_rule(
    "/api/statistics/campaigns/<provider>/<account>",
    view_func=views.StatisticsCampaignsByAccountView.as_view(
        "statistics_campaigns_by_account"
    ),
)
app.add_url_rule(
    "/api/statistics/groups/<provider>/<campaign>",
    view_func=views.StatisticsGroupsByCampaignView.as_view(
        "statistics_groups_by_campaign"
    ),
)


@app.route("/login/hh")
def parse_vacancies():
    token = request.args.get("code")
    url = "https://hh.ru/oauth/token"
    client_id = config["hh"]["client_id"]
    client_secret = config["hh"]["client_secret"]

    params = {
        "grant_type": "authorization_code",
        "client_id": client_id,
        "client_secret": client_secret,
        "code": token,
        "redirect_uri": "https://analytic.neural-university.ru/login/hh",
    }
    response = requests.post(url=url, data=params)
    access_token = response.json()["access_token"]
    headers = {"Authorization": f"Bearer {access_token}"}

    employer_id = config["hh"]["employer_id"]
    managers_url = f"employers/{employer_id}/managers"

    response = requests.get("https://api.hh.ru/" + managers_url, headers=headers)

    manager_ids = [el["id"] for el in response.json()["items"]]

    vacancies = []
    for manager_id in manager_ids:
        manager_vacancies_url = (
            f"employers/{employer_id}/vacancies/active?manager_id={manager_id}"
        )
        response = requests.get(
            "https://api.hh.ru/" + manager_vacancies_url, headers=headers
        )
        for item in response.json()["items"]:
            response = requests.get(
                f'https://api.hh.ru/vacancies/{item["id"]}/stats', headers=headers
            )
            for value in response.json()["items"]:
                vacancy = {
                    "id": item["id"],
                    "name": item["name"],
                    "area": item["area"]["name"],
                    "date": value["date"],
                    "responses": value["responses"],
                    "views": value["views"],
                }
                vacancies.append(vacancy)

    CREDENTIALS_FILE = "analytics-322510-46607fe39c6c.json"  # Имя файла с закрытым ключом, вы должны подставить свое
    spreadsheet_id = "1qrJsJWWs1jyPUFLQ-0ZZ1zDrCtRZWNj0LV8KNBvGVoU"
    # Читаем ключи из файла
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE,
        [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )

    httpAuth = credentials.authorize(httplib2.Http())  # Авторизуемся в системе
    service = apiclient.discovery.build(
        "sheets", "v4", http=httpAuth
    )  # Выбираем работу с таблицами и 4 версию API

    ###
    # Вставить код, заменив строки с 69 включительно, в файле tmp.py
    ###

    # Преобразуем полученные по API свежие данные в новый датасет
    df_update = pd.DataFrame(vacancies)
    df_update.fillna(0, inplace=True)
    df_update = df_update.melt(
        id_vars=["id", "name", "area", "date"], value_vars=["responses", "views"]
    ).sort_values(by=["id", "date"])
    df_update["id"] = df_update["id"].astype("int")  # Преобразуем id в целое число

    # Подгружаем ранее сохраненные данные
    df_storage = pd.read_csv("vacancies.csv")

    # Объединяем датасеты со старыми и новыми данными
    df = pd.concat([df_storage, df_update])
    # Обновляем данные в хранилище
    df.to_csv("vacancies.csv", index=False)

    # Удаляем дубликаты, осталяем последнее значение
    df.drop_duplicates(subset=["id", "date", "variable"], keep="last", inplace=True)
    df.sort_values(by=["id", "date"], inplace=True)  # Сортируем данные по id и дате

    # Создаем вспомогательные датасеты отдельно с откликами и просмотрами по вакансии
    df_res = df[df["variable"] == "responses"]
    df_val = df[df["variable"] == "views"]

    stor_value = (
        0  # Переменная для накопления значения откликов и просмотров по месяцам
    )
    start_id = df_res.values[0][0]  # Получаем id первой вакансии
    start_month = datetime.strptime(
        df_res.values[0][3], "%Y-%m-%d"
    ).month  # Получаем первый месяц

    # Проходим в цикле по датасету с откликами (вакансии идут по порядку id)
    for i in range(df_res.shape[0]):
        cur_id = df_res.values[i][0]  # Получаем  id текущей вакансии
        cur_month = datetime.strptime(
            df_res.values[i][3], "%Y-%m-%d"
        ).month  # Получаем месяц текущей вакансии
        # Если поменялась вакансии или месяц - обнуляем накопление переменно store
        if (start_id != cur_id) or (cur_month != start_month):
            start_id = cur_id
            start_month = cur_month
            stor_value = 0
        stor_value = stor_value + df_res.values[i][5]
        row = {
            "id": df_res.values[i][0],
            "name": df_res.values[i][1],
            "area": df_res.values[i][2],
            "date": df_res.values[i][3],
            "variable": df_res.values[i][4],
            "value": stor_value,
        }
        df = df.append(row, ignore_index=True)

    stor_value = 0
    start_id = df_val.values[0][0]
    start_month = datetime.strptime(df_res.values[0][3], "%Y-%m-%d").month

    for i in range(df_val.shape[0]):
        cur_id = df_val.values[i][0]
        cur_month = datetime.strptime(df_val.values[i][3], "%Y-%m-%d").month
        if (start_id != cur_id) or (cur_month != start_month):
            start_id = cur_id
            start_month = cur_month
            stor_value = 0
        stor_value = stor_value + df_val.values[i][5]
        row = {
            "id": df_val.values[i][0],
            "name": df_val.values[i][1],
            "area": df_val.values[i][2],
            "date": df_val.values[i][3],
            "variable": df_val.values[i][4],
            "value": stor_value,
        }
        df = df.append(row, ignore_index=True)

    df.sort_values(by=["id", "date"], inplace=True)

    for i in range(0, df.shape[0], 4):
        # print(df.values[i])
        cv_1 = (
            round(df.values[i][5] / df.values[i + 1][5] * 100, 0)
            if df.values[i + 1][5] != 0
            else 0
        )
        cv_2 = (
            round(df.values[i + 2][5] / df.values[i + 3][5] * 100, 0)
            if df.values[i + 3][5] != 0
            else 0
        )
        row_1 = {
            "id": df.values[i][0],
            "name": df.values[i][1],
            "area": df.values[i][2],
            "date": df.values[i][3],
            "variable": "CV",
            "value": cv_1,
        }
        row_2 = {
            "id": df.values[i + 1][0],
            "name": df.values[i + 1][1],
            "area": df.values[i + 1][2],
            "date": df.values[i + 1][3],
            "variable": "CV",
            "value": cv_2,
        }
        df = df.append(row_1, ignore_index=True)
        df = df.append(row_2, ignore_index=True)

    df.sort_values(by=["id", "date", "variable"], inplace=True)

    df.insert(5, "range", "д")
    df.insert(6, "fact", "ф")

    for i in range(1, df.shape[0], 2):
        df.iloc[i, 5] = "м"
    df.loc[df["variable"] == "responses", "variable"] = "Отклики"
    df.loc[df["variable"] == "views", "variable"] = "Просмотры"
    df.loc[df["variable"] == "CV", "variable"] = "СV"

    df_out = (
        df.pivot_table(
            index=["id", "name", "area", "variable", "range", "fact"], columns=["date"]
        )
        .fillna(0)
        .astype(int)
        .astype(str)
    )

    vals = df_out.reset_index().T.reset_index().T.values.tolist()

    df_values = df_out.reset_index().T.reset_index().T.values.tolist()
    df_values = df_values[1:]
    df_values[0][0] = "id"
    df_values[0][1] = "Вакансия"
    df_values[0][2] = "Город"
    df_values[0][3] = "Показатели"
    current_id = None
    current_stat = None

    df_values = [[""] + row for i, row in enumerate(df_values)]
    index = 0

    for i in range(1, len(df_values)):
        if current_id != df_values[i][1]:
            current_id = df_values[i][1]
            index = index + 1
            df_values[i][0] = index
        else:
            df_values[i][1] = ""
            df_values[i][2] = ""
            df_values[i][3] = ""

        if current_stat != df_values[i][4]:
            current_stat = df_values[i][4]
        else:
            df_values[i][4] = ""

    for i in range(5, len(df_values), 6):
        df_values[i] = df_values[i][:7] + [
            str(df_values[i][j]) + "%" for j in range(7, len(df_values[i]))
        ]
        df_values[i + 1] = df_values[i + 1][:7] + [
            str(df_values[i + 1][j]) + "%" for j in range(7, len(df_values[i + 1]))
        ]

    df_values[0][0] = "№"

    results = (
        service.spreadsheets()
        .values()
        .batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "valueInputOption": "USER_ENTERED",
                # Данные воспринимаются, как вводимые пользователем (считается значение формул)
                "data": [
                    {
                        "range": "A1:AA1000",
                        "majorDimension": "ROWS",  # Сначала заполнять строки, затем столбцы
                        "values": df_values,
                    }
                ],
            },
        )
        .execute()
    )

    return df.to_html()
