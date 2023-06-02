from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
import pickle as pkl
import json
import os
import re
import sys
import pytz
import numpy
import pandas
import datetime

from enum import Enum
from time import sleep
from typing import List, Dict, Optional, Any
from pandas import DataFrame
from pathlib import Path
from urllib.parse import urlparse, parse_qsl, ParseResult

sys.path.append(Variable.get("APP_FOLDER"))

from app.database import get_leads_data
from app.database.get_crops import get_crops
from app.database.get_target_audience import get_target_audience
from app.database.get_trafficologists import get_trafficologists
from app.database.get_expenses import (
    get_trafficologists_expenses,
    get_trafficologists_expenses_levels,
)
from app.database.get_statuses import get_statuses
from app.database.get_ca_payment_analytic import get_ca_payment_analytic
from app.database.get_payments_table import get_payments_table
from app.database.preprocessing import (
    calculate_trafficologists_expenses,
    calculate_crops_expenses,
    recalc_expenses,
    telegram_restructure,
)
from app.database.preprocessing import get_turnover_on_lead, get_marginality
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
from app.tables import (
    calculate_channels_summary,
    calculate_channels_detailed,
    calculate_payments_accumulation,
)
from app.tables import calculate_marginality
from app.tables import calculate_audience_tables_by_date
from app.tables import calculate_audience_type_result
from app.tables import calculate_audience_type_percent_result
from config import RESULTS_FOLDER

from app.dags.decorators import log_execution_time
from app.dags.utils import RoistatDetectLevels
from app.analytics import pickle_loader
from app.plugins.ads import roistat
from app.data import StatisticsRoistatPackageEnum, PACKAGES_COMPARE


roistat_analytics_columns = [
    "package",
    "marker_level_1",
    "marker_level_2",
    "marker_level_3",
    "marker_level_4",
    "marker_level_5",
    "marker_level_6",
    "marker_level_7",
    "marker_level_1_title",
    "marker_level_2_title",
    "marker_level_3_title",
    "marker_level_4_title",
    "marker_level_5_title",
    "marker_level_6_title",
    "marker_level_7_title",
    "visitsCost",
    "date",
]
roistat_statistics_columns = [
    "date",
    "package",
    "account",
    "campaign",
    "group",
    "ad",
    "account_title",
    "campaign_title",
    "group_title",
    "ad_title",
    "expenses",
]
roistat_leads_columns = [
    "url",
    "qa1",
    "qa2",
    "qa3",
    "qa4",
    "qa5",
    "qa6",
    "ipl",
    "target_class",
    "email",
    "phone",
    "date",
    "expense",
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_content",
    "utm_term",
    "account",
    "campaign",
    "group",
    "ad",
]


class MatchIDs:
    _campaign_ids: List[str]
    _ad_ids: List[str]

    def __init__(self, leads: DataFrame):
        self._campaign_ids = []
        self._ad_ids = []

        for index, lead in leads.iterrows():
            qs = dict(
                parse_qsl(
                    urlparse(lead.traffic_channel).query.replace(r"&amp;amp;", "&")
                )
            )
            campaign_id = self.match_campaign_id(qs)
            ad_id = self.match_ad_id(qs)

            self._campaign_ids.append(
                str(campaign_id) if campaign_id is not None else None
            )
            self._ad_ids.append(str(ad_id) if ad_id is not None else None)

    @property
    def dict(self) -> Dict[str, List[str]]:
        return {
            "campaign_id": self._campaign_ids,
            "ad_id": self._ad_ids,
        }

    def match_campaign_id(self, qs: Dict[str, str]) -> str:
        rs = qs.get("rs", "")
        rs_split = list(filter(None, rs.split("_")))
        if len(rs_split):
            if re.findall(r"^vk.*", rs_split[0]):
                try:
                    return int(rs_split[1])
                except Exception:
                    utm_campaign_findall = re.findall(
                        r"^(\d+).*", qs.get("utm_campaign", "")
                    )
                    if len(utm_campaign_findall) == 1:
                        return int(utm_campaign_findall[0])
                    else:
                        if len(rs_split) == 1:
                            return
                        elif len(rs_split) == 5:
                            try:
                                return int(rs_split[3])
                            except Exception:
                                return
                        else:
                            return

    def match_ad_id(self, qs: Dict[str, str]) -> str:
        rs = qs.get("rs", "")
        rs_split = list(filter(None, rs.split("_")))
        if len(rs_split):
            if re.findall(r"^vk.*", rs_split[0]):
                if len(rs_split) == 3:
                    match_id = re.findall(r"^{*(\d{2,})[\s}]*$", rs_split[2])
                    if len(match_id) == 1:
                        return int(match_id[0])
                    else:
                        try:
                            return int(qs.get("utm_content"))
                        except Exception:
                            return
                else:
                    utm_content_findall = re.findall(
                        r"^(\d+).*", qs.get("utm_content", "")
                    )
                    if len(utm_content_findall) == 1:
                        return int(utm_content_findall[0])
                    else:
                        if rs_split == 1:
                            return
                        else:
                            try:
                                match_id = re.findall(r"^{*(\d{3,})}*$", rs_split[4])
                                if len(match_id) == 1:
                                    return int(match_id[0])
                            except Exception:
                                return


def roistat_detect_package(value) -> Optional[str]:
    if re.match(r"^vk.+$", value):
        return StatisticsRoistatPackageEnum.vk.name
    if (
        re.match(r"^direct\d+.*$", value)
        or re.match(r"^:openstat:direct\.yandex\.ru$", value)
        or re.match(r"^direct$", value)
    ):
        return StatisticsRoistatPackageEnum.yandex_direct.name
    if re.match(r"^ya\.master$", value) or re.match(r"^yulyayamaster$", value):
        return StatisticsRoistatPackageEnum.yandex_master.name
    if re.match(r"^facebook\d+.*$", value):
        return StatisticsRoistatPackageEnum.facebook.name
    if re.match(r"^mytarget\d+$", value):
        return StatisticsRoistatPackageEnum.mytarget.name
    if re.match(r"^google\d+$", value) or re.match(r"^g-adwords\d+$", value):
        return StatisticsRoistatPackageEnum.google.name
    if re.match(r"^site$", value):
        return StatisticsRoistatPackageEnum.site.name
    if re.match(r"^seo$", value):
        return StatisticsRoistatPackageEnum.seo.name
    if re.match(r"^:utm:.+$", value):
        return StatisticsRoistatPackageEnum.utm.name
    if value:
        print("Undefined package:", value)
    return StatisticsRoistatPackageEnum.undefined.name


def roistat_get_levels(
    dimensions: Dict[str, Dict[str, str]],
) -> Dict[str, Optional[str]]:
    output = {
        "package": StatisticsRoistatPackageEnum.undefined.name,
        "marker_level_1": "",
        "marker_level_2": "",
        "marker_level_3": "",
        "marker_level_4": "",
        "marker_level_5": "",
        "marker_level_6": "",
        "marker_level_7": "",
        "marker_level_1_title": StatisticsRoistatPackageEnum.undefined.value,
        "marker_level_2_title": StatisticsRoistatPackageEnum.undefined.value,
        "marker_level_3_title": StatisticsRoistatPackageEnum.undefined.value,
        "marker_level_4_title": StatisticsRoistatPackageEnum.undefined.value,
        "marker_level_5_title": StatisticsRoistatPackageEnum.undefined.value,
        "marker_level_6_title": StatisticsRoistatPackageEnum.undefined.value,
        "marker_level_7_title": StatisticsRoistatPackageEnum.undefined.value,
    }
    levels = dict(sorted(dimensions.items()))
    for name, level in levels.items():
        level_value = level.get("value", "")
        level_title = level.get("title", "")
        if not level_value:
            level_title = StatisticsRoistatPackageEnum.undefined.value
        output.update({name: level_value, f"{name}_title": level_title})
    output.update({"package": roistat_detect_package(output.get("marker_level_1"))})
    return output


def roistat_get_metrics(
    metrics: List[Dict[str, Any]], available_metrics: List[str]
) -> Dict[str, str]:
    return dict(
        map(
            lambda item: (item.get("metric_name"), item.get("value")),
            list(
                filter(
                    lambda value: value.get("metric_name") in available_metrics, metrics
                )
            ),
        )
    )


@log_execution_time("load_crops")
def load_crops():
    crops, crops_list = get_crops()
    with open(os.path.join(RESULTS_FOLDER, "crops.pkl"), "wb") as f:
        pkl.dump(crops, f)
    with open(os.path.join(RESULTS_FOLDER, "crops_list.pkl"), "wb") as f:
        pkl.dump(crops_list, f)


@log_execution_time("load_trafficologists_expenses")
def load_trafficologists_expenses():
    expenses = get_trafficologists_expenses()
    with open(os.path.join(RESULTS_FOLDER, "expenses.json"), "w") as f:
        json.dump(expenses, f)


@log_execution_time("load_trafficologists_expenses_levels")
def load_trafficologists_expenses_levels():
    expenses = get_trafficologists_expenses_levels()
    with open(os.path.join(RESULTS_FOLDER, "expenses_levels.json"), "w") as f:
        json.dump(expenses, f)


@log_execution_time("load_target_audience")
def load_target_audience():
    target_audience = get_target_audience()
    with open(os.path.join(RESULTS_FOLDER, "target_audience.pkl"), "wb") as f:
        pkl.dump(target_audience, f)


@log_execution_time("load_trafficologists")
def load_trafficologists():
    trafficologists = get_trafficologists()
    with open(os.path.join(RESULTS_FOLDER, "trafficologists.pkl"), "wb") as f:
        pkl.dump(trafficologists, f)


@log_execution_time("load_statuses")
def load_status():
    statuses = get_statuses()
    with open(os.path.join(RESULTS_FOLDER, "statuses.pkl"), "wb") as f:
        pkl.dump(statuses, f)


@log_execution_time("load_ca_payment_analytic")
def load_ca_payment_analytic():
    ca_payment_analytic = get_ca_payment_analytic()
    with open(os.path.join(RESULTS_FOLDER, "ca_payment_analytic.pkl"), "wb") as f:
        pkl.dump(ca_payment_analytic, f)


@log_execution_time("load_payments_table")
def load_payments_table():
    payments_table = get_payments_table()
    with open(os.path.join(RESULTS_FOLDER, "payments_table.pkl"), "wb") as f:
        pkl.dump(payments_table, f)


@log_execution_time("load_data")
def load_data():
    try:
        leads_old = pickle_loader.leads
    except FileNotFoundError:
        leads_old = pandas.DataFrame()

    data = get_leads_data()

    ca_payment_analytic = pickle_loader.ca_payment_analytic
    crops = pickle_loader.crops
    trafficologists = pickle_loader.trafficologists

    leads = get_turnover_on_lead(data, ca_payment_analytic)
    leads = get_marginality(leads)
    leads = calculate_crops_expenses(leads, crops)
    leads = calculate_trafficologists_expenses(leads, trafficologists)
    leads = leads.assign(**MatchIDs(leads).dict)
    leads = recalc_expenses(leads)
    leads = telegram_restructure(leads)
    tz = pytz.timezone("Europe/Moscow")
    leads["date"] = leads.created_at.apply(
        lambda value: tz.localize(value).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    )
    leads.drop_duplicates(ignore_index=True, inplace=True)
    with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "wb") as f:
        pkl.dump(leads, f)

    try:
        leads_np = pickle_loader.leads_np
    except FileNotFoundError:
        leads_np = pandas.DataFrame(columns=list(leads.columns))
    leads_np = pandas.concat([leads_np, leads.drop(leads_old.index)])
    with open(os.path.join(RESULTS_FOLDER, "leads_np.pkl"), "wb") as f:
        pkl.dump(leads_np, f)


# @log_execution_time('calculate_channel_expense')
# def calculate_channel_expense():
#     with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'rb') as f:
#         leads = pkl.load(f)
#     with open(os.path.join(RESULTS_FOLDER, 'crops.pkl'), 'rb') as f:
#         crops = pkl.load(f)
#     with open(os.path.join(RESULTS_FOLDER, 'trafficologists.pkl'), 'rb') as f:
#         trafficologists = pkl.load(f)
#     leads = calculate_crops_expenses(leads, crops)
#     leads = calculate_trafficologists_expenses(leads, trafficologists)
#     with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'wb') as f:
#         pkl.dump(leads, f)

# @log_execution_time('calculate_turnover_on_lead')
# def calculate_turnover_on_lead():
#     with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'rb') as f:
#         leads = pkl.load(f)
#     with open(os.path.join(RESULTS_FOLDER, 'ca_payment_analytic.pkl'), 'rb') as f:
#         ca_payment_analytic = pkl.load(f)
#     leads = get_turnover_on_lead(leads, ca_payment_analytic)
#     leads = get_marginality(leads)
#     with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'wb') as f:
#         pkl.dump(leads, f)


@log_execution_time("marginality")
def marginality():
    with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
        data = pkl.load(f)
    marginality = calculate_marginality(data)
    with open(os.path.join(RESULTS_FOLDER, "marginality.pkl"), "wb") as f:
        pkl.dump(marginality, f)
    return "Success"


@log_execution_time("channels_summary")
def channels_summary():
    with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
        data = pkl.load(f)
    channels_summary = calculate_channels_summary(data)
    with open(os.path.join(RESULTS_FOLDER, "channels_summary.pkl"), "wb") as f:
        pkl.dump(channels_summary, f)
    return "Success"


@log_execution_time("channels_detailed")
def channels_detailed():
    with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
        data = pkl.load(f)
    channels_detailed = calculate_channels_detailed(data)
    with open(os.path.join(RESULTS_FOLDER, "channels_detailed.pkl"), "wb") as f:
        pkl.dump(channels_detailed, f)
    return "Success"


@log_execution_time("payments_accumulation")
def payments_accumulation():
    with open(os.path.join(RESULTS_FOLDER, "payments_table.pkl"), "rb") as f:
        data = pkl.load(f)
    payments_accumulation = calculate_payments_accumulation(data)
    with open(os.path.join(RESULTS_FOLDER, "payments_accumulation.pkl"), "wb") as f:
        pkl.dump(payments_accumulation, f)
    return "Success"


@log_execution_time("audience_type_by_date")
def audience_type_by_date():
    with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
        data = pkl.load(f)
    audience_type_by_date = calculate_audience_tables_by_date(data)
    with open(os.path.join(RESULTS_FOLDER, "audience_type_by_date.pkl"), "wb") as f:
        pkl.dump(audience_type_by_date, f)
    return "Success"


@log_execution_time("audience_type")
def audience_type():
    with open(os.path.join(RESULTS_FOLDER, "audience_type_by_date.pkl"), "rb") as f:
        data = pkl.load(f)
    audience_type = calculate_audience_type_result(data)
    with open(os.path.join(RESULTS_FOLDER, "audience_type.pkl"), "wb") as f:
        pkl.dump(audience_type, f)
    return "Success"


@log_execution_time("audience_type_percent")
def audience_type_percent():
    with open(os.path.join(RESULTS_FOLDER, "audience_type_by_date.pkl"), "rb") as f:
        data = pkl.load(f)
    audience_type_percent = calculate_audience_type_percent_result(data)
    with open(os.path.join(RESULTS_FOLDER, "audience_type_percent.pkl"), "wb") as f:
        pkl.dump(audience_type_percent, f)
    return "Success"


@log_execution_time("segments")
def segments():
    with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
        data = pkl.load(f)
    segments = calculate_segments(data)
    with open(os.path.join(RESULTS_FOLDER, "segments.pkl"), "wb") as f:
        pkl.dump(segments, f)
    return "Success"


@log_execution_time("clusters")
def clusters():
    with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
        data = pkl.load(f)
    clusters = calculate_clusters(data)
    with open(os.path.join(RESULTS_FOLDER, "clusters.pkl"), "wb") as f:
        pkl.dump(clusters, f)
    return "Success"


@log_execution_time("landings")
def landings():
    with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
        data = pkl.load(f)
    landings = calculate_landings(data)
    with open(os.path.join(RESULTS_FOLDER, "landings.pkl"), "wb") as f:
        pkl.dump(landings, f)


@log_execution_time("segments_stats")
def segments_stats():
    with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
        data = pkl.load(f)
    segments_stats = calculate_segments_stats(data)
    with open(os.path.join(RESULTS_FOLDER, "segments_stats.pkl"), "wb") as f:
        pkl.dump(segments_stats, f)


@log_execution_time("turnover")
def turnover():
    with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
        data = pkl.load(f)
    turnover = calculate_turnover(data)
    with open(os.path.join(RESULTS_FOLDER, "turnover.pkl"), "wb") as f:
        pkl.dump(turnover, f)


@log_execution_time("traffic_sources")
def traffic_sources():
    with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
        data = pkl.load(f)
    traffic_sources = calculate_traffic_sources(data)
    with open(os.path.join(RESULTS_FOLDER, "traffic_sources.pkl"), "wb") as f:
        pkl.dump(traffic_sources, f)


@log_execution_time("leads_ta_stats")
def leads_ta_stats():
    with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
        data = pkl.load(f)
    leads_ta_stats = calculate_leads_ta_stats(data)
    with open(os.path.join(RESULTS_FOLDER, "leads_ta_stats.pkl"), "wb") as f:
        pkl.dump(leads_ta_stats, f)


@log_execution_time("roistat_analytics")
def roistat_analytics():
    tz = pytz.timezone("Europe/Moscow")
    try:
        analytics = pickle_loader.roistat_analytics
    except Exception:
        analytics = pandas.DataFrame(columns=roistat_analytics_columns)

    datetime_now = datetime.datetime.now(tz=tz).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    for days in range(31):
        current_date = datetime_now - datetime.timedelta(days=days)
        print("Collect analytic:", current_date)
        time_now = datetime.datetime.now(tz=tz)
        response = roistat(
            "analytics",
            dimensions=[
                "marker_level_1",
                "marker_level_2",
                "marker_level_3",
                "marker_level_4",
                "marker_level_5",
                "marker_level_6",
                "marker_level_7",
            ],
            period={
                "from": current_date.strftime("%Y-%m-%dT00:00:00+0300"),
                "to": current_date.strftime("%Y-%m-%dT23:59:59.9999+0300"),
            },
            metrics=["visitsCost", "leadCount", "visitCount", "impressions"],
            interval="1d",
        )
        for item_data in response.get("data"):
            date = tz.localize(
                datetime.datetime.strptime(
                    item_data.get("dateFrom"),
                    "%Y-%m-%dT%H:%M:%S+0000",
                )
                + datetime.timedelta(seconds=3600 * 3)
            )
            analytics.drop(analytics[analytics.date == date].index, inplace=True)
            analytics_date = []
            for item in item_data.get("items"):
                levels = roistat_get_levels(item.get("dimensions"))
                metrics = roistat_get_metrics(item.get("metrics"), ["visitsCost"])
                analytics_date.append({**levels, **metrics, "date": date})
            analytics = analytics.append(analytics_date, ignore_index=True)
        print("---", datetime.datetime.now(tz=tz) - time_now)
        sleep(1)
    analytics = analytics.sort_values(
        by=[
            "date",
            "marker_level_1",
            "marker_level_2",
            "marker_level_3",
            "marker_level_4",
            "marker_level_5",
            "marker_level_6",
            "marker_level_7",
        ]
    ).reset_index(drop=True)
    with open(os.path.join(RESULTS_FOLDER, "roistat_analytics.pkl"), "wb") as f:
        pkl.dump(analytics, f)


@log_execution_time("roistat_statistics")
def roistat_statistics():
    try:
        analytics = pickle_loader.roistat_analytics
    except Exception:
        analytics = pandas.DataFrame(columns=roistat_analytics_columns)
    groups = [pandas.DataFrame(columns=roistat_statistics_columns)]
    for package in StatisticsRoistatPackageEnum:
        data_package = analytics[analytics.package == package.name]
        rel = PACKAGES_COMPARE.get(package)
        if not rel:
            continue
        data_package = data_package[rel[0]].rename(columns=rel[1])
        groups.append(data_package)
    data = pandas.concat(groups).reset_index(drop=True)
    data[["account", "campaign", "group", "ad"]] = data[
        ["account", "campaign", "group", "ad"]
    ].replace({numpy.nan: ""})
    data[["account_title", "campaign_title", "group_title", "ad_title"]] = data[
        ["account_title", "campaign_title", "group_title", "ad_title"]
    ].replace({numpy.nan: StatisticsRoistatPackageEnum.undefined.value})
    with open(os.path.join(RESULTS_FOLDER, "roistat_statistics.pkl"), "wb") as f:
        pkl.dump(data, f)


@log_execution_time("roistat_leads")
def roistat_leads():
    columns = ["account", "campaign", "group", "ad"]
    try:
        statistics = pickle_loader.roistat_statistics
    except Exception:
        statistics = pandas.DataFrame(columns=roistat_statistics_columns)
    try:
        leads = pickle_loader.leads_np
        os.remove(f"{RESULTS_FOLDER}/leads_np.pkl")
    except FileNotFoundError:
        return
    for column in columns:
        leads[column] = ""
    for index, lead in leads.iterrows():
        stats = statistics[statistics.date == lead.date]
        levels = RoistatDetectLevels(lead, stats)
        leads.loc[index, columns] = [
            levels.account,
            levels.campaign,
            levels.group,
            levels.ad,
        ]

    leads = leads[
        [
            "traffic_channel",
            "quiz_answers1",
            "quiz_answers2",
            "quiz_answers3",
            "quiz_answers4",
            "quiz_answers5",
            "quiz_answers6",
            "turnover_on_lead",
            "target_class",
            "email",
            "phone",
            "date",
            "channel_expense",
            "utm_source",
            "utm_medium",
            "utm_campaign",
            "utm_content",
            "utm_term",
        ]
        + columns
    ].rename(
        columns={
            "traffic_channel": "url",
            "quiz_answers1": "qa1",
            "quiz_answers2": "qa2",
            "quiz_answers3": "qa3",
            "quiz_answers4": "qa4",
            "quiz_answers5": "qa5",
            "quiz_answers6": "qa6",
            "turnover_on_lead": "ipl",
            "channel_expense": "expenses",
        }
    )
    try:
        data = pickle_loader.roistat_leads
    except Exception:
        data = pandas.DataFrame(columns=roistat_leads_columns)
    leads = (
        pandas.concat([data, leads])
        .drop_duplicates(keep="last", ignore_index=True)
        .reset_index(drop=True)
    )
    with open(os.path.join(RESULTS_FOLDER, "roistat_leads.pkl"), "wb") as f:
        pkl.dump(leads, f)


@log_execution_time("roistat_update_levels")
def roistat_update_levels():
    statistics = pickle_loader.roistat_statistics
    columns = ["account", "campaign", "group", "ad"]
    date_to = datetime.datetime.now()
    date_from = date_to - datetime.timedelta(weeks=1)
    leads = pickle_loader.roistat_leads
    leads["d"] = leads["date"].apply(lambda item: item.date())
    leads = leads[(leads["d"] >= date_from.date()) & (leads["d"] <= date_to.date())]
    leads = leads.loc[:, leads.columns != "d"]
    leads.rename(columns={"url": "traffic_channel"}, inplace=True)

    for index, lead in leads.iterrows():
        stats = statistics[statistics.date == lead.date]
        levels = RoistatDetectLevels(lead, stats)
        leads.loc[index, columns] = [
            levels.account,
            levels.campaign,
            levels.group,
            levels.ad,
        ]
    leads.rename(columns={"traffic_channel": "url"}, inplace=True)

    source = pickle_loader.roistat_leads
    source.loc[leads.index, columns] = leads[columns].values
    with open(Path(RESULTS_FOLDER, "roistat_leads.pkl"), "wb") as file_ref:
        pkl.dump(source, file_ref)


dag = DAG(
    "calculate_cache",
    description="Calculates tables",
    schedule_interval="0 0,8,16 * * *",
    start_date=datetime.datetime(2017, 3, 20),
    catchup=False,
)

crops_operator = PythonOperator(
    task_id="load_crops", python_callable=load_crops, dag=dag
)
trafficologists_operator = PythonOperator(
    task_id="load_trafficologists", python_callable=load_trafficologists, dag=dag
)
target_audience_operator = PythonOperator(
    task_id="load_target_audience", python_callable=load_target_audience, dag=dag
)
expenses_operator = PythonOperator(
    task_id="load_trafficologists_expenses",
    python_callable=load_trafficologists_expenses,
    dag=dag,
)
expenses_levels_operator = PythonOperator(
    task_id="load_trafficologists_expenses_levels",
    python_callable=load_trafficologists_expenses_levels,
    dag=dag,
)
statuses_operator = PythonOperator(
    task_id="load_statuses", python_callable=load_status, dag=dag
)
ca_payment_analytic_operator = PythonOperator(
    task_id="load_ca_payment_analytic",
    python_callable=load_ca_payment_analytic,
    dag=dag,
)
payments_table_operator = PythonOperator(
    task_id="load_payments_table", python_callable=load_payments_table, dag=dag
)

payments_accumulation_operator = PythonOperator(
    task_id="payments_accumulation", python_callable=payments_accumulation, dag=dag
)

# channel_expense_operator = PythonOperator(task_id='calculate_channel_expense', python_callable=calculate_channel_expense, dag=dag)
# turnover_on_lead_operator = PythonOperator(task_id='calculate_turnover_on_lead', python_callable=calculate_turnover_on_lead, dag=dag)

clean_data_operator = PythonOperator(
    task_id="load_data", python_callable=load_data, dag=dag
)
channels_summary_operator = PythonOperator(
    task_id="channels_summary", python_callable=channels_summary, dag=dag
)
channels_detailed_operator = PythonOperator(
    task_id="channels_detailed", python_callable=channels_detailed, dag=dag
)
marginality_operator = PythonOperator(
    task_id="marginality", python_callable=marginality, dag=dag
)

audience_type_by_date_operator = PythonOperator(
    task_id="audience_type_by_date", python_callable=audience_type_by_date, dag=dag
)
audience_type_operator = PythonOperator(
    task_id="audience_type", python_callable=audience_type, dag=dag
)
audience_type_percent_operator = PythonOperator(
    task_id="audience_type_percent", python_callable=audience_type_percent, dag=dag
)

segments_operator = PythonOperator(
    task_id="segments", python_callable=segments, dag=dag
)
clusters_operator = PythonOperator(
    task_id="clusters", python_callable=clusters, dag=dag
)
landings_operator = PythonOperator(
    task_id="landings", python_callable=landings, dag=dag
)
segments_stats_operator = PythonOperator(
    task_id="segments_stats", python_callable=segments_stats, dag=dag
)
turnover_operator = PythonOperator(
    task_id="turnover", python_callable=turnover, dag=dag
)
leads_ta_stats_operator = PythonOperator(
    task_id="leads_ta_stats", python_callable=leads_ta_stats, dag=dag
)
traffic_sources_operator = PythonOperator(
    task_id="traffic_sources", python_callable=traffic_sources, dag=dag
)
roistat_analytics_operator = PythonOperator(
    task_id="roistat_analytics", python_callable=roistat_analytics, dag=dag
)
roistat_statistics_operator = PythonOperator(
    task_id="roistat_statistics", python_callable=roistat_statistics, dag=dag
)
roistat_leads_operator = PythonOperator(
    task_id="roistat_leads", python_callable=roistat_leads, dag=dag
)
roistat_update_levels_operator = PythonOperator(
    task_id="roistat_update_levels", python_callable=roistat_update_levels, dag=dag
)

crops_operator >> clean_data_operator
trafficologists_operator >> clean_data_operator
target_audience_operator >> clean_data_operator
expenses_operator >> clean_data_operator
expenses_levels_operator >> clean_data_operator
statuses_operator >> clean_data_operator
ca_payment_analytic_operator >> clean_data_operator
payments_table_operator >> clean_data_operator

# clean_data_operator >> channel_expense_operator
#  clean_data_operator >> turnover_on_lead_operator

clean_data_operator >> audience_type_by_date_operator
clean_data_operator >> audience_type_operator
clean_data_operator >> audience_type_percent_operator

clean_data_operator >> payments_accumulation_operator
clean_data_operator >> channels_summary_operator
clean_data_operator >> channels_detailed_operator
clean_data_operator >> marginality_operator
# turnover_on_lead_operator >> payments_accumulation_operator
# turnover_on_lead_operator >> channels_summary_operator
# turnover_on_lead_operator >> channels_detailed_operator
# turnover_on_lead_operator >> marginality_operator

clean_data_operator >> segments_operator
clean_data_operator >> clusters_operator
clean_data_operator >> landings_operator
clean_data_operator >> segments_stats_operator
clean_data_operator >> turnover_operator
clean_data_operator >> leads_ta_stats_operator
clean_data_operator >> traffic_sources_operator
# channel_expense_operator >> segments_operator
# channel_expense_operator >> clusters_operator
# channel_expense_operator >> landings_operator
# channel_expense_operator >> segments_stats_operator
# channel_expense_operator >> turnover_operator
# channel_expense_operator >> leads_ta_stats
# channel_expense_operator >> traffic_sources

clean_data_operator >> roistat_analytics_operator
roistat_analytics_operator >> roistat_statistics_operator
roistat_statistics_operator >> roistat_leads_operator
roistat_leads_operator >> roistat_update_levels_operator
