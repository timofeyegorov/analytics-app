import re
import os
import sys
import pytz
import numpy
import pandas

from enum import Enum
from time import sleep
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qsl, ParseResult

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

sys.path.append(Variable.get("APP_FOLDER"))

from config import RESULTS_FOLDER
from app.data import StatisticsRoistatPackageEnum, PACKAGES_COMPARE
from app.analytics import pickle_loader
from app.plugins.ads import roistat
from app.dags.roistat import reader, writer, data
from app.dags.decorators import log_execution_time


def validate_response(response: Dict[str, Any], name: str):
    if (
        data.ResponseStatusEnum[response.get("status")]
        != data.ResponseStatusEnum.success
    ):
        raise Exception(f"Error while getting response `{name}`: {response}")


def detect_package(value) -> Optional[str]:
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


def get_levels(
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
    output.update({"package": detect_package(output.get("marker_level_1"))})
    return output


def get_metrics(
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


class ExceptionEnum(Enum):
    several_accounts_for_one_campaign = (
        "Found several accounts %s for one campaign '%s'"
    )


class DetectLevels:
    _lead: pandas.Series = None
    _stats: pandas.DataFrame = None
    _url: ParseResult = None
    _qs: Dict[str, str] = None
    _rs: List[str] = None
    _content: Dict[str, str] = None
    _content_available: List[str] = None

    _account: str = None
    _campaign: str = None
    _group: str = None
    _ad: str = None

    def __init__(self, lead: pandas.Series, stats: pandas.DataFrame):
        self._lead = lead
        self._stats = stats
        self._url = urlparse(self._lead.traffic_channel)
        self._qs = dict(parse_qsl(self._url.query.replace(r"&amp;", "&")))
        rs = self._qs.get("rs") or self._qs.get("roistat")
        self._rs = rs.split("_") if rs else []
        self._content = self._parse_utm_content(self._qs.get("utm_content", ""))
        self._content_available = self._get_content_available()

        self._detect_account()
        self._detect_campaign()
        self._detect_group()
        self._detect_ad()

        self._correct_ad()
        if self._ad is not None:
            self._correct_account()
            self._correct_campaign()
            self._correct_group()
        if self._group is not None:
            self._correct_account()
            self._correct_campaign()
        if self._campaign is not None:
            self._correct_account()

    def __str__(self) -> str:
        return f"""{self.__class__.__name__}:
       a: {self.account}
       c: {self.campaign}
       g: {self.group}
       a: {self.ad}"""

    @property
    def account(self) -> Optional[str]:
        return self._account

    @property
    def campaign(self) -> Optional[str]:
        return self._campaign

    @property
    def group(self) -> Optional[str]:
        return self._group

    @property
    def ad(self) -> Optional[str]:
        return self._ad

    @property
    def stats(self) -> pandas.DataFrame:
        return self._stats

    def _correct_account(self):
        accounts = self._stats.account.unique()
        if self._account is None and len(accounts) == 1:
            self._account = accounts[0]

    def _correct_campaign(self):
        campaigns = self._stats.campaign.unique()
        if self._campaign is None and len(campaigns) == 1:
            self._campaign = campaigns[0]

    def _correct_group(self):
        groups = self._stats.group.unique()
        if self._group is None and len(groups) == 1:
            self._group = groups[0]

    def _correct_ad(self):
        ads = self._stats.ad.unique()
        if self._ad is None and len(ads) == 1:
            self._ad = ads[0]

    def _parse_utm_content(self, value: str) -> Dict[str, str]:
        params = value.split("|")
        if re.findall(r":", params[0]):
            return dict(map(lambda item: tuple(item.split(":")), params))
        else:
            keys = params[0::2]
            values = params[1::2]
            values = values + [""] * (len(keys) - len(values))
            return dict(zip(keys, values))

    def _get_content_available(self) -> List[str]:
        return list(
            filter(
                None,
                [
                    self._content.get("cid"),
                    self._content.get("gid"),
                    self._content.get("aid"),
                    self._content.get("pid"),
                    self._content.get("did"),
                ],
            )
        )

    def _exception(self, exception: ExceptionEnum, *args):
        message = exception.value
        if args:
            message = message % args
        raise Exception(message)

    def _detect_account(self):
        account = None
        accounts = list(set(filter(None, self._stats.account.unique())))

        if len(self._rs) > 0:
            account = self._rs[0].lower().strip()
            if account not in accounts:
                account = None

        if account is None:
            utm_source = self._qs.get("utm_source")
            if utm_source:
                account = f":utm:{utm_source}".lower().strip()
                if account not in accounts:
                    account = None

        self._account = account
        if self._account:
            self._stats = self._stats[self._stats.account == self._account]

    def _detect_campaign(self):
        campaign = None
        campaigns = list(set(filter(None, self._stats.campaign.unique())))

        if len(self._rs) > 1:
            campaigns_detect = list(set(self._rs[1:]) & set(campaigns))
            if campaigns_detect:
                campaign = campaigns_detect[0]

        if campaign is None:
            utm_campaign = self._qs.get("utm_campaign")
            if utm_campaign:
                campaign = utm_campaign.strip()
                if campaign not in campaigns:
                    campaign = None

        if campaign is None:
            if self._content_available:
                campaigns_detect = list(set(self._content_available) & set(campaigns))
                if campaigns_detect:
                    campaign = campaigns_detect[0]

        self._campaign = campaign
        if self._campaign:
            self._stats = self._stats[self._stats.campaign == self._campaign]

    def _detect_group(self):
        group = None
        groups = list(set(filter(None, self._stats.group.unique())))

        if len(self._rs) > 1:
            groups_detect = list(set(self._rs[1:]) & set(groups))
            if groups_detect:
                group = groups_detect[0]

        if group is None:
            if self._content_available:
                groups_detect = list(set(self._content_available) & set(groups))
                if groups_detect:
                    group = groups_detect[0]

        self._group = group
        if self._group:
            self._stats = self._stats[self._stats.group == self._group]

    def _detect_ad(self):
        ad = None
        ads = list(set(filter(None, self._stats.ad.unique())))

        if len(self._rs) > 1:
            ads_detect = list(set(self._rs[1:]) & set(ads))
            if ads_detect:
                ad = ads_detect[0]

        if ad is None:
            if self._content_available:
                ads_detect = list(set(self._content_available) & set(ads))
                if ads_detect:
                    ad = ads_detect[0]

        self._ad = ad
        if self._ad:
            self._stats = self._stats[self._stats.ad == self._ad]


@log_execution_time("analytics")
def analytics():
    tz = pytz.timezone("Europe/Moscow")
    analytics = reader("analytics")
    from_date = max(
        list(analytics.date.unique())
        or [tz.localize(datetime.strptime("2021-04-14", "%Y-%m-%d"))]
    ) - timedelta(days=1)
    from_date = from_date.replace(hour=0, minute=0, second=0, microsecond=0)
    to_date = from_date + timedelta(days=30)
    datetime_now = datetime.now(tz=pytz.timezone("Europe/Moscow")).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    if to_date > datetime_now:
        to_date = datetime_now
    for day in range((to_date - from_date).days + 1):
        current_date: datetime = from_date + timedelta(days=day)
        print("Collect analytic:", current_date)
        time_now = datetime.now()
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
                "to": current_date.strftime("%Y-%m-%dT23:59:59+0300"),
            },
            metrics=["visitsCost", "leadCount", "visitCount"],
            interval="1d",
        )
        for item_data in response.get("data"):
            date = tz.localize(
                datetime.strptime(
                    item_data.get("dateFrom"),
                    "%Y-%m-%dT%H:%M:%S+0000",
                )
                + timedelta(seconds=3600 * 3)
            )
            for item in item_data.get("items"):
                levels = get_levels(item.get("dimensions"))
                metrics = get_metrics(item.get("metrics"), ["visitsCost"])
                analytics = analytics.append(
                    {**levels, **metrics, "date": date}, ignore_index=True
                )
        print("---", datetime.now() - time_now)
        sleep(1)
    writer("analytics", data=analytics)


@log_execution_time("statistics")
def statistics():
    analytics = reader("analytics")
    groups = [
        pandas.DataFrame(
            columns=[
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
        )
    ]
    for package in StatisticsRoistatPackageEnum:
        data_package = analytics[analytics.package == package.name]
        rel = PACKAGES_COMPARE.get(package)
        if not rel:
            continue
        data_package = data_package[rel[0]].rename(columns=rel[1])
        if package in (
            StatisticsRoistatPackageEnum.vk,
            StatisticsRoistatPackageEnum.site,
            StatisticsRoistatPackageEnum.seo,
            StatisticsRoistatPackageEnum.utm,
            StatisticsRoistatPackageEnum.mytarget,
        ):
            data_package["group"] = None
            data_package["group_title"] = None
        if package in (StatisticsRoistatPackageEnum.seo,):
            data_package["campaign"] = None
            data_package["campaign_title"] = None
        groups.append(data_package)
    data = pandas.concat(groups)
    writer("statistics", data=data)


@log_execution_time("leads")
def leads():
    columns = ["account", "campaign", "group", "ad"]
    statistics = reader("statistics")
    try:
        leads = pickle_loader.leads_np
        os.remove(f"{RESULTS_FOLDER}/leads_np.pkl")
    except FileNotFoundError:
        return
    for column in columns:
        leads[column] = None
    for index, lead in leads.iterrows():
        stats = statistics[statistics.date == lead.date]
        levels = DetectLevels(lead, stats)
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
        }
    )
    leads = leads.replace({numpy.nan: None})
    leads[leads.account == ""] = None
    leads[leads.campaign == ""] = None
    leads[leads.group == ""] = None
    leads[leads.ad == ""] = None

    data = reader("leads")
    writer("leads", data=pandas.concat([data, leads]))


dag = DAG(
    "api_data_roistat",
    description="Collect Roistat API data",
    schedule_interval="0 * * * *",
    start_date=datetime(2017, 3, 20),
    catchup=False,
)


analytics_po = PythonOperator(
    task_id="analytics",
    python_callable=analytics,
    dag=dag,
)
statistics_po = PythonOperator(
    task_id="statistics",
    python_callable=statistics,
    dag=dag,
)
leads_po = PythonOperator(
    task_id="leads",
    python_callable=leads,
    dag=dag,
)


analytics_po >> statistics_po
statistics_po >> leads_po
