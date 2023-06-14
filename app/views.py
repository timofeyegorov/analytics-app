import os
import re
import json
import pytz
import pandas
import pickle
import requests
import tempfile
import datetime
import httplib2
import apiclient

from enum import Enum
from math import ceil
from uuid import uuid4
from pathlib import Path
from typing import Tuple, List, Dict, Any, Optional
from collections import OrderedDict
from transliterate import slugify
from urllib.parse import urlparse, parse_qsl, urlencode
from pydantic import BaseModel, ConstrainedDate, conint
from werkzeug.datastructures import ImmutableMultiDict
from oauth2client.service_account import ServiceAccountCredentials

from xlsxwriter import Workbook

from flask import request, render_template, send_file, abort, send_file
from flask.views import MethodView

from app import decorators
from app.plugins.ads import vk
from app.analytics.pickle_load import PickleLoader
from app.dags.vk import reader as vk_reader, data as vk_data
from app.utils import detect_week
from app.data import (
    StatisticsProviderEnum,
    StatisticsGroupByEnum,
    StatisticsRoistatGroupByEnum,
    StatisticsUTMGroupByEnum,
    CalculateColumnEnum,
    StatisticsUTMColumnEnum,
)
from config import DATA_FOLDER, CREDENTIALS_FILE, RESULTS_FOLDER


pickle_loader = PickleLoader()

UNDEFINED = "Undefined"


def parse_int(value: str) -> int:
    value = parse_float(value)
    if pandas.isna(value):
        return pandas.NA
    return round(value)


def parse_positive_int_value(value: Optional[str] = None) -> Optional[int]:
    if value is None:
        return pandas.NA
    value = parse_float(value)
    if pandas.isna(value) or value < 0:
        return pandas.NA
    return round(value)


def parse_float(value: str) -> float:
    if pandas.isna(value):
        return pandas.NA
    value = re.sub(r"\s", "", str(value))
    if not re.search(r"^-?\d+\.?\d*$", str(value)):
        return pandas.NA
    return float(value)


def parse_percent(value: float) -> float:
    value = parse_float(value)
    if pandas.isna(value):
        return pandas.NA
    return float(value) * 100


def parse_bool_from_int(value: Optional[Any]) -> bool:
    if str(value).lower() == "on":
        value = 1
    if pandas.isna(value) or value is None:
        return False
    return bool(int(value))


def parse_bool(value: Optional[Any]) -> bool:
    if pandas.isna(value) or value is None:
        return False
    return bool(value)


def parse_estimate(value: pandas.Series) -> float:
    percent = value["purchase_probability"]
    profit = value["potential_order_amount"]
    if pandas.isna(percent) or pandas.isna(profit):
        return pandas.NA
    return profit * percent / 100


def parse_percent_value(value: Optional[float] = None) -> Optional[float]:
    if value is None:
        return pandas.NA
    value = parse_float(value)
    if pandas.isna(value) or value < 0 or value > 100:
        return pandas.NA
    return int(value)


def parse_slug(value: str) -> str:
    if str(value) == "" or pandas.isna(value):
        return pandas.NA
    return slugify(str(value), "ru").replace("-", "_")


def parse_date(value: str) -> datetime.date:
    if pandas.isna(value) or value is None:
        return pandas.NA
    if isinstance(value, datetime.date):
        return value
    if isinstance(value, datetime.datetime):
        return value.date()
    match = re.search(r"^(\d{1,2})\.(\d{1,2})\.(\d{4})$", str(value))
    if not match:
        return pandas.NA
    groups = list(match.groups())
    if len(groups[0]) == 1:
        groups[0] = f"0{groups[0]}"
    if len(groups[1]) == 1:
        groups[1] = f"0{groups[1]}"
    return datetime.date.fromisoformat("-".join(list(reversed(groups))))


class ContextTemplate:
    data: Dict[str, Any] = {}

    def __call__(self, name: str, value: Any):
        self.data.update({name: value})


class TemplateView(MethodView):
    context: ContextTemplate = ContextTemplate()
    template_name: str
    title: str = ""

    @decorators.auth
    def dispatch_request(self, *args, **kwargs):
        return super().dispatch_request(*args, **kwargs)

    def get_template_name(self) -> str:
        return self.template_name

    def render(self):
        self.context("title", self.title)
        return render_template(self.get_template_name(), **self.context.data)

    def get(self):
        return self.render()


class APIView(MethodView):
    data: Dict[str, Any] = {}

    def render(self):
        return self.data

    def get(self, *args, **kwargs):
        return self.render()

    def post(self, *args, **kwargs):
        return self.render()


class StatisticsFiltersData(BaseModel):
    date: Tuple[Optional[ConstrainedDate], Optional[ConstrainedDate]]
    provider: Optional[str]
    account: Optional[int]
    campaign: Optional[int]
    group: Optional[int]
    groupby: str


class WeekStatsFiltersData(BaseModel):
    date: ConstrainedDate
    manager: Optional[str]
    accumulative: bool = False

    def __getitem__(self, item):
        if item == "manager":
            return self.manager

    def __setitem__(self, key, value):
        if key == "manager":
            self.manager = value


class WeekStatsFiltersEmptyData(BaseModel):
    pass


class WeekStatsFiltersCohortsData(WeekStatsFiltersEmptyData):
    date: ConstrainedDate
    group: Optional[str]
    manager: Optional[str]
    channel: Optional[str]
    accumulative: bool = False
    profit: bool = False

    def __getitem__(self, item):
        if item == "group":
            return self.group
        elif item == "manager":
            return self.manager
        elif item == "channel":
            return self.channel

    def __setitem__(self, key, value):
        if key == "group":
            self.group = value
        elif key == "manager":
            self.manager = value
        elif key == "channel":
            self.channel = value


class WeekStatsFiltersManagersData(BaseModel):
    value_date_from: Optional[ConstrainedDate]
    value_date_to: Optional[ConstrainedDate]
    profit_date_from: Optional[ConstrainedDate]
    profit_date_to: Optional[ConstrainedDate]
    hide_inactive_managers: bool = False


class WeekStatsFiltersChannelsData(BaseModel):
    order_date_from: Optional[ConstrainedDate]
    order_date_to: Optional[ConstrainedDate]
    profit_date_from: Optional[ConstrainedDate]
    profit_date_to: Optional[ConstrainedDate]


class ZoomsFiltersData(BaseModel):
    date_from: Optional[ConstrainedDate]
    date_to: Optional[ConstrainedDate]
    group: Optional[str]
    manager: Optional[str]
    purchase_probability_from: Optional[conint(ge=0, le=100)]
    purchase_probability_to: Optional[conint(ge=0, le=100)]
    expected_payment_date_from: Optional[ConstrainedDate]
    expected_payment_date_to: Optional[ConstrainedDate]
    on_control: Optional[bool]

    def __getitem__(self, item):
        if item == "group":
            return self.group
        elif item == "manager":
            return self.manager

    def __setitem__(self, key, value):
        if key == "group":
            self.group = value
        elif key == "manager":
            self.manager = value


class ManagersSalesCoursesFiltersData(BaseModel):
    payment_date_from: Optional[ConstrainedDate]
    payment_date_to: Optional[ConstrainedDate]


class ManagersSalesDatesFiltersData(BaseModel):
    payment_date_from: Optional[ConstrainedDate]
    payment_date_to: Optional[ConstrainedDate]
    course: Optional[str]

    def __getitem__(self, item):
        if item == "course":
            return self.course

    def __setitem__(self, key, value):
        if key == "course":
            self.course = value


class IntensivesFiltersData(BaseModel):
    date_from: Optional[ConstrainedDate]
    date_to: Optional[ConstrainedDate]


class SearchLeadsFiltersData(BaseModel):
    id: str = ""


class StatisticsRoistatFiltersData(BaseModel):
    date: Tuple[Optional[ConstrainedDate], Optional[ConstrainedDate]]
    account: Optional[str]
    campaign: Optional[str]
    group: Optional[str]
    groupby: str
    only_ru: bool

    def __getitem__(self, item):
        if item == "account":
            return self.account
        elif item == "campaign":
            return self.campaign
        elif item == "group":
            return self.group

    def __setitem__(self, key, value):
        if key == "account":
            self.account = value
        elif key == "campaign":
            self.campaign = value
        elif key == "group":
            self.group = value


class StatisticsUTMFiltersData(BaseModel):
    date: Tuple[Optional[ConstrainedDate], Optional[ConstrainedDate]]
    utm_source: Optional[str]
    utm_medium: Optional[str]
    utm_campaign: Optional[str]
    utm_term: Optional[str]
    utm_content: Optional[str]
    groupby: str
    only_ru: bool

    def __getitem__(self, item):
        if item == "utm_source":
            return self.utm_source
        elif item == "utm_medium":
            return self.utm_medium
        elif item == "utm_campaign":
            return self.utm_campaign
        elif item == "utm_term":
            return self.utm_term
        elif item == "utm_content":
            return self.utm_content

    def __setitem__(self, key, value):
        if key == "utm_source":
            self.utm_source = value
        elif key == "utm_medium":
            self.utm_medium = value
        elif key == "utm_campaign":
            self.utm_campaign = value
        elif key == "utm_term":
            self.utm_term = value
        elif key == "utm_content":
            self.utm_content = value


class StatisticsView(TemplateView):
    template_name = "statistics/index.html"
    title = "Статистика"
    statistics = None
    extras = None
    filters = None

    def get_extras(self) -> Dict[str, Any]:
        providers = StatisticsProviderEnum.dict()
        groupby = StatisticsGroupByEnum.dict()
        data = {
            "providers": list(map(lambda item: (item[0], item[1]), providers.items())),
            "groupby": list(map(lambda item: (item[0], item[1]), groupby.items())),
        }
        return data

    def get_filters(self, source: ImmutableMultiDict) -> StatisticsFiltersData:
        date = [source.get("date_from") or None, source.get("date_to") or None]
        provider = source.get("provider")
        account = source.get("account")
        campaign = source.get("campaign")
        group = source.get("group")
        groupby = source.get("groupby")

        # provider
        providers = list(map(lambda item: item[0], self.extras.get("providers")))
        if provider not in providers:
            provider = None

        if provider:
            available_accounts = list(
                map(
                    lambda item: str(item.get("value")),
                    requests.get(
                        f"{request.host_url}api/statistics/accounts/{provider}"
                    )
                    .json()
                    .get("accounts"),
                )
            )
            if account not in available_accounts:
                account = None
            if account:
                available_campaigns = list(
                    map(
                        lambda item: str(item.get("value")),
                        requests.get(
                            f"{request.host_url}api/statistics/campaigns/{provider}/{account}"
                        )
                        .json()
                        .get("campaigns"),
                    )
                )
                if campaign not in available_campaigns:
                    campaign = None
                if campaign:
                    available_groups = list(
                        map(
                            lambda item: str(item.get("value")),
                            requests.get(
                                f"{request.host_url}api/statistics/groups/{provider}/{campaign}"
                            )
                            .json()
                            .get("groups"),
                        )
                    )
                    if group not in available_groups:
                        group = None

        # groupby
        try:
            StatisticsGroupByEnum[groupby]
        except KeyError:
            groupby = StatisticsGroupByEnum.provider.name

        return StatisticsFiltersData(
            date=date,
            provider=provider,
            account=account,
            campaign=campaign,
            group=group,
            groupby=groupby,
        )

    def get_statistics(self) -> pandas.DataFrame:
        statistics = pickle_loader.statistics

        data = statistics.data
        data.date = pandas.to_datetime(data.date).dt.date

        if self.filters.date[0]:
            data = data[data.date >= self.filters.date[0]]
        if self.filters.date[1]:
            data = data[data.date <= self.filters.date[1]]
        if self.filters.provider:
            data = data[data.provider == self.filters.provider]
        if self.filters.account:
            data = data[data.account == self.filters.account]
        if self.filters.campaign:
            data = data[data.campaign == self.filters.campaign]
        if self.filters.group:
            data = data[data.group == self.filters.group]

        data = data.groupby(self.filters.groupby)

        output = pandas.DataFrame()

        for _id, item in data:
            if self.filters.groupby == StatisticsGroupByEnum.provider.name:
                name = StatisticsProviderEnum[_id].value
            else:
                info = {}
                if self.filters.groupby == StatisticsGroupByEnum.account.name:
                    info = statistics.accounts
                elif self.filters.groupby == StatisticsGroupByEnum.campaign.name:
                    info = statistics.campaigns
                elif self.filters.groupby == StatisticsGroupByEnum.group.name:
                    info = statistics.groups
                elif self.filters.groupby == StatisticsGroupByEnum.ad.name:
                    info = statistics.ads
                info_item = info.get(item.iloc[0].provider, {}).get(_id)
                name = info_item.get("name") if info_item else UNDEFINED

            output = output.append(
                {"Название": name, "Потрачено": ceil(item.spent.sum() * 100) / 100},
                ignore_index=True,
            )

        return output

    def get(self):
        self.extras = self.get_extras()
        self.filters = self.get_filters(request.args)
        self.statistics = self.get_statistics()

        self.context("extras", self.extras)
        self.context("filters", self.filters)
        self.context("statistics", self.statistics)

        return super().get()


class StatusColor(str, Enum):
    high = "high"
    middle = "middle"
    low = "low"


class Action(str, Enum):
    suppose = "Оставить"
    disable = "Выключить"
    pending = "Ждем"


class DetectPositive:
    def __call__(self, value) -> StatusColor:
        if value >= 0:
            return StatusColor.high
        elif -50 <= value < 0:
            return StatusColor.middle
        else:
            return StatusColor.low


class DetectActivity:
    def __call__(self, value) -> StatusColor:
        if value >= 30:
            return StatusColor.high
        elif 10 <= value < 29:
            return StatusColor.middle
        else:
            return StatusColor.low


class DetectAction:
    def __call__(
        self,
        positive_period: StatusColor,
        positive_30d: StatusColor,
        activity_period: StatusColor,
        activity_30d: StatusColor,
    ) -> Action:
        if activity_period == StatusColor.high:
            if positive_period in (StatusColor.high, StatusColor.middle):
                return Action.suppose
            elif positive_period == StatusColor.low:
                return Action.disable
        elif activity_period in (StatusColor.middle, StatusColor.low):
            if activity_30d == StatusColor.high:
                if positive_30d in (StatusColor.high, StatusColor.middle):
                    return Action.suppose
                elif positive_30d == StatusColor.low:
                    return Action.disable
            elif activity_30d in (StatusColor.middle, StatusColor.low):
                return Action.pending


detect_positive = DetectPositive()
detect_activity = DetectActivity()
detect_action = DetectAction()


class Calculate:
    _leads: pandas.DataFrame
    _statistics: pandas.DataFrame
    _filters: StatisticsRoistatFiltersData

    _data: pandas.DataFrame

    def __init__(
        self,
        leads: pandas.DataFrame,
        statistics: pandas.DataFrame,
        leads_30d: pandas.DataFrame,
        statistics_30d: pandas.DataFrame,
        filters: StatisticsRoistatFiltersData,
    ):
        self._leads = leads
        self._statistics = statistics
        self._leads_30d = leads_30d
        self._statistics_30d = statistics_30d
        self._filters = filters

        stats = dict(
            map(
                lambda item: (str(item[0]), item[1]),
                self._statistics.groupby(by=self._filters.groupby, dropna=False),
            )
        )
        stats_30d = dict(
            map(
                lambda item: (str(item[0]), item[1]),
                self._statistics_30d.groupby(by=self._filters.groupby, dropna=False),
            )
        )

        self._data = pandas.DataFrame(columns=self.columns.keys())
        for name, group in self._leads.groupby(by=self._filters.groupby, dropna=False):
            group_30d = self._leads_30d[self._leads_30d[self._filters.groupby] == name]

            leads = len(group)
            leads_30d = len(group_30d)
            if not leads:
                continue

            stats_group = stats.get(str(name))
            stats_group_30d = stats_30d.get(
                str(name), pandas.DataFrame(columns=self.columns.keys())
            )

            expenses = (
                round(stats_group.expenses.sum()) if stats_group is not None else 0
            )
            if not expenses and name != ":utm:email":
                expenses = leads * 400

            expenses_month = (
                round(stats_group_30d.expenses.sum())
                if stats_group_30d is not None
                else 0
            )
            if not expenses_month and name != ":utm:email":
                expenses_month = leads_30d * 400

            name = (
                stats_group[self._filters.groupby].unique()[0]
                if stats_group is not None
                else "undefined"
            )
            title = (
                stats_group[f"{self._filters.groupby}_title"].unique()[0]
                if stats_group is not None
                else "Undefined"
            )
            income = int(group.ipl.sum())
            income_month = int(group_30d.ipl.sum())
            ipl = int(round(income / leads)) if leads else 0
            profit = int(round(income - expenses - (leads * 250 + income * 0.35)))
            profit_30d = int(
                round(
                    income_month
                    - expenses_month
                    - (leads_30d * 250 + income_month * 0.35)
                )
            )
            ppl = int(round(profit / leads)) if leads else 0
            ppl_30d = int(round(profit_30d / leads_30d)) if leads_30d else 0
            cpl = int(round(expenses / leads)) if leads else 0
            ppl_range = detect_positive(ppl)
            ppl_30d_value = detect_positive(ppl_30d)
            leads_range = detect_activity(leads)
            leads_30d_value = detect_activity(leads_30d)
            action = detect_action(
                ppl_range, ppl_30d_value, leads_range, leads_30d_value
            )
            self._data = self._data.append(
                {
                    CalculateColumnEnum.name.name: (
                        "" if title is None else title,
                        action.name,
                        name,
                    ),
                    CalculateColumnEnum.leads.name: leads,
                    CalculateColumnEnum.leads_month.name: leads_30d,
                    CalculateColumnEnum.income.name: income,
                    CalculateColumnEnum.income_month.name: income_month,
                    CalculateColumnEnum.ipl.name: ipl,
                    CalculateColumnEnum.expenses.name: expenses,
                    CalculateColumnEnum.expenses_month.name: expenses_month,
                    CalculateColumnEnum.profit.name: profit,
                    CalculateColumnEnum.ppl.name: ppl,
                    CalculateColumnEnum.cpl.name: cpl,
                    CalculateColumnEnum.ppl_range.name: (ppl, ppl_range.value),
                    CalculateColumnEnum.ppl_30d.name: (ppl_30d, ppl_30d_value.value),
                    CalculateColumnEnum.leads_range.name: (leads, leads_range.value),
                    CalculateColumnEnum.leads_30d.name: (
                        leads_30d,
                        leads_30d_value.value,
                    ),
                    CalculateColumnEnum.action.name: (action.value, action.name),
                },
                ignore_index=True,
            )

        self._data = self._data.reset_index(drop=True)

    @property
    def columns(self) -> Dict[str, str]:
        return CalculateColumnEnum.dict()

    @property
    def data(self) -> pandas.DataFrame:
        return self._data


def extra_table(leads: pandas.DataFrame) -> pandas.DataFrame:
    target_audience = pickle_loader.target_audience
    column_names = ["Ответ 2", "Ответ 3", "Ответ 4", "Ответ 5"]
    subcategory_names = {
        "Ответ 2": "Возраст",
        "Ответ 3": "Профессия",
        "Ответ 4": "Доход",
        "Ответ 5": "Обучение",
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
        if column_name == column_names[0]:
            subcategories = sorted(list(leads[column_name].unique()))
            try:
                subcategories.pop(subcategories.index("до 18 лет"))
                subcategories.insert(0, "до 18 лет")
            except ValueError:
                pass
        elif column_name == column_names[2]:
            available = [
                "0 руб.",
                "до 30 000 руб.",
                "до 60 000 руб.",
                "до 100 000 руб.",
                "более 100 000 руб.",
            ]
            subcategories = (
                available
                + leads[~leads[column_name].isin(available)][column_name]
                .unique()
                .tolist()
            )
            for item in subcategories.copy():
                if item not in leads[column_name].unique():
                    subcategories.remove(item)
        else:
            subcategories = (
                leads[leads[column_name].isin(target_audience)][column_name]
                .unique()
                .tolist()
                + leads[~leads[column_name].isin(target_audience)][column_name]
                .unique()
                .tolist()
            )
        for subcategory in subcategories:
            result.append(
                [
                    subcategory,
                    len(leads[leads[column_name] == subcategory]),
                    str(
                        int(
                            round(
                                leads[leads[column_name] == subcategory].shape[0]
                                / leads.shape[0]
                                * 100,
                                0,
                            )
                        )
                    )
                    + " %",
                ]
            )
        output.update({column_name: pandas.DataFrame(result)})

    data = pandas.concat(
        [output["Ответ 2"], output["Ответ 3"], output["Ответ 4"], output["Ответ 5"]],
        axis=1,
    )
    data.columns = data.iloc[0, :].values
    data = data.iloc[1:, :].reset_index(drop=True).fillna("")
    data.columns = pandas.MultiIndex.from_tuples(
        [
            ("", item[0]) if pandas.isnull(item[1]) else item
            for item in data.columns.str.split(", ", expand=True).values
        ]
    )
    return data


class StatisticsRoistatView(TemplateView):
    template_name: str = "statistics/roistat/index.html"
    title: str = "Статистика Roistat"

    leads_full: pandas.DataFrame = None
    statistics_full: pandas.DataFrame = None

    order: List[Dict[str, str]]
    filters: StatisticsRoistatFiltersData = None
    leads: pandas.DataFrame = None
    statistics: pandas.DataFrame = None
    leads_30d: pandas.DataFrame = None
    statistics_30d: pandas.DataFrame = None
    extras = None

    output_columns: List[str] = [
        CalculateColumnEnum.name.name,
        CalculateColumnEnum.leads.name,
        CalculateColumnEnum.income.name,
        CalculateColumnEnum.ipl.name,
        CalculateColumnEnum.expenses.name,
        CalculateColumnEnum.profit.name,
        CalculateColumnEnum.ppl.name,
        CalculateColumnEnum.cpl.name,
        CalculateColumnEnum.ppl_range.name,
        CalculateColumnEnum.ppl_30d.name,
        CalculateColumnEnum.leads_range.name,
        CalculateColumnEnum.leads_30d.name,
        CalculateColumnEnum.action.name,
    ]

    def parse_order(self, order: str, available: List[str]) -> List[Dict[str, str]]:
        output = []
        if order:
            for item in order.split(","):
                direction = "asc"
                if item[0] == "-":
                    direction = "desc"
                    item = item[1:]
                if item in available:
                    output.append({"name": item, "direction": direction})
        return output

    def get_filters(self, source: ImmutableMultiDict) -> StatisticsFiltersData:
        date = [source.get("date_from") or None, source.get("date_to") or None]
        account = source.get("account", "__all__")
        campaign = source.get("campaign", "__all__")
        group = source.get("group", "__all__")
        groupby = source.get("groupby")
        only_ru = bool(source.get("only_ru"))

        if account == "__all__":
            account = None
        if campaign == "__all__":
            campaign = None
        if group == "__all__":
            group = None

        try:
            StatisticsRoistatGroupByEnum[groupby]
        except KeyError:
            groupby = StatisticsRoistatGroupByEnum.account.name

        return StatisticsRoistatFiltersData(
            date=date,
            account=account,
            campaign=campaign,
            group=group,
            groupby=groupby,
            only_ru=only_ru,
        )

    def get_statistics(self) -> Tuple[pandas.DataFrame, pandas.DataFrame]:
        leads = self.leads_full.copy()
        statistics = self.statistics_full.copy()

        tz = pytz.timezone("Europe/Moscow")
        date = list(self.filters.date)

        if date[0]:
            date_from = tz.localize(
                datetime.datetime(
                    year=date[0].year, month=date[0].month, day=date[0].day
                )
            )
            leads = leads[leads.date >= date_from]
            statistics = statistics[statistics.date >= date_from]

        if date[1]:
            date_to = tz.localize(
                datetime.datetime(
                    year=date[1].year, month=date[1].month, day=date[1].day
                )
            )
            leads = leads[leads.date <= date_to]
            statistics = statistics[statistics.date <= date_to]

        if self.filters.only_ru:
            leads = leads[leads.qa1 == "Россия"]

        return leads, statistics

    def get_statistics_30d(self) -> Tuple[pandas.DataFrame, pandas.DataFrame]:
        leads = self.leads_full.copy()
        statistics = self.statistics_full.copy()

        tz = pytz.timezone("Europe/Moscow")
        date = list(self.filters.date)

        if not date[1]:
            date[1] = tz.localize(datetime.datetime.now())
        date[0] = date[1] - datetime.timedelta(days=30)

        date_from = tz.localize(
            datetime.datetime(year=date[0].year, month=date[0].month, day=date[0].day)
        )
        leads = leads[leads.date >= date_from]
        statistics = statistics[statistics.date >= date_from]

        date_to = tz.localize(
            datetime.datetime(year=date[1].year, month=date[1].month, day=date[1].day)
        )
        leads = leads[leads.date <= date_to]
        statistics = statistics[statistics.date <= date_to]

        if self.filters.only_ru:
            leads = leads[leads.qa1 == "Россия"]

        return leads, statistics

    def get_extras_group(self, group: str) -> List[Tuple[str, str]]:
        stats_groups = []
        for name, item in self.statistics.groupby(group):
            stats_groups.append((name, item[f"{group}_title"].unique()[0]))
        leads_groups = []
        for name, item in self.leads.groupby(group):
            leads_groups.append(name)
        output = list(filter(lambda item: item[0] in leads_groups, stats_groups))
        if self.filters[group] not in list(map(lambda item: item[0], output)):
            self.filters[group] = None
        if self.filters[group] is not None:
            self.leads = self.leads[self.leads[group] == self.filters[group]]
            self.statistics = self.statistics[
                self.statistics[group] == self.filters[group]
            ]
        return output

    def get_extras(self) -> Dict[str, Any]:
        accounts = self.get_extras_group("account")
        campaigns = self.get_extras_group("campaign")
        groups = self.get_extras_group("group")
        return {
            "groupby": list(
                map(
                    lambda item: (item[0], item[1]),
                    StatisticsRoistatGroupByEnum.dict().items(),
                )
            ),
            "accounts": sorted(accounts, key=lambda item: item[1]),
            "campaigns": sorted(campaigns, key=lambda item: item[1]),
            "groups": sorted(groups, key=lambda item: item[1]),
            "columns": CalculateColumnEnum.dict(),
        }

    def get_details(
        self, name: str = None
    ) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        if name is None:
            return None, None

        if name == "undefined":
            name = ""
        leads = self.leads[self.leads[self.filters.groupby] == name]
        statistics = self.statistics[self.statistics[self.filters.groupby] == name]

        leads = (
            leads[
                [
                    "date",
                    "ipl",
                    "qa1",
                    "qa2",
                    "qa3",
                    "qa4",
                    "qa5",
                    "qa6",
                    "email",
                    "url",
                ]
            ]
            .rename(
                columns={
                    "date": "Дата",
                    "ipl": "IPL",
                    "qa1": "Ответ 1",
                    "qa2": "Ответ 2",
                    "qa3": "Ответ 3",
                    "qa4": "Ответ 4",
                    "qa5": "Ответ 5",
                    "qa6": "Ответ 6",
                    "email": "E-mail",
                    "url": "URL",
                }
            )
            .sort_values(by=["Дата"])
            .reset_index(drop=True)
        )

        extra = extra_table(leads)

        stats_grouped = statistics[f"{self.filters.groupby}_title"].unique()
        return (
            {
                "title": "Дополнительная таблица",
                "columns0": extra.columns.get_level_values(0).unique(),
                "columns1": extra.columns.get_level_values(1),
                "data": extra,
            },
            {
                "title": f"Лиды в разбивке по {StatisticsRoistatGroupByEnum[self.filters.groupby].value} = {stats_grouped[0] if len(stats_grouped) else 'Undefined'}",
                "title_short": f"{StatisticsRoistatGroupByEnum[self.filters.groupby].value} = {stats_grouped[0] if len(stats_grouped) else 'Undefined'}",
                "data": leads,
            },
        )

    def get_download_statistics(
        self, workbook: Workbook, data: pandas.DataFrame, total_data: dict
    ):
        worksheet = workbook.add_worksheet("Статистика")
        worksheet.write_row(
            0,
            0,
            list(
                map(
                    lambda item: self.extras.get("columns").get(item),
                    self.output_columns,
                )
            ),
        )
        exclude_columns = []
        for index, item in enumerate(data.columns):
            if item not in self.output_columns:
                exclude_columns.append(index)
        index = -1
        for index, row in data.iterrows():
            item = list(row.values)
            item[0] = item[0][0]
            item[11] = item[11][1]
            item[12] = item[12][1]
            item[13] = item[13][1]
            item[14] = item[14][1]
            item[15] = item[15][1]
            worksheet.write_row(
                index + 1,
                0,
                list(
                    dict(
                        filter(
                            lambda value: value[0] not in exclude_columns,
                            enumerate(item),
                        )
                    ).values()
                ),
            )
        total_values = list(total_data.values())
        total_values[0] = total_values[0][0]
        total_values[11] = total_values[11][1]
        total_values[12] = total_values[12][1]
        total_values[13] = total_values[13][1]
        total_values[14] = total_values[14][1]
        total_values[15] = total_values[15][1]
        worksheet.write_row(
            index + 2,
            0,
            list(
                dict(
                    filter(
                        lambda value: value[0] not in exclude_columns,
                        enumerate(total_values),
                    )
                ).values()
            ),
        )
        worksheet.autofilter("A1:M1")

    def get_download_extra(self, workbook: Workbook, data):
        worksheet = workbook.add_worksheet(data.get("title"))
        for index, column in enumerate(data.get("columns0")):
            worksheet.merge_range(0, index * 3, 0, index * 3 + 2, column)
        worksheet.write_row(1, 0, data.get("columns1"))
        for index, row in data.get("data").iterrows():
            values = list(row.values)
            worksheet.write_row(index + 2, 0, values)

    def get_download_leads(self, workbook: Workbook, data):
        worksheet = workbook.add_worksheet(
            f'{data.get("title_short")[:28]}{"..." if len(data.get("title_short")) > 31 else ""}'
        )
        worksheet.write_row(0, 0, data.get("data").columns)
        for index, row in data.get("data").iterrows():
            values = list(row.values)
            values[0] = values[0].strftime("%Y-%m-%d")
            worksheet.write_row(index + 1, 0, values)
        worksheet.autofilter("A1:H1")

    def get(self):
        self.leads_full = pickle_loader.roistat_leads
        self.statistics_full = pickle_loader.roistat_statistics
        self.filters = self.get_filters(request.args)
        self.leads, self.statistics = self.get_statistics()
        self.leads_30d, self.statistics_30d = self.get_statistics_30d()
        self.extras = self.get_extras()
        self.extras["columns"]["name"] = StatisticsRoistatGroupByEnum[
            self.filters.groupby
        ].value

        calc = Calculate(
            self.leads,
            self.statistics,
            self.leads_30d,
            self.statistics_30d,
            self.filters,
        )

        total_data = dict(zip(calc.columns.keys(), [None] * len(calc.columns.keys())))

        total_title = "Итого"
        total_leads = calc.data.leads.sum()
        total_leads_30d = calc.data.leads_month.sum()
        total_income = calc.data.income.sum()
        total_income_30d = calc.data.income_month.sum()
        total_ipl = int(round(total_income / total_leads)) if total_leads else 0
        total_expenses = round(calc.data.expenses.sum())
        total_expenses_30d = round(calc.data.expenses_month.sum())
        total_profit = int(
            round(
                total_income
                - total_expenses
                - (total_leads * 250 + total_income * 0.35)
            )
        )
        total_profit_30d = int(
            round(
                total_income_30d
                - total_expenses_30d
                - (total_leads_30d * 250 + total_income_30d * 0.35)
            )
        )
        total_ppl = int(round(total_profit / total_leads)) if total_leads else 0
        total_ppl_30d = (
            int(round(total_profit_30d / total_leads_30d)) if total_leads_30d else 0
        )
        total_cpl = int(round(total_expenses / total_leads)) if total_leads else 0
        total_ppl_range = detect_positive(total_ppl)
        total_ppl_30d_value = detect_positive(total_ppl_30d)
        total_leads_range = detect_activity(total_leads)
        total_leads_30d_value = detect_activity(total_leads_30d)
        total_action = detect_action(
            total_ppl_range,
            total_ppl_30d_value,
            total_leads_range,
            total_leads_30d_value,
        )

        total_data.update(
            {
                CalculateColumnEnum.name.name: (total_title, total_action.name),
                CalculateColumnEnum.leads.name: total_leads,
                CalculateColumnEnum.leads_month.name: total_leads_30d,
                CalculateColumnEnum.income.name: total_income,
                CalculateColumnEnum.income_month.name: total_income_30d,
                CalculateColumnEnum.ipl.name: total_ipl,
                CalculateColumnEnum.expenses.name: total_expenses,
                CalculateColumnEnum.expenses_month.name: total_expenses_30d,
                CalculateColumnEnum.profit.name: total_profit,
                CalculateColumnEnum.ppl.name: total_ppl,
                CalculateColumnEnum.cpl.name: total_cpl,
                CalculateColumnEnum.ppl_range.name: (total_ppl, total_ppl_range.value),
                CalculateColumnEnum.ppl_30d.name: (
                    total_ppl_30d,
                    total_ppl_30d_value.value,
                ),
                CalculateColumnEnum.leads_range.name: (
                    total_leads,
                    total_leads_range.value,
                ),
                CalculateColumnEnum.leads_30d.name: (
                    total_leads_30d,
                    total_leads_30d_value.value,
                ),
                CalculateColumnEnum.action.name: (
                    total_action.value,
                    total_action.name,
                ),
            }
        )

        details = request.args.get("details")
        if details not in list(map(lambda item: item[2], calc.data.name.unique())):
            details = None
        details_extra, details_leads = self.get_details(details)

        if "download" in request.args.keys():
            target = tempfile.NamedTemporaryFile(suffix=".xlsx")
            workbook = Workbook(target.name)
            self.get_download_statistics(workbook, calc.data, total_data)
            if details_extra:
                self.get_download_extra(workbook, details_extra)
            if details_leads:
                self.get_download_leads(workbook, details_leads)
            workbook.close()
            return send_file(
                workbook.filename,
                download_name=f"statistics.xlsx",
                as_attachment=True,
            )

        url = urlparse(request.url)
        qs = dict(parse_qsl(url.query))
        qs.pop("details", None)
        link = request.path
        if qs:
            link = f"{link}?{urlencode(qs)}"

        data = calc.data
        order = self.parse_order(
            request.args.get("orderby", ""), list(calc.data.columns)
        )

        data.sort_values(
            by=list(map(lambda item: item.get("name"), order)),
            ascending=list(map(lambda item: item.get("direction") == "asc", order)),
            inplace=True,
        )

        self.context("filters", self.filters)
        self.context("extras", self.extras)
        self.context("columns", self.output_columns)
        self.context("data", calc.data)
        self.context("total", pandas.Series(total_data))
        self.context("url", link)
        self.context("qs_tail", "&" if qs else "?")
        self.context("details", details)
        self.context("details_extra", details_extra)
        self.context("details_leads", details_leads)
        self.context("order", order)

        return super().get()


class StatisticsUTMView(TemplateView):
    template_name: str = "statistics/utm/index.html"
    title: str = "Статистика UTM"

    leads: pandas.DataFrame = None
    filters: StatisticsUTMFiltersData = None

    output_columns: List[str] = [
        StatisticsUTMColumnEnum.name.name,
        StatisticsUTMColumnEnum.leads.name,
        StatisticsUTMColumnEnum.income.name,
        StatisticsUTMColumnEnum.ipl.name,
    ]

    def parse_order(self, order: str, available: List[str]) -> List[Dict[str, str]]:
        output = []
        if order:
            for item in order.split(","):
                direction = "asc"
                if item[0] == "-":
                    direction = "desc"
                    item = item[1:]
                if item in available:
                    output.append({"name": item, "direction": direction})
        return output

    def get_filters(self, source: ImmutableMultiDict) -> StatisticsUTMFiltersData:
        date = [source.get("date_from") or None, source.get("date_to") or None]
        utm_source = source.get("utm_source", "__all__")
        utm_medium = source.get("utm_medium", "__all__")
        utm_campaign = source.get("utm_campaign", "__all__")
        utm_term = source.get("utm_term", "__all__")
        utm_content = source.get("utm_content", "__all__")
        groupby = source.get("groupby")
        only_ru = bool(source.get("only_ru"))

        if utm_source == "__all__":
            utm_source = None
        if utm_medium == "__all__":
            utm_medium = None
        if utm_campaign == "__all__":
            utm_campaign = None
        if utm_term == "__all__":
            utm_term = None
        if utm_content == "__all__":
            utm_content = None

        try:
            StatisticsUTMGroupByEnum[groupby]
        except KeyError:
            groupby = StatisticsUTMGroupByEnum.utm_source.name

        return StatisticsUTMFiltersData(
            date=date,
            utm_source=utm_source,
            utm_medium=utm_medium,
            utm_campaign=utm_campaign,
            utm_term=utm_term,
            utm_content=utm_content,
            groupby=groupby,
            only_ru=only_ru,
        )

    def get_extras_group(self, group: str) -> List[Tuple[str, str]]:
        values = list(self.leads[group].unique())
        output = tuple(zip(values, values))
        if self.filters[group] not in values:
            self.filters[group] = None
        if self.filters[group] is not None:
            self.leads = self.leads[self.leads[group] == self.filters[group]]
        return output

    def get_extras(self) -> Dict[str, Any]:
        utm_sources = self.get_extras_group("utm_source")
        utm_mediums = self.get_extras_group("utm_medium")
        utm_campaigns = self.get_extras_group("utm_campaign")
        utm_terms = self.get_extras_group("utm_term")
        utm_contents = self.get_extras_group("utm_content")

        return {
            "groupby": list(
                map(
                    lambda item: (item[0], item[1]),
                    StatisticsUTMGroupByEnum.dict().items(),
                )
            ),
            "utm_sources": sorted(utm_sources, key=lambda item: item[1]),
            "utm_mediums": sorted(utm_mediums, key=lambda item: item[1]),
            "utm_campaigns": sorted(utm_campaigns, key=lambda item: item[1]),
            "utm_terms": sorted(utm_terms, key=lambda item: item[1]),
            "utm_contents": sorted(utm_contents, key=lambda item: item[1]),
            "columns": StatisticsUTMColumnEnum.dict(),
        }

    def get_details(
        self, name: str = None
    ) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        if name is None:
            return None, None

        title = name
        if not title:
            title = "Undefined"

        leads = self.leads[self.leads[self.filters.groupby] == name]

        leads = (
            leads[
                [
                    "date",
                    "ipl",
                    "qa1",
                    "qa2",
                    "qa3",
                    "qa4",
                    "qa5",
                    "qa6",
                    "email",
                    "url",
                ]
            ]
            .rename(
                columns={
                    "date": "Дата",
                    "ipl": "IPL",
                    "qa1": "Ответ 1",
                    "qa2": "Ответ 2",
                    "qa3": "Ответ 3",
                    "qa4": "Ответ 4",
                    "qa5": "Ответ 5",
                    "qa6": "Ответ 6",
                    "email": "E-mail",
                    "url": "URL",
                }
            )
            .sort_values(by=["Дата"])
            .reset_index(drop=True)
        )

        extra = extra_table(leads)

        return (
            {
                "title": "Дополнительная таблица",
                "columns0": extra.columns.get_level_values(0).unique(),
                "columns1": extra.columns.get_level_values(1),
                "data": extra,
            },
            {
                "title": f"Лиды в разбивке по {StatisticsUTMGroupByEnum[self.filters.groupby].value} = {title}",
                "title_short": f"{StatisticsUTMGroupByEnum[self.filters.groupby].value} = {title}",
                "data": leads,
            },
        )

    def get_statistics(self) -> pandas.DataFrame:
        leads = self.leads.copy()

        tz = pytz.timezone("Europe/Moscow")
        date = list(self.filters.date)

        if date[0]:
            date_from = tz.localize(
                datetime.datetime(
                    year=date[0].year, month=date[0].month, day=date[0].day
                )
            )
            leads = leads[leads.date >= date_from]

        if date[1]:
            date_to = tz.localize(
                datetime.datetime(
                    year=date[1].year, month=date[1].month, day=date[1].day
                )
            )
            leads = leads[leads.date <= date_to]

        if self.filters.only_ru:
            leads = leads[leads.qa1 == "Россия"]

        return leads

    def get(self):
        self.leads = pickle_loader.roistat_leads
        self.filters = self.get_filters(request.args)
        self.leads = self.get_statistics()
        self.extras = self.get_extras()
        self.extras["columns"]["name"] = StatisticsUTMGroupByEnum[
            self.filters.groupby
        ].value

        data = pandas.DataFrame(columns=self.extras["columns"].keys())
        for name, group in self.leads.groupby(by=self.filters.groupby, dropna=False):
            leads = len(group)
            if not leads:
                continue
            income = int(group.ipl.sum())
            ipl = int(round(income / leads)) if leads else 0
            data = data.append(
                {
                    StatisticsUTMColumnEnum.name.name: (
                        name,
                        "Undefined" if not name else name,
                    ),
                    StatisticsUTMColumnEnum.leads.name: leads,
                    StatisticsUTMColumnEnum.income.name: income,
                    StatisticsUTMColumnEnum.ipl.name: ipl,
                },
                ignore_index=True,
            )

        data = data.reset_index(drop=True)

        total_data = dict(
            zip(
                self.extras["columns"].keys(),
                [None] * len(self.extras["columns"].keys()),
            )
        )
        total_title = "Итого"
        total_leads = data.leads.sum()
        total_income = data.income.sum()
        total_ipl = int(round(total_income / total_leads)) if total_leads else 0
        total_data.update(
            {
                CalculateColumnEnum.name.name: total_title,
                CalculateColumnEnum.leads.name: total_leads,
                CalculateColumnEnum.income.name: total_income,
                CalculateColumnEnum.ipl.name: total_ipl,
            }
        )

        details = request.args.get("details")
        if details not in list(map(lambda item: item[0], data.name.unique())):
            details = None
        details_extra, details_leads = self.get_details(details)

        url = urlparse(request.url)
        qs = dict(parse_qsl(url.query))
        qs.pop("details", None)
        link = request.path
        if qs:
            link = f"{link}?{urlencode(qs)}"

        order = self.parse_order(request.args.get("orderby", ""), list(data.columns))

        data.sort_values(
            by=list(map(lambda item: item.get("name"), order)),
            ascending=list(map(lambda item: item.get("direction") == "asc", order)),
            inplace=True,
        )

        self.context("filters", self.filters)
        self.context("extras", self.extras)
        self.context("columns", self.output_columns)
        self.context("data", data)
        self.context("total", pandas.Series(total_data))
        self.context("url", link)
        self.context("qs_tail", "&" if qs else "?")
        self.context("details", details)
        self.context("details_extra", details_extra)
        self.context("details_leads", details_leads)
        self.context("order", order)

        return super().get()


class VKStatisticsView(TemplateView):
    template_name = "vk/statistics.html"
    title = "Статистика объявлений в ВК"

    @property
    def posts(self) -> Dict[int, vk_data.WallPostData]:
        return dict(map(lambda item: (item.ad_id, item), vk_reader("wall.get")))

    @property
    def accounts(self) -> Dict[int, vk_data.AccountData]:
        return dict(
            map(lambda item: (item.account_id, item), vk_reader("ads.getAccounts"))
        )

    @property
    def clients(self) -> Dict[int, vk_data.ClientData]:
        return dict(map(lambda item: (item.id, item), vk_reader("ads.getClients")))

    @property
    def campaigns(self) -> Dict[int, vk_data.CampaignData]:
        return dict(map(lambda item: (item.id, item), vk_reader("ads.getCampaigns")))

    @property
    def ads(self) -> Dict[int, vk_data.AdData]:
        return dict(map(lambda item: (item.id, item), vk_reader("ads.getAds")))

    @property
    def ads_layout(self) -> Dict[int, vk_data.AdLayoutData]:
        return dict(map(lambda item: (item.id, item), vk_reader("ads.getAdsLayout")))

    def set_context(self, data: Dict[str, Any]):
        for name, value in data.items():
            self.context(name, value)

    def get_args(self) -> Dict[str, Any]:
        return {
            "group_by": request.args.get("group_by", "ad_id"),
            "account": request.args.get("account_id") or None,
            "client": request.args.get("client_id") or None,
            "campaign": request.args.get("campaign_id") or None,
        }

    def get_stats(self) -> pandas.DataFrame:
        # TODO: убрать ограничение на 100 записей
        stats = vk_reader("collectStatisticsDataFrame")[:100]
        stats["spent"] = stats["spent"].apply(lambda value: "%.2f" % value)
        stats["ctr"] = stats["ctr"].apply(lambda value: "%.3f" % value)
        stats["effective_cost_per_click"] = stats["effective_cost_per_click"].apply(
            lambda value: "%.3f" % value
        )
        stats["effective_cost_per_mille"] = stats["effective_cost_per_mille"].apply(
            lambda value: "%.3f" % value
        )
        stats["effective_cpf"] = stats["effective_cpf"].apply(
            lambda value: "%.3f" % value
        )
        stats["effective_cost_per_message"] = stats["effective_cost_per_message"].apply(
            lambda value: "%.2f" % value
        )
        stats["sex__m__impressions_rate"] = stats["sex__m__impressions_rate"].apply(
            lambda value: "%.3f" % value
        )
        stats["sex__f__impressions_rate"] = stats["sex__f__impressions_rate"].apply(
            lambda value: "%.3f" % value
        )
        stats["sex__m__clicks_rate"] = stats["sex__m__clicks_rate"].apply(
            lambda value: "%.3f" % value
        )
        stats["sex__f__clicks_rate"] = stats["sex__f__clicks_rate"].apply(
            lambda value: "%.3f" % value
        )
        stats["age__12_18__impressions_rate"] = stats[
            "age__12_18__impressions_rate"
        ].apply(lambda value: "%.3f" % value)
        stats["age__18_21__impressions_rate"] = stats[
            "age__18_21__impressions_rate"
        ].apply(lambda value: "%.3f" % value)
        stats["age__21_24__impressions_rate"] = stats[
            "age__21_24__impressions_rate"
        ].apply(lambda value: "%.3f" % value)
        stats["age__24_27__impressions_rate"] = stats[
            "age__24_27__impressions_rate"
        ].apply(lambda value: "%.3f" % value)
        stats["age__27_30__impressions_rate"] = stats[
            "age__27_30__impressions_rate"
        ].apply(lambda value: "%.3f" % value)
        stats["age__30_35__impressions_rate"] = stats[
            "age__30_35__impressions_rate"
        ].apply(lambda value: "%.3f" % value)
        stats["age__35_45__impressions_rate"] = stats[
            "age__35_45__impressions_rate"
        ].apply(lambda value: "%.3f" % value)
        stats["age__45_100__impressions_rate"] = stats[
            "age__45_100__impressions_rate"
        ].apply(lambda value: "%.3f" % value)
        stats["age__12_18__clicks_rate"] = stats["age__12_18__clicks_rate"].apply(
            lambda value: "%.3f" % value
        )
        stats["age__18_21__clicks_rate"] = stats["age__18_21__clicks_rate"].apply(
            lambda value: "%.3f" % value
        )
        stats["age__21_24__clicks_rate"] = stats["age__21_24__clicks_rate"].apply(
            lambda value: "%.3f" % value
        )
        stats["age__24_27__clicks_rate"] = stats["age__24_27__clicks_rate"].apply(
            lambda value: "%.3f" % value
        )
        stats["age__27_30__clicks_rate"] = stats["age__27_30__clicks_rate"].apply(
            lambda value: "%.3f" % value
        )
        stats["age__30_35__clicks_rate"] = stats["age__30_35__clicks_rate"].apply(
            lambda value: "%.3f" % value
        )
        stats["age__35_45__clicks_rate"] = stats["age__35_45__clicks_rate"].apply(
            lambda value: "%.3f" % value
        )
        stats["age__45_100__clicks_rate"] = stats["age__45_100__clicks_rate"].apply(
            lambda value: "%.3f" % value
        )
        return stats

    def get(self):
        self.context("posts", self.posts)
        self.context("accounts", self.accounts)
        self.context("clients", self.clients)
        self.context("campaigns", self.campaigns)
        self.context("ads", self.ads)
        self.context("ads_layout", self.ads_layout)
        self.set_context({"fields": self.get_args()})
        self.context("stats", self.get_stats())
        return super().get()


class VKCreateAdView(TemplateView):
    template_name = "vk/create-ad.html"
    title = "Создание объявления в ВК"

    def set_context(self, data: Dict[str, Any]):
        for name, value in data.items():
            self.context(name, value)

    def form_context_add(self, **kwargs):
        self.set_context(
            {
                "fields": {
                    "account_id": kwargs.get("account_id", ""),
                    "campaign_id": kwargs.get("campaign_id", ""),
                    "cost_type": kwargs.get("cost_type", ""),
                    "ad_format": kwargs.get("ad_format", ""),
                    "link_url": kwargs.get("link_url", ""),
                    "title": kwargs.get("title", ""),
                    "description": kwargs.get("description", ""),
                    "photo": kwargs.get("photo", ""),
                    "goal_type": kwargs.get("goal_type", ""),
                }
            }
        )

    def get_photo_url(self, file, ad_format: int) -> str:
        upload_url = vk("ads.getUploadURL", ad_format=ad_format)
        target = tempfile.NamedTemporaryFile(suffix=Path(file.filename).suffix)
        target.writelines(file.stream.readlines())
        with open(target.name, "rb") as target_ref:
            response = requests.post(upload_url, files={"file": target_ref})
            output = response.json()
            if output.get("errcode"):
                raise Exception(output)
            return output.get("photo", "")

    def get(self):
        self.form_context_add()
        return super().get()

    def post(self):
        account_id = request.form.get("account_id")
        campaign_id = request.form.get("campaign_id")
        cost_type = request.form.get("cost_type")
        ad_format = request.form.get("ad_format")
        link_url = request.form.get("link_url", "")
        title = request.form.get("title", "")
        description = request.form.get("description", "")
        goal_type = request.form.get("goal_type", "")
        form_context = {
            "account_id": account_id,
            "campaign_id": campaign_id,
            "cost_type": cost_type,
            "ad_format": ad_format,
            "link_url": link_url,
            "title": title,
            "description": description,
            "goal_type": goal_type,
        }
        try:
            photo = request.form.get("photo") or self.get_photo_url(
                request.files.get("photo_file"), ad_format
            )
            form_context.update({"photo": photo})
        except Exception as error:
            self.form_context_add(**form_context)
            self.context("error", f"Основное изображение: {error}")
            return self.render()
        self.form_context_add(**form_context)
        params = {
            "account_id": int(account_id) if account_id else "",
            "data": json.dumps(
                [
                    {
                        "campaign_id": int(campaign_id) if campaign_id else "",
                        "cost_type": int(cost_type) if cost_type else "",
                        "ad_format": int(ad_format) if ad_format else "",
                        "link_url": link_url,
                        "title": title,
                        "description": description,
                        "photo": photo,
                        "goal_type": int(goal_type) if goal_type else "",
                    }
                ]
            ),
        }
        try:
            response = vk("ads.createAds", **params)
        except Exception as error:
            self.context("error", error)
        return self.render()


class VKXlsxAdsView(MethodView):
    @decorators.auth
    def dispatch_request(self, *args, **kwargs):
        return super().dispatch_request(*args, **kwargs)

    def create_ads(
        self, workbook: Workbook, categories: Dict[int, str], countries: Dict[int, str]
    ):
        worksheet = workbook.add_worksheet("Ads")
        columns = [
            "id",
            "title",
            "text",
            "image",
            "target_url",
            "ad_format",
            "cost_type",
            "cpc",
            "cpm",
            "ocpm",
            "goal_type",
            "ad_platform",
            "publisher_platforms",
            "all_limit",
            "day_limit",
            "autobidding",
            "autobidding_max_cost",
            "category1_id",
            "category2_id",
            "status",
            "approved",
            "targeting__sex",
            "targeting__age_from",
            "targeting__age_to",
            "targeting__birthday",
            "targeting__country",
            "targeting__cities",
            "targeting__cities_not",
            "targeting__statuses",
            "targeting__groups",
            "targeting__groups_not",
            "targeting__apps",
            "targeting__apps_not",
            "targeting__districts",
            "targeting__stations",
            "targeting__streets",
            "targeting__schools",
            "targeting__positions",
            "targeting__religions",
            "targeting__interest_categories",
            "targeting__interests",
            "targeting__user_devices",
            "targeting__user_os",
            "targeting__user_browsers",
            "targeting__retargeting_groups",
            "targeting__retargeting_groups_not",
            "targeting__count",
        ]
        ads = vk_reader("ads.getAds")

        def link_column(index: int) -> str:
            return f"internal:'Description \"Ads\"'!A{index + 1}"

        def link_enum(name: str, enum_data: Enum) -> str:
            return f"internal:'{name}'!A{list(type(enum_data)).index(enum_data) + 2}"

        def link_category(index: int) -> str:
            return f"internal:'categories'!A{index}"

        def link_country(index: int) -> str:
            return f"internal:'targeting__country'!A{index}"

        for index, column in enumerate(columns):
            worksheet.write_url(0, index, link_column(index + 1), string=column)

        posts = dict(map(lambda post: (post.ad_id, post.dict()), vk_reader("wall.get")))
        targeting = dict(
            map(lambda item: (item.id, item), vk_reader("ads.getAdsTargeting"))
        )
        for row, ad in enumerate(ads):
            target = targeting.get(ad.id, vk_data.AdTargetingData())
            post = posts.get(ad.id, {})
            worksheet.write_row(
                row + 1,
                0,
                [
                    ad.id,
                    post.get("title", ""),
                    post.get("text", ""),
                    post.get("image", ""),
                    post.get("target_url", ""),
                    str(ad.ad_format or ""),
                    str(ad.cost_type or ""),
                    ad.cpc / 100 if ad.cpc else None,
                    ad.cpm / 100 if ad.cpm else None,
                    ad.ocpm / 100 if ad.ocpm else None,
                    str(ad.goal_type or ""),
                    str(ad.ad_platform or ""),
                    str(ad.publisher_platforms or ""),
                    ad.all_limit,
                    ad.day_limit,
                    str(ad.autobidding or ""),
                    ad.autobidding_max_cost / 100 if ad.autobidding_max_cost else None,
                    str(ad.category1_id or ""),
                    str(ad.category2_id or ""),
                    str(ad.status or ""),
                    str(ad.approved or ""),
                    str(target.sex or ""),
                    target.age_from,
                    target.age_to,
                    target.birthday,
                    str(target.country or ""),
                    ",".join(list(map(lambda item: str(item), target.cities))),
                    ",".join(list(map(lambda item: str(item), target.cities_not))),
                    ",".join(list(map(lambda item: str(item.value), target.statuses))),
                    ",".join(list(map(lambda item: str(item), target.groups))),
                    ",".join(list(map(lambda item: str(item), target.groups_not))),
                    ",".join(list(map(lambda item: str(item), target.apps))),
                    ",".join(list(map(lambda item: str(item), target.apps_not))),
                    ",".join(list(map(lambda item: str(item), target.districts))),
                    ",".join(list(map(lambda item: str(item), target.stations))),
                    ",".join(list(map(lambda item: str(item), target.streets))),
                    ",".join(list(map(lambda item: str(item), target.schools))),
                    ",".join(list(map(lambda item: str(item), target.positions))),
                    ",".join(list(map(lambda item: str(item), target.religions))),
                    ",".join(
                        list(map(lambda item: str(item), target.interest_categories))
                    ),
                    ",".join(list(map(lambda item: str(item), target.interests))),
                    ",".join(list(map(lambda item: str(item), target.user_devices))),
                    ",".join(list(map(lambda item: str(item), target.user_os))),
                    ",".join(list(map(lambda item: str(item), target.user_browsers))),
                    ",".join(
                        list(map(lambda item: str(item), target.retargeting_groups))
                    ),
                    ",".join(
                        list(map(lambda item: str(item), target.retargeting_groups_not))
                    ),
                    target.count,
                ],
            )
            if ad.ad_format:
                worksheet.write_url(
                    row + 1,
                    5,
                    link_enum("ad_format", ad.ad_format),
                    string=str(ad.ad_format.value),
                )
            if ad.cost_type:
                worksheet.write_url(
                    row + 1,
                    6,
                    link_enum("cost_type", ad.cost_type),
                    string=str(ad.cost_type.value),
                )
            if ad.goal_type:
                worksheet.write_url(
                    row + 1,
                    10,
                    link_enum("goal_type", ad.goal_type),
                    string=str(ad.goal_type.value),
                )
            if ad.ad_platform:
                worksheet.write_url(
                    row + 1,
                    11,
                    link_enum("ad_platform", ad.ad_platform),
                    string=str(ad.ad_platform.value),
                )
            if ad.publisher_platforms:
                worksheet.write_url(
                    row + 1,
                    12,
                    link_enum("publisher_platforms", ad.publisher_platforms),
                    string=str(ad.publisher_platforms.value),
                )
            if ad.autobidding:
                worksheet.write_url(
                    row + 1,
                    15,
                    link_enum("autobidding", ad.autobidding),
                    string=str(ad.autobidding.value),
                )
            categories_list = list(OrderedDict(categories).keys())
            category1_id = int(ad.category1_id or 0)
            if ad.category1_id and category1_id in categories_list:
                worksheet.write_url(
                    row + 1,
                    17,
                    link_category(categories_list.index(category1_id) + 2),
                    string=str(ad.category1_id),
                )
            category2_id = int(ad.category2_id or 0)
            if ad.category2_id and category2_id in categories_list:
                worksheet.write_url(
                    row + 1,
                    18,
                    link_category(categories_list.index(category2_id) + 2),
                    string=str(ad.category2_id),
                )
            if ad.status:
                worksheet.write_url(
                    row + 1,
                    19,
                    link_enum("status", ad.status),
                    string=str(ad.status.value),
                )
            if ad.approved:
                worksheet.write_url(
                    row + 1,
                    20,
                    link_enum("approved", ad.approved),
                    string=str(ad.approved.value),
                )
            if target.sex:
                worksheet.write_url(
                    row + 1,
                    21,
                    link_enum("targeting__sex", target.sex),
                    string=str(target.sex.value),
                )
            countries_list = list(OrderedDict(countries).keys())
            country = int(target.country or 0)
            if target.country and country in countries_list:
                worksheet.write_url(
                    row + 1,
                    25,
                    link_country(countries_list.index(country) + 2),
                    string=str(target.country),
                )
        worksheet.autofilter("A1:AU1")

    def create_statistics(self, workbook: Workbook):
        worksheet = workbook.add_worksheet("Statistics")
        data = vk_reader("collectStatisticsDataFrame")
        data = data.drop(
            [
                "account_id",
                "client_id",
                "campaign_id",
                "message_sends",
            ],
            axis=1,
        )

        def link_column(index: int) -> str:
            return f"internal:'Description \"Statistics\"'!A{index + 1}"

        for index, column in enumerate(data.columns):
            worksheet.write_url(0, index, link_column(index + 1), string=column)

        data["date"] = data["date"].astype(str)
        data["sex__m__impressions_rate"] = data["sex__m__impressions_rate"] * 100
        data["sex__m__clicks_rate"] = data["sex__m__clicks_rate"] * 100
        data["sex__f__impressions_rate"] = data["sex__f__impressions_rate"] * 100
        data["sex__f__clicks_rate"] = data["sex__f__clicks_rate"] * 100
        data["age__12_18__impressions_rate"] = (
            data["age__12_18__impressions_rate"] * 100
        )
        data["age__12_18__clicks_rate"] = data["age__12_18__clicks_rate"] * 100
        data["age__18_21__impressions_rate"] = (
            data["age__18_21__impressions_rate"] * 100
        )
        data["age__18_21__clicks_rate"] = data["age__18_21__clicks_rate"] * 100
        data["age__21_24__impressions_rate"] = (
            data["age__21_24__impressions_rate"] * 100
        )
        data["age__21_24__clicks_rate"] = data["age__21_24__clicks_rate"] * 100
        data["age__24_27__impressions_rate"] = (
            data["age__24_27__impressions_rate"] * 100
        )
        data["age__24_27__clicks_rate"] = data["age__24_27__clicks_rate"] * 100
        data["age__27_30__impressions_rate"] = (
            data["age__27_30__impressions_rate"] * 100
        )
        data["age__27_30__clicks_rate"] = data["age__27_30__clicks_rate"] * 100
        data["age__30_35__impressions_rate"] = (
            data["age__30_35__impressions_rate"] * 100
        )
        data["age__30_35__clicks_rate"] = data["age__30_35__clicks_rate"] * 100
        data["age__35_45__impressions_rate"] = (
            data["age__35_45__impressions_rate"] * 100
        )
        data["age__35_45__clicks_rate"] = data["age__35_45__clicks_rate"] * 100
        data["age__45_100__impressions_rate"] = (
            data["age__45_100__impressions_rate"] * 100
        )
        data["age__45_100__clicks_rate"] = data["age__45_100__clicks_rate"] * 100
        worksheet.write_row(0, 0, data.columns)
        for row, stat in enumerate(data.iterrows()):
            worksheet.write_row(row + 1, 0, stat[1].values)
        worksheet.autofilter("A1:AD1")

    def create_enum(self, workbook: Workbook, enum_data: Enum, title: str):
        worksheet = workbook.add_worksheet(title)
        worksheet.write_row(0, 0, ["Идентификатор", "Описание"])
        for row, data in enumerate(enum_data):
            worksheet.write_row(row + 1, 0, [data.value, data.title])
        worksheet.autofilter("A1:B1")

    def create_enums(self, workbook: Workbook):
        items = [
            (vk_data.AdFormatEnum, "ad_format"),
            (vk_data.AdCostTypeEnum, "cost_type"),
            (vk_data.AdGoalTypeEnum, "goal_type"),
            (vk_data.AdPlatformEnum, "ad_platform"),
            (vk_data.AdPublisherPlatformsEnum, "publisher_platforms"),
            (vk_data.AdAutobiddingEnum, "autobidding"),
            (vk_data.AdStatusEnum, "status"),
            (vk_data.AdApprovedEnum, "approved"),
            (vk_data.SexEnum, "targeting__sex"),
            (vk_data.BirthdayEnum, "targeting__birthday"),
            (vk_data.FamilyStatusEnum, "targeting__statuses"),
        ]
        for item in items:
            self.create_enum(workbook, *item)

    def create_categories(self, workbook: Workbook, categories: Dict[int, str]):
        worksheet = workbook.add_worksheet("categories")
        worksheet.write_row(0, 0, ["Идентификатор", "Описание"])
        for index, category in enumerate(categories.items()):
            worksheet.write_row(index + 1, 0, category)
        worksheet.autofilter("A1:B1")

    def create_countries(self, workbook: Workbook, countries: Dict[int, str]):
        worksheet = workbook.add_worksheet("targeting__country")
        worksheet.write_row(0, 0, ["Идентификатор", "Описание"])
        for row, country in enumerate(countries.items()):
            worksheet.write_row(row + 1, 0, country)
        worksheet.autofilter("A1:B1")

    def create_datatype(self, workbook: Workbook):
        worksheet = workbook.add_worksheet("Datatype")
        for row, item in enumerate(vk_data.DatatypeEnum.table_data()):
            worksheet.write_row(row, 0, item)
        worksheet.autofilter("A1:B1")

    def create_description_ads(self, workbook: Workbook):
        datatype_indexes = vk_data.DatatypeEnum.table_indexes()

        def link_datatype(value: vk_data.DatatypeEnum) -> Tuple[str, str]:
            return (
                f"internal:'Datatype'!A{datatype_indexes.get(value.name)}",
                value.value,
            )

        columns = [
            [
                "id",
                link_datatype(vk_data.DatatypeEnum.PositiveInteger),
                "Идентификатор объявления",
            ],
            [
                "title",
                link_datatype(vk_data.DatatypeEnum.String),
                "Заголовок объявления",
            ],
            [
                "text",
                link_datatype(vk_data.DatatypeEnum.String),
                "Текст объявления",
            ],
            [
                "image",
                link_datatype(vk_data.DatatypeEnum.URL),
                "Изображение объявления",
            ],
            [
                "target_url",
                link_datatype(vk_data.DatatypeEnum.URL),
                "Целевая ссылка",
            ],
            [
                "ad_format",
                link_datatype(vk_data.DatatypeEnum.Anchor),
                "Формат объявления",
            ],
            [
                "cost_type",
                link_datatype(vk_data.DatatypeEnum.Anchor),
                "Тип оплаты",
            ],
            [
                "cpc",
                link_datatype(vk_data.DatatypeEnum.NonNegativeFloat),
                "Цена за переход (в рублях)",
            ],
            [
                "cpm",
                link_datatype(vk_data.DatatypeEnum.NonNegativeFloat),
                "Цена за 1000 показов (в рублях)",
            ],
            [
                "ocpm",
                link_datatype(vk_data.DatatypeEnum.NonNegativeFloat),
                "Цена за действие для oCPM (в рублях)",
            ],
            [
                "goal_type",
                link_datatype(vk_data.DatatypeEnum.Anchor),
                "Тип цели",
            ],
            [
                "ad_platform",
                link_datatype(vk_data.DatatypeEnum.Anchor),
                "Рекламные площадки, на которых будет показываться объявление",
            ],
            [
                "publisher_platforms",
                link_datatype(vk_data.DatatypeEnum.Anchor),
                "На каких площадках показывается объявление",
            ],
            [
                "all_limit",
                link_datatype(vk_data.DatatypeEnum.NonNegativeInteger),
                "Общий лимит объявления (в рублях)",
            ],
            [
                "day_limit",
                link_datatype(vk_data.DatatypeEnum.NonNegativeInteger),
                "Дневной лимит объявления (в рублях)",
            ],
            [
                "autobidding",
                link_datatype(vk_data.DatatypeEnum.Anchor),
                "Автоматическое управление ценой",
            ],
            [
                "autobidding_max_cost",
                link_datatype(vk_data.DatatypeEnum.NonNegativeInteger),
                "Максимальное ограничение автоматической ставки (в рублях)",
            ],
            [
                "category1_id",
                link_datatype(vk_data.DatatypeEnum.Anchor),
                "Тематика или подраздел тематики объявления",
            ],
            [
                "category2_id",
                link_datatype(vk_data.DatatypeEnum.Anchor),
                "Тематика или подраздел тематики объявления. Дополнительная тематика",
            ],
            [
                "status",
                link_datatype(vk_data.DatatypeEnum.Anchor),
                "Статус объявления",
            ],
            [
                "approved",
                link_datatype(vk_data.DatatypeEnum.Anchor),
                "Статус модерации объявления",
            ],
            [
                "targeting__sex",
                link_datatype(vk_data.DatatypeEnum.Anchor),
                "Пол",
            ],
            [
                "targeting__age_from",
                link_datatype(vk_data.DatatypeEnum.NonNegativeInteger),
                "Нижняя граница возраста (0 — не задано)",
            ],
            [
                "targeting__age_to",
                link_datatype(vk_data.DatatypeEnum.NonNegativeInteger),
                "Верхняя граница возраста (0 — не задано)",
            ],
            [
                "targeting__birthday",
                link_datatype(vk_data.DatatypeEnum.PositiveInteger),
                "День рождения. Задаётся в виде суммы флагов",
            ],
            [
                "targeting__country",
                link_datatype(vk_data.DatatypeEnum.Anchor),
                "Страна (0 — не задано)",
            ],
            [
                "targeting__cities",
                link_datatype(vk_data.DatatypeEnum.AnchorList),
                "Список городов и регионов",
            ],
            [
                "targeting__cities_not",
                link_datatype(vk_data.DatatypeEnum.AnchorList),
                "Список городов и регионов, которые следует исключить из таргетинга",
            ],
            [
                "targeting__statuses",
                link_datatype(vk_data.DatatypeEnum.AnchorList),
                "Список семейных положений",
            ],
            [
                "targeting__groups",
                link_datatype(vk_data.DatatypeEnum.AnchorList),
                "Список сообществ",
            ],
            [
                "targeting__groups_not",
                link_datatype(vk_data.DatatypeEnum.AnchorList),
                "Список сообществ, которые следует исключить из таргетинга",
            ],
            [
                "targeting__apps",
                link_datatype(vk_data.DatatypeEnum.AnchorList),
                "Список приложений",
            ],
            [
                "targeting__apps_not",
                link_datatype(vk_data.DatatypeEnum.AnchorList),
                "Список приложений, которые следует исключить из таргетинга",
            ],
            [
                "targeting__districts",
                link_datatype(vk_data.DatatypeEnum.AnchorList),
                "Список районов",
            ],
            [
                "targeting__stations",
                link_datatype(vk_data.DatatypeEnum.AnchorList),
                "Список станций метро",
            ],
            [
                "targeting__streets",
                link_datatype(vk_data.DatatypeEnum.AnchorList),
                "Список улиц",
            ],
            [
                "targeting__schools",
                link_datatype(vk_data.DatatypeEnum.AnchorList),
                "Список учебных заведений",
            ],
            [
                "targeting__positions",
                link_datatype(vk_data.DatatypeEnum.AnchorList),
                "Список должностей",
            ],
            [
                "targeting__religions",
                link_datatype(vk_data.DatatypeEnum.AnchorList),
                "Список религиозных взглядов",
            ],
            [
                "targeting__interest_categories",
                link_datatype(vk_data.DatatypeEnum.AnchorList),
                "Список категорий интересов",
            ],
            [
                "targeting__interests",
                link_datatype(vk_data.DatatypeEnum.AnchorList),
                "Список интересов",
            ],
            [
                "targeting__user_devices",
                link_datatype(vk_data.DatatypeEnum.AnchorList),
                "Список устройств",
            ],
            [
                "targeting__user_os",
                link_datatype(vk_data.DatatypeEnum.AnchorList),
                "Список операционных систем",
            ],
            [
                "targeting__user_browsers",
                link_datatype(vk_data.DatatypeEnum.AnchorList),
                "Список интернет-браузеров",
            ],
            [
                "targeting__retargeting_groups",
                link_datatype(vk_data.DatatypeEnum.AnchorList),
                "Список групп ретаргетинга",
            ],
            [
                "targeting__retargeting_groups_not",
                link_datatype(vk_data.DatatypeEnum.AnchorList),
                "Список групп ретаргетинга, которые следует исключить из таргетинга",
            ],
            [
                "targeting__count",
                link_datatype(vk_data.DatatypeEnum.NonNegativeInteger),
                "Размер целевой аудитории на момент сохранения объявления",
            ],
        ]

        worksheet = workbook.add_worksheet('Description "Ads"')
        worksheet.write_row(0, 0, ["Название колонки", "Тип данных", "Описание"])
        for row, item in enumerate(columns):
            worksheet.write_row(
                row + 1,
                0,
                list(
                    map(
                        lambda cell: str(cell) if isinstance(cell, tuple) else cell,
                        item,
                    )
                ),
            )
            worksheet.write_url(row + 1, 1, item[1][0], string=item[1][1])
        worksheet.autofilter("A1:C1")

    def create_description_statistics(self, workbook: Workbook):
        datatype_indexes = vk_data.DatatypeEnum.table_indexes()

        def link_datatype(value: vk_data.DatatypeEnum) -> Tuple[str, str]:
            return (
                f"internal:'Datatype'!A{datatype_indexes.get(value.name)}",
                value.value,
            )

        columns = [
            [
                "id",
                link_datatype(vk_data.DatatypeEnum.PositiveInteger),
                "Идентификатор объявления",
            ],
            [
                "date",
                link_datatype(vk_data.DatatypeEnum.Date),
                "Дата",
            ],
            [
                "sex__m__impressions_rate",
                link_datatype(vk_data.DatatypeEnum.Rate),
                "Доля мужчин, просмотревших объявление",
            ],
            [
                "sex__m__clicks_rate",
                link_datatype(vk_data.DatatypeEnum.Rate),
                "Доля мужчин, кликнувших по объявлению",
            ],
            [
                "sex__f__impressions_rate",
                link_datatype(vk_data.DatatypeEnum.Rate),
                "Доля женщин, просмотревших объявление",
            ],
            [
                "sex__f__clicks_rate",
                link_datatype(vk_data.DatatypeEnum.Rate),
                "Доля женщин, кликнувших по объявлению",
            ],
            [
                "age__12_18__impressions_rate",
                link_datatype(vk_data.DatatypeEnum.Rate),
                "Доля возрастной группы от 12 до 18 лет, просмотревших объявление",
            ],
            [
                "age__12_18__clicks_rate",
                link_datatype(vk_data.DatatypeEnum.Rate),
                "Доля возрастной группы от 12 до 18 лет, кликнувших по объявлению",
            ],
            [
                "age__18_21__impressions_rate",
                link_datatype(vk_data.DatatypeEnum.Rate),
                "Доля возрастной группы от 18 до 21 года, просмотревших объявление",
            ],
            [
                "age__18_21__clicks_rate",
                link_datatype(vk_data.DatatypeEnum.Rate),
                "Доля возрастной группы от 18 до 21 года, кликнувших по объявлению",
            ],
            [
                "age__21_24__impressions_rate",
                link_datatype(vk_data.DatatypeEnum.Rate),
                "Доля возрастной группы от 21 до 24 лет, просмотревших объявление",
            ],
            [
                "age__21_24__clicks_rate",
                link_datatype(vk_data.DatatypeEnum.Rate),
                "Доля возрастной группы от 21 до 24 лет, кликнувших по объявлению",
            ],
            [
                "age__24_27__impressions_rate",
                link_datatype(vk_data.DatatypeEnum.Rate),
                "Доля возрастной группы от 24 до 27 лет, просмотревших объявление",
            ],
            [
                "age__24_27__clicks_rate",
                link_datatype(vk_data.DatatypeEnum.Rate),
                "Доля возрастной группы от 24 до 27 лет, кликнувших по объявлению",
            ],
            [
                "age__27_30__impressions_rate",
                link_datatype(vk_data.DatatypeEnum.Rate),
                "Доля возрастной группы от 27 до 30 лет, просмотревших объявление",
            ],
            [
                "age__27_30__clicks_rate",
                link_datatype(vk_data.DatatypeEnum.Rate),
                "Доля возрастной группы от 27 до 30 лет, кликнувших по объявлению",
            ],
            [
                "age__30_35__impressions_rate",
                link_datatype(vk_data.DatatypeEnum.Rate),
                "Доля возрастной группы от 30 до 35 лет, просмотревших объявление",
            ],
            [
                "age__30_35__clicks_rate",
                link_datatype(vk_data.DatatypeEnum.Rate),
                "Доля возрастной группы от 30 до 35 лет, кликнувших по объявлению",
            ],
            [
                "age__35_45__impressions_rate",
                link_datatype(vk_data.DatatypeEnum.Rate),
                "Доля возрастной группы от 35 до 45 лет, просмотревших объявление",
            ],
            [
                "age__35_45__clicks_rate",
                link_datatype(vk_data.DatatypeEnum.Rate),
                "Доля возрастной группы от 35 до 45 лет, кликнувших по объявлению",
            ],
            [
                "age__45_100__impressions_rate",
                link_datatype(vk_data.DatatypeEnum.Rate),
                "Доля возрастной группы от 45 до 100 лет, просмотревших объявление",
            ],
            [
                "age__45_100__clicks_rate",
                link_datatype(vk_data.DatatypeEnum.Rate),
                "Доля возрастной группы от 45 до 100 лет, кликнувших по объявлению",
            ],
            [
                "spent",
                link_datatype(vk_data.DatatypeEnum.NonNegativeFloat),
                "Потраченные средства",
            ],
            [
                "impressions",
                link_datatype(vk_data.DatatypeEnum.NonNegativeInteger),
                "Просмотры",
            ],
            [
                "clicks",
                link_datatype(vk_data.DatatypeEnum.NonNegativeInteger),
                "Клики",
            ],
            [
                "ctr",
                link_datatype(vk_data.DatatypeEnum.Rate),
                "CTR",
            ],
            [
                "effective_cost_per_click",
                link_datatype(vk_data.DatatypeEnum.NonNegativeFloat),
                "eCPC",
            ],
            [
                "effective_cost_per_mille",
                link_datatype(vk_data.DatatypeEnum.NonNegativeFloat),
                "eCPM",
            ],
            [
                "effective_cpf",
                link_datatype(vk_data.DatatypeEnum.NonNegativeFloat),
                "eCPF",
            ],
            [
                "effective_cost_per_message",
                link_datatype(vk_data.DatatypeEnum.NonNegativeFloat),
                "Стоимость сообщения",
            ],
        ]

        worksheet = workbook.add_worksheet('Description "Statistics"')
        worksheet.write_row(0, 0, ["Название колонки", "Тип данных", "Описание"])
        for row, item in enumerate(columns):
            worksheet.write_row(
                row + 1,
                0,
                list(
                    map(
                        lambda cell: str(cell) if isinstance(cell, tuple) else cell,
                        item,
                    )
                ),
            )
            worksheet.write_url(row + 1, 1, item[1][0], string=item[1][1])
        worksheet.autofilter("A1:C1")

    def get(self):
        target = tempfile.NamedTemporaryFile(suffix=".xlsx")
        workbook = Workbook(target.name)
        categories = dict(
            OrderedDict(
                sorted(
                    dict(
                        map(
                            lambda item: (item.id, item.name),
                            vk_reader("ads.getSuggestions.interest_categories_v2"),
                        )
                    ).items()
                )
            )
        )
        countries = dict(
            OrderedDict(
                sorted(
                    dict(
                        map(
                            lambda item: (item.id, item.name),
                            vk_reader("ads.getSuggestions.countries"),
                        )
                    ).items()
                )
            )
        )
        self.create_ads(workbook, categories, countries)
        self.create_statistics(workbook)
        self.create_description_ads(workbook)
        self.create_description_statistics(workbook)
        self.create_datatype(workbook)
        self.create_enums(workbook)
        self.create_categories(workbook, categories)
        self.create_countries(workbook, countries)
        workbook.close()
        return send_file(
            workbook.filename,
            download_name=f'analytic-nu-vk-ads-{datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%f")}.xlsx',
            as_attachment=True,
        )


class VKXlsxLeadsView(MethodView):
    @decorators.auth
    def dispatch_request(self, *args, **kwargs):
        return super().dispatch_request(*args, **kwargs)

    def create_leads(self, workbook: Workbook):
        leads = pickle_loader.leads
        leads = leads.drop(
            [
                "status_amo",
                "payment_amount",
                "date_request",
                "date_payment",
                "date_status_change",
                "opener",
                "closer",
                "current_lead_amo",
                "main_lead_amo",
                "is_double",
                "is_processed",
                "amo_marker",
                "updated_at",
                "channel_expense2",
                "id",
                "email",
                "phone",
            ],
            axis=1,
        )
        leads = leads[
            (
                leads.utm_source.str.contains("vk")
                | leads.utm_source.str.contains("VK")
                | leads.utm_source.str.contains("Vk")
            )
            & ~leads.utm_source.str.contains("kladovka")
        ]
        leads["created_at"] = leads["created_at"].astype(str)
        leads["turnover_on_lead"] = leads["turnover_on_lead"].astype(float)
        leads = leads.reset_index(drop=True)
        worksheet = workbook.add_worksheet("Leads")

        def link_column(index: int) -> str:
            return f"internal:'Description \"Leads\"'!A{index + 1}"

        for index, column in enumerate(leads.columns):
            worksheet.write_url(0, index, link_column(index + 1), string=column)

        for row, lead in leads.iterrows():
            worksheet.write_row(row + 1, 0, lead.values)
        worksheet.autofilter("A1:R1")

    def create_description(self, workbook: Workbook):
        datatype_indexes = vk_data.DatatypeEnum.table_indexes()

        def link_datatype(value: vk_data.DatatypeEnum) -> Tuple[str, str]:
            return (
                f"internal:'Datatype'!A{datatype_indexes.get(value.name)}",
                value.value,
            )

        columns = [
            [
                "traffic_channel",
                link_datatype(vk_data.DatatypeEnum.URL),
                "Целевая ссылка",
            ],
            [
                "quiz_answers1",
                link_datatype(vk_data.DatatypeEnum.String),
                "Ответ на вопрос 1 квиза",
            ],
            [
                "quiz_answers2",
                link_datatype(vk_data.DatatypeEnum.String),
                "Ответ на вопрос 2 квиза",
            ],
            [
                "quiz_answers3",
                link_datatype(vk_data.DatatypeEnum.String),
                "Ответ на вопрос 3 квиза",
            ],
            [
                "quiz_answers4",
                link_datatype(vk_data.DatatypeEnum.String),
                "Ответ на вопрос 4 квиза",
            ],
            [
                "quiz_answers5",
                link_datatype(vk_data.DatatypeEnum.String),
                "Ответ на вопрос 5 квиза",
            ],
            [
                "quiz_answers6",
                link_datatype(vk_data.DatatypeEnum.String),
                "Ответ на вопрос 6 квиза",
            ],
            [
                "turnover_on_lead",
                link_datatype(vk_data.DatatypeEnum.NonNegativeFloat),
                "Оборот за лид (в рублях)",
            ],
            [
                "trafficologist",
                link_datatype(vk_data.DatatypeEnum.String),
                "Название трафиколога",
            ],
            [
                "account",
                link_datatype(vk_data.DatatypeEnum.String),
                "Название аккаунта",
            ],
            [
                "target_class",
                link_datatype(vk_data.DatatypeEnum.PositiveInteger),
                f'Количество попаданий лидов по вопросам квиза в целевую аудиторию [{", ".join(pickle_loader.target_audience)}]',
            ],
            [
                "channel_expense",
                link_datatype(vk_data.DatatypeEnum.NonNegativeFloat),
                "Расход (в рублях)",
            ],
            [
                "created_at",
                link_datatype(vk_data.DatatypeEnum.Datetime),
                "Дата создания",
            ],
            [
                "utm_source",
                link_datatype(vk_data.DatatypeEnum.String),
                "UTM source",
            ],
            [
                "utm_medium",
                link_datatype(vk_data.DatatypeEnum.String),
                "UTM medium",
            ],
            [
                "utm_campaign",
                link_datatype(vk_data.DatatypeEnum.String),
                "UTM campaign",
            ],
            [
                "utm_content",
                link_datatype(vk_data.DatatypeEnum.String),
                "UTM content",
            ],
            [
                "utm_term",
                link_datatype(vk_data.DatatypeEnum.String),
                "UTM term",
            ],
        ]
        worksheet = workbook.add_worksheet('Description "Leads"')
        worksheet.write_row(0, 0, ["Название колонки", "Тип данных", "Описание"])
        for row, item in enumerate(columns):
            worksheet.write_row(
                row + 1,
                0,
                list(
                    map(
                        lambda cell: str(cell) if isinstance(cell, tuple) else cell,
                        item,
                    )
                ),
            )
            worksheet.write_url(row + 1, 1, item[1][0], string=item[1][1])
        worksheet.autofilter("A1:C1")

    def create_datatype(self, workbook: Workbook):
        worksheet = workbook.add_worksheet("Datatype")
        for row, item in enumerate(vk_data.DatatypeEnum.table_data()):
            worksheet.write_row(row, 0, item)
        worksheet.autofilter("A1:B1")

    def get(self):
        target = tempfile.NamedTemporaryFile(suffix=".xlsx")
        workbook = Workbook(target.name)
        self.create_leads(workbook)
        self.create_description(workbook)
        self.create_datatype(workbook)
        workbook.close()
        return send_file(
            workbook.filename,
            download_name=f'analytic-nu-leads-{datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%f")}.xlsx',
            as_attachment=True,
        )


class VKXlsxView(TemplateView):
    template_name = "vk/xlsx.html"
    title = "Скачать таблицу XLSX"

    @property
    def ads(self) -> pandas.DataFrame:
        posts = dict(map(lambda post: (post.ad_id, post.dict()), vk_reader("wall.get")))
        rows = []
        ads_columns = []
        posts_columns = []
        for item in vk_reader("ads.getAds"):
            post = posts.get(item.id, {})
            ads_columns = item.dict().keys()
            posts_columns = post.keys()
            rows.append(list(item.dict().values()) + list(post.values()))
        return pandas.DataFrame(rows, columns=(list(ads_columns) + list(posts_columns)))

    @property
    def leads(self) -> pandas.DataFrame:
        leads = pickle_loader.leads
        leads = leads.drop(
            [
                "id",
                "email",
                "phone",
                "quiz_answers1",
                "quiz_answers2",
                "quiz_answers3",
                "quiz_answers4",
                "quiz_answers5",
                "quiz_answers6",
                "status_amo",
                "payment_amount",
                "turnover_on_lead",
                "date_request",
                "date_payment",
                "date_status_change",
                "opener",
                "closer",
                "current_lead_amo",
                "main_lead_amo",
                "is_double",
                "is_processed",
                "trafficologist",
                "account",
                "target_class",
                "channel_expense",
                "amo_marker",
                "created_at",
                "updated_at",
                "channel_expense2",
            ],
            axis=1,
        )
        leads = leads[
            leads.utm_source.str.contains("vk") | leads.utm_source.str.contains("VK")
        ]
        # leads["date_request"] = leads["date_request"].astype(str)
        # leads["date_payment"] = leads["date_payment"].astype(str)
        # leads["date_status_change"] = leads["date_status_change"].astype(str)
        # leads["created_at"] = leads["created_at"].astype(str)
        # leads["updated_at"] = leads["updated_at"].astype(str)
        leads = leads.reset_index(drop=True)
        return leads

    def get(self):
        leads = self.leads
        ads = self.ads
        self.context("leads", leads)
        return super().get()


class ApiVKCreateAdDependesFieldsView(APIView):
    def get(self):
        data = dict(parse_qsl(request.query_string.decode("utf-8")))
        account_id = int(data.get("account_id", 0))
        accounts = [("", "", False)] + list(
            map(
                lambda item: (
                    item.account_id,
                    item.account_name,
                    item.account_id == account_id,
                ),
                vk_reader("ads.getAccounts"),
            )
        )
        campaigns = []
        if list(filter(lambda item: item[2], accounts)):
            campaign_id = int(data.get("campaign_id", 0))
            campaigns = [("", "", False)] + list(
                map(
                    lambda item: (
                        item.id,
                        item.name,
                        item.id == campaign_id,
                    ),
                    list(
                        filter(
                            lambda campaign: campaign.account_id == account_id,
                            vk_reader("ads.getCampaigns"),
                        )
                    ),
                )
            )
        cost_type = int(data.get("cost_type", -1))
        cost_types = [("", "", False)] + list(
            map(
                lambda item: (item.value, item.title, item.value == cost_type),
                vk_data.CampaignCostTypeEnum,
            )
        )
        ad_format = int(data.get("ad_format", -1))
        ad_formats = [("", "", False)] + list(
            map(
                lambda item: (item.value, item.title, item.value == ad_format),
                vk_data.CampaignAdFormatEnum,
            )
        )
        goal_type = int(data.get("goal_type", -1))
        goal_types = [("", "", False)] + list(
            map(
                lambda item: (item.value, item.title, item.value == goal_type),
                vk_data.AdGoalTypeEnum,
            )
        )
        self.data = {
            "accounts": accounts,
            "campaigns": campaigns,
            "cost_type": cost_types,
            "ad_format": ad_formats,
            "goal_type": goal_types,
        }
        return super().get()


class ChannelsView(TemplateView):
    template_name = "channels/index.html"
    title = "Каналы"

    def _filter_date_from(
        self, date: datetime.datetime, leads: pandas.DataFrame
    ) -> pandas.DataFrame:
        if date:
            leads = leads[
                leads.created_at >= datetime.datetime.strptime(date, "%Y-%m-%d")
            ]
        return leads

    def _filter_date_to(
        self, date: datetime.datetime, leads: pandas.DataFrame
    ) -> pandas.DataFrame:
        if date:
            leads = leads[
                leads.created_at
                < (
                    datetime.datetime.strptime(date, "%Y-%m-%d")
                    + datetime.timedelta(days=1)
                )
            ]
        return leads

    def get_choices(self, leads: pandas.DataFrame) -> Dict[str, List[str]]:
        date_from = request.args.get("date_from") or None
        date_to = request.args.get("date_to") or None

        leads = self._filter_date_from(date_from, leads)
        leads = self._filter_date_to(date_to, leads)

        return {
            "accounts": [""] + list(leads["account"].unique()),
        }

    def get_filters(self, choices: Dict[str, List[str]]) -> Dict[str, str]:
        date_from = request.args.get("date_from") or None
        date_to = request.args.get("date_to") or None
        account = request.args.get("account") or None

        if account not in choices.get("accounts"):
            account = None

        return {
            "date_from": date_from,
            "date_to": date_to,
            "account": account,
        }

    def get_filtered_data(
        self, leads: pandas.DataFrame, filters: Dict[str, str]
    ) -> pandas.DataFrame:
        leads = self._filter_date_from(filters.get("date_from"), leads)
        leads = self._filter_date_to(filters.get("date_to"), leads)

        if filters.get("account"):
            leads = leads[leads["account"] == filters.get("account")].groupby(
                "utm_campaign"
            )
        else:
            leads = leads.groupby("account")

        return leads

    def get(self):
        leads = pickle_loader.leads.sort_values(["created_at"])

        choices = self.get_choices(leads)
        filters = self.get_filters(choices)
        data = self.get_filtered_data(leads, filters)

        output = list(
            map(
                lambda group: {
                    "name": group[0],
                    "dates": list(
                        map(
                            lambda item: int(item / 1000000),
                            group[1]["created_at"].unique().tolist(),
                        )
                    ),
                },
                data,
            )
        )

        if filters.get("date_from"):
            date_from = int(
                datetime.datetime.strptime(filters.get("date_from"), "%Y-%m-%d")
                .replace(tzinfo=datetime.timezone.utc)
                .timestamp()
                * 1000
            )
        else:
            date_from = min(list(map(lambda item: min(item.get("dates")), output)))

        if filters.get("date_to"):
            date_to = int(
                datetime.datetime.strptime(filters.get("date_to"), "%Y-%m-%d")
                .replace(tzinfo=datetime.timezone.utc)
                .timestamp()
                * 1000
            )
        else:
            date_to = max(list(map(lambda item: max(item.get("dates")), output)))

        self.context("choices", choices)
        self.context("filters", filters)
        self.context("date_range", [date_from, date_to])
        self.context("data", output)

        return super().get()


class ApiVKLeadsView(APIView):
    data: Dict[str, Any] = {}

    @property
    def leads(self) -> pandas.DataFrame:
        leads = pickle_loader.leads
        leads = leads.drop(
            [
                "amo_marker",
                "channel_expense2",
                "closer",
                "current_lead_amo",
                "date_payment",
                "date_request",
                "date_status_change",
                "email",
                "id",
                "is_double",
                "is_processed",
                "main_lead_amo",
                "opener",
                "payment_amount",
                "phone",
                "status_amo",
                "updated_at",
            ],
            axis=1,
        )
        leads = leads[
            leads.utm_source.str.contains("vk")
            | leads.utm_source.str.contains("VK")
            | leads.utm_source.str.contains("Vk")
        ]
        leads["created_at"] = leads["created_at"].astype(str)
        leads = leads.reset_index(drop=True)
        return leads

    def get(self):
        leads = self.leads
        columns = list(leads.columns)
        leads_output = []
        for index, item in leads.iterrows():
            leads_output.append(dict(zip(columns, list(item.values))))
        self.data.update({"leads": leads_output})
        return super().get()


class ApiVKAdsView(APIView):
    data: Dict[str, Any]

    def __init__(self):
        self.data = {}
        super().__init__()

    @property
    def add_format(self) -> Dict[str, str]:
        return dict(
            map(lambda item: (str(item.value), item.title), vk_data.AdFormatEnum)
        )

    @property
    def ad_platform(self) -> Dict[str, str]:
        return dict(
            map(lambda item: (str(item.value), item.title), vk_data.AdPlatformEnum)
        )

    @property
    def approved(self) -> Dict[str, str]:
        return dict(
            map(lambda item: (str(item.value), item.title), vk_data.AdApprovedEnum)
        )

    @property
    def autobidding(self) -> Dict[str, str]:
        return dict(
            map(lambda item: (str(item.value), item.title), vk_data.AdAutobiddingEnum)
        )

    @property
    def categories(self) -> Dict[str, str]:
        return dict(
            map(
                lambda item: (str(item.id), item.name),
                vk_reader("ads.getSuggestions.interest_categories_v2"),
            )
        )

    @property
    def cost_type(self) -> Dict[str, str]:
        return dict(
            map(lambda item: (str(item.value), item.title), vk_data.AdCostTypeEnum)
        )

    @property
    def goal_type(self) -> Dict[str, str]:
        return dict(
            map(lambda item: (str(item.value), item.title), vk_data.AdGoalTypeEnum)
        )

    @property
    def publisher_platforms(self) -> Dict[str, str]:
        return dict(
            map(
                lambda item: (str(item.value), item.title),
                vk_data.AdPublisherPlatformsEnum,
            )
        )

    @property
    def status(self) -> Dict[str, str]:
        return dict(
            map(lambda item: (str(item.value), item.title), vk_data.AdStatusEnum)
        )

    @property
    def ads(self) -> pandas.DataFrame:
        posts = dict(map(lambda post: (post.ad_id, post.dict()), vk_reader("wall.get")))
        rows = []
        columns = []
        for item in vk_reader("ads.getAds"):
            post = posts.get(item.id, {})
            post.pop("ad_id", None)
            post.pop("attachments", None)
            post.pop("date", None)
            post.pop("from_id", None)
            post.pop("id", None)
            post.pop("owner_id", None)

            item = item.dict()
            item.pop("account_id", None)
            item.pop("ad_platform_no_ad_network", None)
            item.pop("ad_platform_no_wall", None)
            item.pop("campaign_id", None)
            item.pop("name", None)
            item.pop("client_id", None)
            item.pop("disclaimer_medical", None)
            item.pop("disclaimer_specialist", None)
            item.pop("disclaimer_supplements", None)
            item.pop("impressions_limit", None)
            item.pop("impressions_limited", None)
            item.pop("events_retargeting_groups", None)
            item.pop("video", None)
            item.pop("weekly_schedule_hours", None)
            item.pop("weekly_schedule_use_holidays", None)

            item.update(
                {
                    "ad_format": str(item.get("ad_format").value)
                    if item.get("ad_format")
                    else None,
                    "ad_platform": str(item.get("ad_platform").value)
                    if item.get("ad_platform")
                    else None,
                    "all_limit": int(item.get("all_limit", 0)),
                    "approved": str(item.get("approved").value)
                    if item.get("approved")
                    else None,
                    "autobidding": str(
                        (item.get("autobidding") or vk_data.AdAutobiddingEnum(0)).value
                    ),
                    "autobidding_max_cost": int(item.get("autobidding_max_cost") / 100)
                    if item.get("autobidding_max_cost")
                    else 0,
                    "category1_id": str(item.get("category1_id"))
                    if item.get("category1_id")
                    else None,
                    "category2_id": str(item.get("category2_id"))
                    if item.get("category2_id")
                    else None,
                    "cost_type": str(item.get("cost_type").value)
                    if item.get("cost_type")
                    else None,
                    "cpc": float(item.get("cpc") / 100) if item.get("cpc") else None,
                    "cpm": float(item.get("cpm") / 100) if item.get("cpm") else None,
                    "ocpm": float(item.get("ocpm") / 100) if item.get("ocpm") else None,
                    "day_limit": int(item.get("day_limit", 0)),
                    "goal_type": str(item.get("goal_type").value)
                    if item.get("goal_type")
                    else None,
                    "id": int(item.get("id")),
                    "publisher_platforms": str(item.get("publisher_platforms").value)
                    if item.get("publisher_platforms")
                    else None,
                    "status": str(item.get("status").value)
                    if item.get("status")
                    else None,
                    **post,
                }
            )
            columns = item.keys()
            rows.append(list(item.values()))
        return pandas.DataFrame(rows, columns=(list(columns)))

    def get(self):
        ads = self.ads
        columns = list(ads.columns)
        leads_output = []
        for index, item in ads.iterrows():
            leads_output.append(dict(zip(columns, list(item.values))))
        self.data.update(
            {
                "ads": leads_output,
                "ad_format": self.add_format,
                "ad_platform": self.ad_platform,
                "approved": self.approved,
                "autobidding": self.autobidding,
                "categories": self.categories,
                "cost_type": self.cost_type,
                "goal_type": self.goal_type,
                "publisher_platforms": self.publisher_platforms,
                "status": self.status,
                "titles": {
                    "ad_format": "Формат объявления",
                    "ad_platform": "Рекламные площадки, на которых будет показываться объявление",
                    "all_limit": "Общий лимит объявления (в рублях, 0 — лимит не задан)",
                    "approved": "Статус модерации объявления",
                    "autobidding": "Автоматическое управление ценой",
                    "autobidding_max_cost": "Максимальное ограничение автоматической ставки (в рублях)",
                    "category1_id": "ID тематики или подраздела тематики объявления",
                    "category2_id": "ID тематики или подраздела тематики объявления (дополнительная тематика)",
                    "cost_type": "Тип оплаты",
                    "cpc": "Цена за переход (в рублях)",
                    "cpm": "Цена за 1000 показов (в рублях)",
                    "day_limit": "Дневной лимит объявления (в рублях, 0 — лимит не задан)",
                    "goal_type": "Тип цели",
                    "id": "Идентификатор объявления",
                    "ocpm": "Цена за действие для oCPM (в рублях)",
                    "publisher_platforms": "На каких площадках показывается объявление",
                    "status": "Статус объявления",
                    "image": "Изображение объявления",
                    "target_url": "Целевая ссылка",
                    "text": "Описание объявления",
                    "title": "Заголовок объявления",
                },
            }
        )
        return super().get()


class StatisticsAccountsByProviderView(APIView):
    def get(self, provider: str):
        if provider not in StatisticsProviderEnum.dict().keys():
            abort(404)

        accounts = []

        if provider == StatisticsProviderEnum.vk.name:
            accounts = sorted(
                list(
                    map(
                        lambda account: {
                            "value": account.account_id,
                            "name": account.account_name,
                        },
                        vk_reader("ads.getAccounts"),
                    )
                ),
                key=lambda account: account.get("name"),
            )

        elif provider == StatisticsProviderEnum.yandex.name:
            accounts = []

        elif provider == StatisticsProviderEnum.tg.name:
            accounts = []

        self.data = {"accounts": accounts}

        return super().get()


class StatisticsCampaignsByAccountView(APIView):
    def get(self, provider: str, account: int):
        if provider not in StatisticsProviderEnum.dict().keys():
            abort(404)

        campaigns = []

        if provider == StatisticsProviderEnum.vk.name:
            campaigns = sorted(
                list(
                    map(
                        lambda campaign: {
                            "value": campaign.id,
                            "name": campaign.name,
                        },
                        list(
                            filter(
                                lambda item: item.account_id == int(account),
                                vk_reader("ads.getCampaigns"),
                            )
                        ),
                    )
                ),
                key=lambda campaign: campaign.get("name"),
            )

        elif provider == StatisticsProviderEnum.yandex.name:
            campaigns = []

        elif provider == StatisticsProviderEnum.tg.name:
            campaigns = []

        self.data = {"campaigns": campaigns}

        return super().get()


class StatisticsGroupsByCampaignView(APIView):
    def get(self, provider: str, campaign: int):
        if provider not in StatisticsProviderEnum.dict().keys():
            abort(404)

        groups = []

        if provider == StatisticsProviderEnum.vk.name:
            groups = []

        elif provider == StatisticsProviderEnum.yandex.name:
            groups = []

        elif provider == StatisticsProviderEnum.tg.name:
            groups = []

        self.data = {"groups": groups}

        return super().get()


class FilteringBaseView(TemplateView):
    filters_class = WeekStatsFiltersEmptyData
    filters: WeekStatsFiltersEmptyData
    extras: Dict[str, Any]

    def get_filters_class(self) -> type:
        return self.filters_class

    def filters_initial(self) -> Dict[str, Any]:
        return {}

    def filters_preprocess(self, **kwargs) -> Dict[str, Any]:
        return kwargs

    def get_filters(self):
        initial = self.filters_initial()
        data = self.filters_preprocess(**initial)
        filters_class = self.get_filters_class()
        self.filters = filters_class(**data)

    def load_dataframe(self, path: Path) -> pandas.DataFrame:
        with open(path, "rb") as file_ref:
            dataframe: pandas.DataFrame = pickle.load(file_ref)
        return dataframe

    def filtering_values(self):
        raise NotImplementedError(
            '%s must implement "filtering_values" method.' % self.__class__
        )

    def get_extras(self):
        self.extras = {}


class ZoomsView(FilteringBaseView):
    template_name = "zooms/index.html"
    title = "Zooms"
    filters_class = ZoomsFiltersData
    filters: ZoomsFiltersData
    values: pandas.DataFrame
    values_path: Path = Path(DATA_FOLDER) / "week" / "managers_zooms.pkl"
    controllable_path: Path = (
        Path(DATA_FOLDER) / "week" / "managers_zooms_controllable.pkl"
    )

    def filters_initial(self) -> Dict[str, Any]:
        return {
            "date_from": datetime.datetime.now().date() - datetime.timedelta(weeks=1),
        }

    def filtering_values(self):
        if self.filters.date_from:
            self.values = self.values[self.values["date"] >= self.filters.date_from]

        if self.filters.date_to:
            self.values = self.values[self.values["date"] <= self.filters.date_to]

        if self.filters.purchase_probability_from is not None:
            self.values = self.values[
                self.values["purchase_probability"]
                >= self.filters.purchase_probability_from
            ]

        if self.filters.purchase_probability_to is not None:
            self.values = self.values[
                self.values["purchase_probability"]
                <= self.filters.purchase_probability_to
            ]

        if self.filters.expected_payment_date_from:
            self.values = self.values[
                self.values["expected_payment_date"]
                >= self.filters.expected_payment_date_from
            ]

        if self.filters.expected_payment_date_to:
            self.values = self.values[
                self.values["expected_payment_date"]
                <= self.filters.expected_payment_date_to
            ]

        if self.filters.on_control is not None:
            self.values = self.values[
                self.values["on_control"] == self.filters.on_control
            ]

        self.values.reset_index(drop=True, inplace=True)

    def get_filters(self):
        initial = self.filters_initial()

        date_from = request.args.get("date_from")
        if date_from is None:
            date_from = initial.get("date_from")
        if isinstance(date_from, str):
            date_from = (
                datetime.date.fromisoformat(date_from) if str(date_from) else None
            )

        date_to = request.args.get("date_to")
        if date_to is None:
            date_to = initial.get("date_to")
        if isinstance(date_to, str):
            date_to = datetime.date.fromisoformat(date_to) if str(date_to) else None

        purchase_probability_from = (
            request.args.get("purchase_probability_from") or None
        )
        purchase_probability_to = request.args.get("purchase_probability_to") or None

        expected_payment_date_from = (
            request.args.get("expected_payment_date_from") or None
        )
        if expected_payment_date_from is None:
            expected_payment_date_from = initial.get("expected_payment_date_from")
        if isinstance(expected_payment_date_from, str):
            expected_payment_date_from = datetime.date.fromisoformat(
                expected_payment_date_from
            )

        expected_payment_date_to = request.args.get("expected_payment_date_to") or None
        if expected_payment_date_to is None:
            expected_payment_date_to = initial.get("expected_payment_date_to")
        if isinstance(expected_payment_date_to, str):
            expected_payment_date_to = datetime.date.fromisoformat(
                expected_payment_date_to
            )

        on_control = request.args.get("on_control")
        if on_control is None:
            on_control = initial.get("on_control", "__all__")
        if on_control == "__all__":
            on_control = None
        if on_control is not None:
            on_control = parse_bool_from_int(on_control)

        group = request.args.get("group")
        if group is None:
            group = initial.get("group", "__all__")
        if group == "__all__":
            group = None

        manager = request.args.get("manager")
        if manager is None:
            manager = initial.get("manager", "__all__")
        if manager == "__all__":
            manager = None

        data = self.filters_preprocess(
            date_from=date_from,
            date_to=date_to,
            group=group,
            manager=manager,
            purchase_probability_from=purchase_probability_from,
            purchase_probability_to=purchase_probability_to,
            expected_payment_date_from=expected_payment_date_from,
            expected_payment_date_to=expected_payment_date_to,
            on_control=on_control,
        )

        filters_class = self.get_filters_class()

        self.filters = filters_class(**data)

    def get_extras_group(self, group: str) -> List[List[str]]:
        group_id = f"{group}_id"
        groups: pandas.DataFrame = (
            self.values[[group_id, group]]
            .drop_duplicates()
            .sort_values(group)
            .reset_index(drop=True)
        )
        if self.filters[group] not in list(groups[group_id]):
            self.filters[group] = None
        if self.filters[group] is not None:
            self.values = self.values[
                self.values[group_id] == self.filters[group]
            ].reset_index(drop=True)
        return groups.values.tolist()

    def get_extras(self):
        cyr_month = [
            "январь",
            "февраль",
            "март",
            "апрель",
            "май",
            "июнь",
            "июль",
            "август",
            "сентябрь",
            "октябрь",
            "ноябрь",
            "декабрь",
        ]
        date = datetime.datetime.now()
        year = date.year
        month = list(
            map(
                lambda item: ("%i-%02i" % (year, item), cyr_month[item - 1]),
                range(1, date.month + 1),
            )
        )
        self.extras = {
            "exclude_columns": [
                "manager_id",
                "group_id",
                "estimate",
                "purchase_probability",
                "potential_order_amount",
                "expected_payment_date",
                "on_control",
            ],
            "groups": self.get_extras_group("group"),
            "managers": self.get_extras_group("manager"),
            "month": month,
        }

    def get(self, is_download=False):
        self.get_filters()
        self.values = self.load_dataframe(self.values_path)
        try:
            self.controllable = self.load_dataframe(self.controllable_path)
        except FileNotFoundError:
            self.controllable = pandas.DataFrame(
                columns=[
                    "manager_id",
                    "lead",
                    "date",
                    "purchase_probability",
                    "potential_order_amount",
                    "expected_payment_date",
                    "on_control",
                ]
            )

        self.values = self.values.merge(
            self.controllable, how="left", on=["manager_id", "lead", "date"]
        )
        self.values.fillna(pandas.NA, inplace=True)
        if len(self.values):
            self.values["estimate"] = (
                self.values.apply(parse_estimate, axis=1).apply(parse_int).fillna("")
            )
        self.values["purchase_probability"] = (
            self.values["purchase_probability"].apply(parse_int).fillna("")
        )
        self.values["potential_order_amount"] = (
            self.values["potential_order_amount"].apply(parse_int).fillna("")
        )
        self.values["expected_payment_date"] = (
            self.values["expected_payment_date"].apply(parse_date).fillna("")
        )
        self.values["on_control"] = (
            self.values["on_control"].apply(parse_bool).fillna("")
        )

        self.filtering_values()
        self.get_extras()

        data = self.values.sort_values(by=["group", "manager", "date"])

        total = pandas.Series(
            {
                "name": "Итого",
                "profit": data["profit"].sum(),
                "lead": len(data),
                "potential_order_amount": data[data["potential_order_amount"] != ""][
                    "potential_order_amount"
                ].sum(),
                "estimate": data[data["estimate"] != ""]["estimate"].sum(),
            }
        )

        data = data[
            [
                "group",
                "manager",
                "date",
                "lead",
                "profit",
                "manager_id",
                "group_id",
                "estimate",
                "purchase_probability",
                "potential_order_amount",
                "expected_payment_date",
                "on_control",
            ]
        ]
        data.rename(
            columns={
                "group": "Группа",
                "manager": "Менеджер",
                "date": "Дата зума",
                "lead": "Лид",
                "profit": "Оплата",
            },
            inplace=True,
        )

        self.context("filters", self.filters)
        self.context("extras", self.extras)
        self.context("total", total)
        self.context("data", data)

        if is_download:
            return data, total
        else:
            return super().get()

    def fill_yes_no(self, value: bool) -> str:
        if value is True:
            return "Да"
        elif value is False:
            return "Нет"
        else:
            return ""

    def post(self, *args, **kwargs):
        data, total = self.get(is_download=True)
        data["Лид"] = data["Лид"].apply(
            lambda item: f"https://neuraluniversity.amocrm.ru/leads/detail/{item}/"
        )
        data["on_control"] = data["on_control"].apply(self.fill_yes_no)
        total.rename(
            {
                "name": "Группа",
                "profit": "Оплата",
                "lead": "Лид",
            },
            inplace=True,
        )
        data = pandas.concat([pandas.DataFrame([total]), data])
        data.rename(
            columns={
                "potential_order_amount": "Потенциальная сумма заказа",
                "estimate": "Оценочный оборот",
                "purchase_probability": "Вероятность покупки",
                "expected_payment_date": "Ожидаемая дата оплаты",
                "on_control": "На контроле",
            },
            inplace=True,
        )
        columns = [
            "Группа",
            "Менеджер",
            "Дата зума",
            "Лид",
            "Оплата",
            "Вероятность покупки",
            "Потенциальная сумма заказа",
            "Ожидаемая дата оплаты",
            "Оценочный оборот",
            "На контроле",
        ]
        data = data.reindex(columns, axis=1)
        data.fillna("", inplace=True)
        data.reset_index(drop=True, inplace=True)
        target = tempfile.NamedTemporaryFile(
            delete=False, prefix="zooms-", suffix=".xlsx"
        )
        with pandas.ExcelWriter(target.name, engine="xlsxwriter") as writer:
            data.to_excel(writer)
        return send_file(
            target.name,
            as_attachment=True,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            download_name="zooms.xlsx",
        )


class WeekStatsBaseCohortsView(FilteringBaseView):
    filters_class = WeekStatsFiltersCohortsData
    filters: WeekStatsFiltersCohortsData

    value_column_name: str = "Значение"

    values: pandas.DataFrame
    counts: pandas.DataFrame

    values_path: Path
    counts_path: Path

    def filters_initial(self) -> Dict[str, Any]:
        return {
            "date": datetime.datetime.now().date() - datetime.timedelta(weeks=10),
        }

    def filters_preprocess(self, **kwargs) -> Dict[str, Any]:
        data = super().filters_preprocess(**kwargs)
        data.update(
            {
                "date": detect_week(data.get("date"))[0],
            }
        )
        return data

    def get_filters(self):
        initial = self.filters_initial()

        date = request.args.get("date")
        if date is None:
            date = initial.get("date")
        if isinstance(date, str):
            date = datetime.date.fromisoformat(date)

        group = request.args.get("group")
        if group is None:
            group = initial.get("group", "__all__")
        if group == "__all__":
            group = None

        manager = request.args.get("manager")
        if manager is None:
            manager = initial.get("manager", "__all__")
        if manager == "__all__":
            manager = None

        channel = request.args.get("channel")
        if channel is None:
            channel = initial.get("channel", "__all__")
        if channel == "__all__":
            channel = None

        accumulative = request.args.get("accumulative")
        if accumulative is None:
            accumulative = initial.get("accumulative", False)

        profit = request.args.get("profit")
        if profit is None:
            profit = initial.get("profit", False)

        data = self.filters_preprocess(
            date=date,
            group=group,
            manager=manager,
            channel=channel,
            accumulative=accumulative,
            profit=profit,
        )

        filters_class = self.get_filters_class()

        self.filters = filters_class(**data)

    def filtering_values(self):
        if self.filters.date:
            self.values = self.values[
                self.values["date"] >= self.filters.date
            ].reset_index(drop=True)
            self.counts = self.counts[
                self.counts["date"] >= self.filters.date
            ].reset_index(drop=True)

    def get_extras_group(self, group: str) -> List[List[str]]:
        group_id = f"{group}_id"
        groups: pandas.DataFrame = (
            self.values[[group_id, group]]
            .drop_duplicates()
            .sort_values(group)
            .reset_index(drop=True)
        )
        if self.filters[group] not in list(groups[group_id]):
            self.filters[group] = None
        if self.filters[group] is not None:
            if (
                group in ["group", "manager", "channel"]
                and group_id in self.counts.columns
            ):
                self.counts = self.counts[
                    self.counts[group_id] == self.filters[group]
                ].reset_index(drop=True)
            self.values = self.values[
                self.values[group_id] == self.filters[group]
            ].reset_index(drop=True)
        return groups.values.tolist()

    def get_extras(self):
        self.extras = {
            "value_column_name": self.value_column_name,
            "groups": self.get_extras_group("group"),
            "managers": self.get_extras_group("manager"),
            "channels": self.get_extras_group("channel"),
        }

    def get_values_week(
        self,
        date_from: datetime.date,
        date_end: datetime.date,
        date_to: datetime.date,
        weeks: int,
    ) -> List[int]:
        values = self.values[
            (self.values["date"] >= date_from) & (self.values["date"] <= date_to)
        ].reset_index(drop=True)

        output = []
        while date_from <= date_end:
            date_to = date_from + datetime.timedelta(days=6)
            output.append(
                values[
                    (values["profit_date"] >= date_from)
                    & (values["profit_date"] <= date_to)
                ]["profit"].sum()
            )
            date_from += datetime.timedelta(weeks=1)

        output += [pandas.NA] * (weeks - len(output))

        return output

    def get(self):
        self.get_filters()

        self.values = self.load_dataframe(self.values_path)
        self.counts = self.load_dataframe(self.counts_path)

        self.values = self.values[self.values["profit_date"] >= self.values["date"]]

        with open(Path(DATA_FOLDER) / "week" / "groups.pkl", "rb") as file_ref:
            groups: pandas.DataFrame = pickle.load(file_ref)
        self.values = self.values.merge(groups, how="left", on=["manager_id"]).rename(
            columns={"group": "group_id"}
        )
        self.values["group"] = self.values["group_id"].apply(
            lambda item: f'Группа "{item}"'
        )
        if "manager_id" in self.counts.columns:
            self.counts = self.counts.merge(
                groups, how="left", on=["manager_id"]
            ).rename(columns={"group": "group_id"})

        with open(Path(DATA_FOLDER) / "week" / "channels.pkl", "rb") as file_ref:
            channels: pandas.DataFrame = pickle.load(file_ref)
        self.values = self.values.merge(channels, how="left", on=["channel_id"])

        self.filtering_values()

        self.get_extras()

        date_from = self.filters.date
        date_end = detect_week(datetime.datetime.now().date())[0] - datetime.timedelta(
            weeks=1
        )
        weeks = ((date_end - date_from) / 7 + datetime.timedelta(days=1)).days

        values_from = [date_from]
        values_to = [date_end + datetime.timedelta(days=6)]
        values_weeks = []
        counts_weeks = []

        while date_from <= date_end:
            date_to = date_from + datetime.timedelta(days=6)
            values_week = self.get_values_week(date_from, date_end, date_to, weeks)
            values_weeks.append(values_week)
            values_from.append(date_from)
            values_to.append(date_to)
            counts_weeks.append(
                self.counts[
                    (self.counts["date"] >= date_from)
                    & (self.counts["date"] <= date_to)
                ]["count"].sum()
            )
            date_from += datetime.timedelta(weeks=1)

        data = pandas.DataFrame(columns=list(range(1, weeks + 1)), data=values_weeks)
        data.insert(0, self.value_column_name, counts_weeks)
        data.insert(1, "Сумма", [pandas.NA] * weeks)
        data = pandas.concat(
            [
                pandas.DataFrame(
                    data=[[pandas.NA] * (weeks + 2)], columns=data.columns
                ),
                data,
            ],
            ignore_index=True,
        )
        data.insert(0, "С даты", values_from)
        data.insert(1, "По дату", values_to)
        total = data.iloc[0]
        data = data.iloc[1:].reset_index(drop=True)

        self.context("filters", self.filters)
        self.context("extras", self.extras)
        self.context("data", data)
        self.context("total", total)

        return super().get()


class WeekStatsExpensesView(WeekStatsBaseCohortsView):
    template_name = "week-stats/expenses/index.html"
    title = 'Когорты "Расход"'
    values_path = Path(DATA_FOLDER) / "week" / "expenses.pkl"
    counts_path = Path(DATA_FOLDER) / "week" / "expenses_count.pkl"
    value_column_name: str = "Расход"


class WeekStatsZoomView(WeekStatsBaseCohortsView):
    template_name = "week-stats/zoom/index.html"
    title = 'Когорты "Zoom"'
    values_path = Path(DATA_FOLDER) / "week" / "zoom.pkl"
    counts_path = Path(DATA_FOLDER) / "week" / "zoom_count.pkl"
    value_column_name: str = "Zoom"


class WeekStatsSpecialOffersView(WeekStatsBaseCohortsView):
    template_name = "week-stats/so/index.html"
    title = 'Когорты "Special Offers"'
    values_path = Path(DATA_FOLDER) / "week" / "so.pkl"
    counts_path = Path(DATA_FOLDER) / "week" / "so_count.pkl"
    value_column_name: str = "SO"


class WeekStatsManagersView(FilteringBaseView):
    template_name = "week-stats/managers/index.html"
    title = "Менеджеры"

    filters_class = WeekStatsFiltersManagersData
    filters: WeekStatsFiltersManagersData

    managers: pandas.DataFrame
    values_zoom: pandas.DataFrame
    counts_zoom: pandas.DataFrame
    values_so: pandas.DataFrame
    counts_so: pandas.DataFrame

    managers_path: Path = Path(DATA_FOLDER) / "week" / "groups.pkl"
    values_zoom_path: Path = Path(DATA_FOLDER) / "week" / "zoom.pkl"
    counts_zoom_path: Path = Path(DATA_FOLDER) / "week" / "zoom_count.pkl"
    values_so_path: Path = Path(DATA_FOLDER) / "week" / "so.pkl"
    counts_so_path: Path = Path(DATA_FOLDER) / "week" / "so_count.pkl"

    def filters_initial(self) -> Dict[str, Any]:
        date_from_default = datetime.datetime.now().date() - datetime.timedelta(weeks=4)
        return {
            "value_date_from": date_from_default,
            "profit_date_from": date_from_default,
        }

    def get_filters(self):
        initial = self.filters_initial()

        value_date_from = request.args.get("value_date_from") or None
        if value_date_from is None:
            value_date_from = initial.get("value_date_from")
        if isinstance(value_date_from, str):
            value_date_from = datetime.date.fromisoformat(value_date_from)

        value_date_to = request.args.get("value_date_to") or None
        if value_date_to is None:
            value_date_to = initial.get("value_date_to")
        if isinstance(value_date_to, str):
            value_date_to = datetime.date.fromisoformat(value_date_to)

        profit_date_from = request.args.get("profit_date_from") or None
        if profit_date_from is None:
            profit_date_from = initial.get("profit_date_from")
        if isinstance(profit_date_from, str):
            profit_date_from = datetime.date.fromisoformat(profit_date_from)

        profit_date_to = request.args.get("profit_date_to") or None
        if profit_date_to is None:
            profit_date_to = initial.get("profit_date_to")
        if isinstance(profit_date_to, str):
            profit_date_to = datetime.date.fromisoformat(profit_date_to)

        hide_inactive_managers = request.args.get("hide_inactive_managers")
        if hide_inactive_managers is None:
            hide_inactive_managers = initial.get("hide_inactive_managers", False)

        data = self.filters_preprocess(
            value_date_from=value_date_from,
            value_date_to=value_date_to,
            profit_date_from=profit_date_from,
            profit_date_to=profit_date_to,
            hide_inactive_managers=hide_inactive_managers,
        )

        filters_class = self.get_filters_class()

        self.filters = filters_class(**data)

    def filtering_values(self):
        if self.filters.value_date_from:
            self.values_zoom = self.values_zoom[
                self.values_zoom["date"] >= self.filters.value_date_from
            ].reset_index(drop=True)
            self.counts_zoom = self.counts_zoom[
                self.counts_zoom["date"] >= self.filters.value_date_from
            ].reset_index(drop=True)
            self.values_so = self.values_so[
                self.values_so["date"] >= self.filters.value_date_from
            ].reset_index(drop=True)
            self.counts_so = self.counts_so[
                self.counts_so["date"] >= self.filters.value_date_from
            ].reset_index(drop=True)

        if self.filters.value_date_to:
            self.values_zoom = self.values_zoom[
                self.values_zoom["date"] <= self.filters.value_date_to
            ].reset_index(drop=True)
            self.counts_zoom = self.counts_zoom[
                self.counts_zoom["date"] <= self.filters.value_date_to
            ].reset_index(drop=True)
            self.values_so = self.values_so[
                self.values_so["date"] <= self.filters.value_date_to
            ].reset_index(drop=True)
            self.counts_so = self.counts_so[
                self.counts_so["date"] <= self.filters.value_date_to
            ].reset_index(drop=True)

        if self.filters.profit_date_from:
            self.values_zoom = self.values_zoom[
                self.values_zoom["profit_date"] >= self.filters.profit_date_from
            ].reset_index(drop=True)
            self.values_so = self.values_so[
                self.values_so["profit_date"] >= self.filters.profit_date_from
            ].reset_index(drop=True)

        if self.filters.profit_date_to:
            self.values_zoom = self.values_zoom[
                self.values_zoom["profit_date"] <= self.filters.profit_date_to
            ].reset_index(drop=True)
            self.values_so = self.values_so[
                self.values_so["profit_date"] <= self.filters.profit_date_to
            ].reset_index(drop=True)

    def get_extras(self) -> Dict[str, Any]:
        cyr_month = [
            "январь",
            "февраль",
            "март",
            "апрель",
            "май",
            "июнь",
            "июль",
            "август",
            "сентябрь",
            "октябрь",
            "ноябрь",
            "декабрь",
        ]
        date = datetime.datetime.now()
        year = date.year
        month = list(
            map(
                lambda item: ("%i-%02i" % (year, item), cyr_month[item - 1]),
                range(1, date.month + 1),
            )
        )
        self.extras = {
            "exclude_columns": ["is_group", "is_total", "inactive", "manager_id"],
            "month": month,
        }

    def get(self):
        self.get_filters()

        self.managers = self.load_dataframe(self.managers_path)
        self.values_zoom = self.load_dataframe(self.values_zoom_path)
        self.counts_zoom = self.load_dataframe(self.counts_zoom_path)
        self.values_so = self.load_dataframe(self.values_so_path)
        self.counts_so = self.load_dataframe(self.counts_so_path)

        two_weeks = datetime.datetime.now().date() - datetime.timedelta(weeks=2)
        active_managers = []
        for manager_id, rows in self.counts_zoom[
            self.counts_zoom["date"] >= two_weeks
        ].groupby(by=["manager_id"]):
            if rows["count"].sum() > 0:
                active_managers.append(manager_id)
        inactive_mangers = self.managers[
            ~self.managers["manager_id"].isin(active_managers)
        ][["manager_id"]]
        inactive_mangers["inactive"] = True

        self.values_zoom = self.values_zoom[
            self.values_zoom["profit_date"] >= self.values_zoom["date"]
        ]
        self.values_so = self.values_so[
            self.values_so["profit_date"] >= self.values_so["date"]
        ]

        self.filtering_values()

        self.get_extras()

        data_zoom = pandas.DataFrame(
            list(
                map(
                    lambda item: [item[0], item[1]["profit"].sum()],
                    self.values_zoom.groupby(by=["manager_id"]),
                )
            ),
            columns=["manager_id", "profit_from_zoom"],
        )
        data_zoom_count = pandas.DataFrame(
            list(
                map(
                    lambda item: [item[0], item[1]["count"].sum()],
                    self.counts_zoom.groupby(by=["manager_id"]),
                )
            ),
            columns=["manager_id", "count_zoom"],
        )
        data_zoom: pandas.DataFrame = data_zoom.merge(
            data_zoom_count, how="outer", on=["manager_id"]
        ).reset_index(drop=True)
        data_zoom["profit_from_zoom"] = (
            data_zoom["profit_from_zoom"].fillna(0).apply(parse_int)
        )
        data_zoom["count_zoom"] = data_zoom["count_zoom"].fillna(0).apply(parse_int)
        if len(data_zoom):
            data_zoom["profit_on_zoom"] = data_zoom.apply(
                lambda item: item["profit_from_zoom"] / item["count_zoom"]
                if item["count_zoom"]
                else 0,
                axis=1,
            ).apply(parse_int)
        data_zoom = data_zoom.merge(
            pandas.DataFrame(
                self.values_zoom.groupby(by=["manager_id"])["manager_id"].count(),
                columns=["manager_id"],
            ).rename(columns={"manager_id": "payment_count_zoom"}),
            how="left",
            left_on="manager_id",
            right_index=True,
        )
        data_zoom["payment_count_zoom"] = (
            data_zoom["payment_count_zoom"].fillna(0).apply(parse_int)
        )
        if len(data_zoom):
            data_zoom["conversion_zoom"] = data_zoom.apply(
                lambda item: item["payment_count_zoom"] / item["count_zoom"]
                if item["count_zoom"]
                else 0,
                axis=1,
            )
            data_zoom["average_payment_zoom"] = data_zoom.apply(
                lambda item: item["profit_from_zoom"] / item["payment_count_zoom"]
                if item["payment_count_zoom"]
                else 0,
                axis=1,
            )

        data_so = pandas.DataFrame(
            list(
                map(
                    lambda item: [item[0], item[1]["profit"].sum()],
                    self.values_so.groupby(by=["manager_id"]),
                )
            ),
            columns=["manager_id", "profit_from_so"],
        )
        data_so_count = pandas.DataFrame(
            list(
                map(
                    lambda item: [item[0], item[1]["count"].sum()],
                    self.counts_so.groupby(by=["manager_id"]),
                )
            ),
            columns=["manager_id", "count_so"],
        )
        data_so: pandas.DataFrame = data_so.merge(
            data_so_count, how="outer", on=["manager_id"]
        ).reset_index(drop=True)
        data_so["profit_from_so"] = data_so["profit_from_so"].fillna(0).apply(parse_int)
        data_so["count_so"] = data_so["count_so"].fillna(0).apply(parse_int)
        if len(data_so):
            data_so["profit_on_so"] = data_so.apply(
                lambda item: item["profit_from_so"] / item["count_so"]
                if item["count_so"]
                else 0,
                axis=1,
            ).apply(parse_int)
        data_so = data_so.merge(
            pandas.DataFrame(
                self.values_so.groupby(by=["manager_id"])["manager_id"].count(),
                columns=["manager_id"],
            ).rename(columns={"manager_id": "payment_count_so"}),
            how="left",
            left_on="manager_id",
            right_index=True,
        )
        data_so["payment_count_so"] = (
            data_so["payment_count_so"].fillna(0).apply(parse_int)
        )
        if len(data_so):
            data_so["conversion_so"] = data_so.apply(
                lambda item: item["payment_count_so"] / item["count_so"]
                if item["count_so"]
                else 0,
                axis=1,
            )
            data_so["average_payment_so"] = data_so.apply(
                lambda item: item["profit_from_so"] / item["payment_count_so"]
                if item["payment_count_so"]
                else 0,
                axis=1,
            )

        data_merged: pandas.DataFrame = pandas.merge(
            data_zoom, data_so, how="outer", on=["manager_id"]
        ).reset_index(drop=True)

        with open(Path(DATA_FOLDER) / "week" / "groups.pkl", "rb") as file_ref:
            groups: pandas.DataFrame = pickle.load(file_ref)

        data_merged = (
            data_merged.merge(groups, how="left", on=["manager_id"])
            .rename(columns={"group": "group_id"})
            .sort_values(by=["group_id", "manager"])
            .reset_index(drop=True)
        )
        data_merged.insert(0, "is_total", False)
        data_merged.insert(1, "is_group", False)

        total_count_zoom = data_merged["count_zoom"].sum()
        total_profit_from_zoom = data_merged["profit_from_zoom"].sum()
        total_profit_on_zoom = (
            round(total_profit_from_zoom / total_count_zoom) if total_count_zoom else 0
        )
        total_payment_count_zoom = data_merged["payment_count_zoom"].sum()
        total_conversion_zoom = (
            total_payment_count_zoom / total_count_zoom if total_count_zoom else 0
        )
        total_average_payment_zoom = (
            total_profit_from_zoom / total_payment_count_zoom
            if total_payment_count_zoom
            else 0
        )
        total_count_so = data_merged["count_so"].sum()
        total_profit_from_so = data_merged["profit_from_so"].sum()
        total_profit_on_so = (
            round(total_profit_from_so / total_count_so) if total_count_so else 0
        )
        total_payment_count_so = data_merged["payment_count_so"].sum()
        total_conversion_so = (
            total_payment_count_so / total_count_so if total_count_so else 0
        )
        total_average_payment_so = (
            total_profit_from_so / total_payment_count_so
            if total_payment_count_so
            else 0
        )

        data_merged = data_merged.merge(inactive_mangers, how="left", on="manager_id")
        data_merged["inactive"].fillna(False, inplace=True)

        data = [
            pandas.DataFrame(
                [
                    {
                        "is_total": True,
                        "is_group": True,
                        "inactive": False,
                        "manager": "Всего",
                        "manager_id": "Всего",
                        "count_zoom": total_count_zoom,
                        "profit_from_zoom": total_profit_from_zoom,
                        "profit_on_zoom": total_profit_on_zoom,
                        "count_so": total_count_so,
                        "profit_from_so": total_profit_from_so,
                        "profit_on_so": total_profit_on_so,
                        "payment_count_zoom": total_payment_count_zoom,
                        "conversion_zoom": total_conversion_zoom,
                        "average_payment_zoom": total_average_payment_zoom,
                        "payment_count_so": total_payment_count_so,
                        "conversion_so": total_conversion_so,
                        "average_payment_so": total_average_payment_so,
                    }
                ]
            )
        ]
        for group_id, rows in data_merged.groupby(by=["group_id"]):
            group_count_zoom = rows["count_zoom"].sum()
            group_profit_from_zoom = rows["profit_from_zoom"].sum()
            group_profit_on_zoom = (
                round(group_profit_from_zoom / group_count_zoom)
                if group_count_zoom
                else 0
            )
            group_payment_count_zoom = rows["payment_count_zoom"].sum()
            group_conversion_zoom = (
                group_payment_count_zoom / group_count_zoom if group_count_zoom else 0
            )
            group_average_payment_zoom = (
                group_profit_from_zoom / group_payment_count_zoom
                if group_payment_count_zoom
                else 0
            )
            group_count_so = rows["count_so"].sum()
            group_profit_from_so = rows["profit_from_so"].sum()
            group_profit_on_so = (
                round(group_profit_from_so / group_count_so) if group_count_so else 0
            )
            group_payment_count_so = rows["payment_count_so"].sum()
            group_conversion_so = (
                group_payment_count_so / group_count_so if group_count_so else 0
            )
            group_average_payment_so = (
                group_profit_from_so / group_payment_count_so
                if group_payment_count_so
                else 0
            )
            data += [
                pandas.DataFrame(
                    [
                        {
                            "is_total": False,
                            "is_group": True,
                            "inactive": len(rows[~rows["inactive"]]) == 0,
                            "manager": f'Группа "{group_id}"',
                            "manager_id": group_id,
                            "count_zoom": group_count_zoom,
                            "profit_from_zoom": group_profit_from_zoom,
                            "profit_on_zoom": group_profit_on_zoom,
                            "count_so": group_count_so,
                            "profit_from_so": group_profit_from_so,
                            "profit_on_so": group_profit_on_so,
                            "payment_count_zoom": group_payment_count_zoom,
                            "conversion_zoom": group_conversion_zoom,
                            "average_payment_zoom": group_average_payment_zoom,
                            "payment_count_so": group_payment_count_so,
                            "conversion_so": group_conversion_so,
                            "average_payment_so": group_average_payment_so,
                        }
                    ]
                ),
                rows,
            ]
        data = pandas.concat(data, ignore_index=True)[
            [
                "is_total",
                "is_group",
                "inactive",
                "manager",
                "manager_id",
                "count_zoom",
                "profit_from_zoom",
                "profit_on_zoom",
                "payment_count_zoom",
                "conversion_zoom",
                "average_payment_zoom",
                "count_so",
                "profit_from_so",
                "profit_on_so",
                "payment_count_so",
                "conversion_so",
                "average_payment_so",
            ]
        ]
        data["count_zoom"] = data["count_zoom"].fillna(0).apply(parse_int)
        data["profit_from_zoom"] = data["profit_from_zoom"].fillna(0).apply(parse_int)
        data["profit_on_zoom"] = data["profit_on_zoom"].fillna(0).apply(parse_int)
        data["payment_count_zoom"] = (
            data["payment_count_zoom"].fillna(0).apply(parse_int)
        )
        data["conversion_zoom"] = data["conversion_zoom"].fillna(0).apply(parse_percent)
        data["average_payment_zoom"] = (
            data["average_payment_zoom"].fillna(0).apply(parse_int)
        )
        data["count_so"] = data["count_so"].fillna(0).apply(parse_int)
        data["profit_from_so"] = data["profit_from_so"].fillna(0).apply(parse_int)
        data["profit_on_so"] = data["profit_on_so"].fillna(0).apply(parse_int)
        data["payment_count_so"] = data["payment_count_so"].fillna(0).apply(parse_int)
        data["conversion_so"] = data["conversion_so"].fillna(0).apply(parse_percent)
        data["average_payment_so"] = (
            data["average_payment_so"].fillna(0).apply(parse_int)
        )
        data.rename(
            columns={
                "manager": "Менеджер/Группа",
                "count_zoom": "Количество Zoom",
                "profit_from_zoom": "Оборот от Zoom",
                "profit_on_zoom": "Оборот на Zoom",
                "payment_count_zoom": "Оплаты Zoom",
                "conversion_zoom": "Конверсия Zoom",
                "average_payment_zoom": "Средний чек Zoom",
                "count_so": "Количество SO",
                "profit_from_so": "Оборот от SO",
                "profit_on_so": "Оборот на SO",
                "payment_count_so": "Оплаты SO",
                "conversion_so": "Конверсия SO",
                "average_payment_so": "Средний чек SO",
            },
            inplace=True,
        )

        self.context("filters", self.filters)
        self.context("extras", self.extras)
        self.context("data", data)

        return super().get()


class ManagersSalesCoursesView(FilteringBaseView):
    template_name = "managers/sales/courses/index.html"
    title = "Продажи менеджеров по курсам"

    filters_class = ManagersSalesCoursesFiltersData
    filters: ManagersSalesCoursesFiltersData

    sales: pandas.DataFrame

    sales_path: Path = Path(DATA_FOLDER) / "week" / "managers_sales.pkl"

    def get_filters(self):
        initial = self.filters_initial()

        payment_date_from = request.args.get("payment_date_from") or None
        if payment_date_from is None:
            payment_date_from = initial.get("payment_date_from")
        if isinstance(payment_date_from, str):
            payment_date_from = datetime.date.fromisoformat(payment_date_from)

        payment_date_to = request.args.get("payment_date_to") or None
        if payment_date_to is None:
            payment_date_to = initial.get("payment_date_to")
        if isinstance(payment_date_to, str):
            payment_date_to = datetime.date.fromisoformat(payment_date_to)

        data = self.filters_preprocess(
            payment_date_from=payment_date_from,
            payment_date_to=payment_date_to,
        )

        filters_class = self.get_filters_class()

        self.filters = filters_class(**data)

    def filtering_values(self):
        if self.filters.payment_date_from:
            self.sales = self.sales[
                self.sales["payment_date"] >= self.filters.payment_date_from
            ].reset_index(drop=True)
        if self.filters.payment_date_to:
            self.sales = self.sales[
                self.sales["payment_date"] <= self.filters.payment_date_to
            ].reset_index(drop=True)

    def get_extras(self) -> Dict[str, Any]:
        self.extras = {
            "exclude_columns": ["is_manager"],
        }

    def get(self):
        self.get_filters()

        self.sales = self.load_dataframe(self.sales_path)
        self.sales.sort_values(by=["manager"], inplace=True, ignore_index=True)

        self.filtering_values()
        self.get_extras()

        sales = self.sales[~self.sales["surcharge"]]
        source = []
        profit_total = self.sales["profit"].sum()
        for manager_name, manager in sales.groupby(by=["manager"], sort=False):
            profit_manager = manager["profit"].sum()
            profit_manager_total = self.sales[self.sales["manager"] == manager_name][
                "profit"
            ].sum()
            surcharge_manager = self.sales[
                self.sales["surcharge"] & (self.sales["manager"] == manager_name)
            ]
            source.append(
                {
                    "is_manager": True,
                    "name": manager_name,
                    "profit": profit_manager,
                    "profit_percent": profit_manager / profit_manager_total * 100,
                    "profit_percent_total": profit_manager / profit_total * 100,
                    "surcharge": surcharge_manager["profit"].sum(),
                }
            )
            for course_name, course in manager.groupby(by=["course"]):
                profit_course = course["profit"].sum()
                surcharge_course = surcharge_manager[
                    surcharge_manager["course"] == course_name
                ]
                source.append(
                    {
                        "is_manager": False,
                        "name": course_name,
                        "profit": profit_course,
                        "profit_percent": profit_course / profit_manager_total * 100,
                        "profit_percent_total": profit_course / profit_total * 100,
                        "surcharge": surcharge_course["profit"].sum(),
                    }
                )

        data = pandas.DataFrame(source).rename(
            columns={
                "name": "",
                "profit": "Оборот",
                "profit_percent": "% от менеджера с учетом доплат",
                "profit_percent_total": "% от компании с учетом доплат",
                "surcharge": "Доплаты",
            }
        )

        self.context("filters", self.filters)
        self.context("extras", self.extras)
        self.context("data", data)

        return super().get()


class ManagersSalesDatesView(FilteringBaseView):
    template_name = "managers/sales/dates/index.html"
    title = "Продажи менеджеров по датам заявок"

    filters_class = ManagersSalesDatesFiltersData
    filters: ManagersSalesDatesFiltersData

    sales: pandas.DataFrame

    sales_path: Path = Path(DATA_FOLDER) / "week" / "managers_sales.pkl"

    def get_filters(self):
        initial = self.filters_initial()

        payment_date_from = request.args.get("payment_date_from") or None
        if payment_date_from is None:
            payment_date_from = initial.get("payment_date_from")
        if isinstance(payment_date_from, str):
            payment_date_from = datetime.date.fromisoformat(payment_date_from)

        payment_date_to = request.args.get("payment_date_to") or None
        if payment_date_to is None:
            payment_date_to = initial.get("payment_date_to")
        if isinstance(payment_date_to, str):
            payment_date_to = datetime.date.fromisoformat(payment_date_to)

        course = request.args.get("course")
        if course is None:
            course = initial.get("course", "__all__")
        if course == "__all__":
            course = None

        data = self.filters_preprocess(
            payment_date_from=payment_date_from,
            payment_date_to=payment_date_to,
            course=course,
        )

        filters_class = self.get_filters_class()

        self.filters = filters_class(**data)

    def filtering_values(self):
        if self.filters.payment_date_from:
            self.sales = self.sales[
                self.sales["payment_date"] >= self.filters.payment_date_from
            ].reset_index(drop=True)
        if self.filters.payment_date_to:
            self.sales = self.sales[
                self.sales["payment_date"] <= self.filters.payment_date_to
            ].reset_index(drop=True)

    def get_extras_group(self, group: str) -> List[List[str]]:
        group_id = f"{group}_id"
        groups: pandas.DataFrame = (
            self.sales[[group_id, group]]
            .drop_duplicates()
            .sort_values(group)
            .reset_index(drop=True)
        )
        if self.filters[group] not in list(groups[group_id]):
            self.filters[group] = None
        if self.filters[group] is not None:
            self.sales = self.sales[
                self.sales[group_id] == self.filters[group]
            ].reset_index(drop=True)
        return groups.values.tolist()

    def get_extras(self) -> Dict[str, Any]:
        self.extras = {
            "courses": self.get_extras_group("course"),
            "exclude_columns": ["is_date"],
        }

    def get(self):
        self.get_filters()

        self.sales = self.load_dataframe(self.sales_path)
        self.sales["course_id"] = self.sales["course"].apply(parse_slug)
        self.sales.sort_values(
            by=["order_date"], ascending=[False], inplace=True, ignore_index=True
        )

        self.filtering_values()
        self.get_extras()

        months = [
            "январь",
            "февраль",
            "март",
            "апрель",
            "май",
            "июнь",
            "июль",
            "август",
            "сентябрь",
            "октябрь",
            "ноябрь",
            "декабрь",
        ]
        current_year = datetime.datetime.now().year
        self.sales["order_date_name"] = self.sales["order_date"].apply(
            lambda item: "undefined"
            if pandas.isna(item)
            else (
                f"{item.year}, {months[item.month-1]}"
                if item.year == current_year
                else f"{item.year}"
            )
        )

        source = []
        profit_total = self.sales["profit"].sum()
        for order_date_name, order_date in self.sales.groupby(
            by=["order_date_name"], sort=False
        ):
            profit_order_date = order_date["profit"].sum()
            profit_order_date_total = self.sales[
                self.sales["order_date_name"] == order_date_name
            ]["profit"].sum()
            source.append(
                {
                    "is_date": True,
                    "name": order_date_name,
                    "profit": profit_order_date,
                    "profit_percent": profit_order_date / profit_order_date_total * 100,
                    "profit_percent_total": profit_order_date / profit_total * 100,
                }
            )
            for manager_name, manager in order_date.groupby(by=["manager"]):
                profit_manager = manager["profit"].sum()
                source.append(
                    {
                        "is_date": False,
                        "name": manager_name,
                        "profit": profit_manager,
                        "profit_percent": profit_manager
                        / profit_order_date_total
                        * 100,
                        "profit_percent_total": profit_manager / profit_total * 100,
                    }
                )

        data = pandas.DataFrame(source).rename(
            columns={
                "name": "",
                "profit": "Оборот",
                "profit_percent": "% от менеджера",
                "profit_percent_total": "% от компании",
            }
        )

        self.context("filters", self.filters)
        self.context("extras", self.extras)
        self.context("data", data)

        return super().get()


class WeekStatsChannelsView(FilteringBaseView):
    template_name = "week-stats/channels/index.html"
    title = "Каналы трафика"

    filters_class = WeekStatsFiltersChannelsData
    filters: WeekStatsFiltersChannelsData

    roistat: pandas.DataFrame
    channels_count: pandas.DataFrame
    values_expenses: pandas.DataFrame
    counts_expenses: pandas.DataFrame

    channels_count_path: Path = Path(DATA_FOLDER) / "week" / "channels_count_russia.pkl"
    values_expenses_path: Path = Path(DATA_FOLDER) / "week" / "expenses.pkl"
    counts_expenses_path: Path = Path(DATA_FOLDER) / "week" / "expenses_count.pkl"

    def filters_initial(self) -> Dict[str, Any]:
        date_from_default = datetime.datetime.now().date() - datetime.timedelta(weeks=4)
        return {
            "order_date_from": date_from_default,
            "profit_date_from": date_from_default,
        }

    def get_filters(self):
        initial = self.filters_initial()

        order_date_from = request.args.get("order_date_from") or None
        if order_date_from is None:
            order_date_from = initial.get("order_date_from")
        if isinstance(order_date_from, str):
            order_date_from = datetime.date.fromisoformat(order_date_from)

        order_date_to = request.args.get("order_date_to") or None
        if order_date_to is None:
            order_date_to = initial.get("order_date_to")
        if isinstance(order_date_to, str):
            order_date_to = datetime.date.fromisoformat(order_date_to)

        profit_date_from = request.args.get("profit_date_from") or None
        if profit_date_from is None:
            profit_date_from = initial.get("profit_date_from")
        if isinstance(profit_date_from, str):
            profit_date_from = datetime.date.fromisoformat(profit_date_from)

        profit_date_to = request.args.get("profit_date_to") or None
        if profit_date_to is None:
            profit_date_to = initial.get("profit_date_to")
        if isinstance(profit_date_to, str):
            profit_date_to = datetime.date.fromisoformat(profit_date_to)

        data = self.filters_preprocess(
            order_date_from=order_date_from,
            order_date_to=order_date_to,
            profit_date_from=profit_date_from,
            profit_date_to=profit_date_to,
        )

        filters_class = self.get_filters_class()

        self.filters = filters_class(**data)

    def filtering_values(self):
        if self.filters.order_date_from:
            self.values_expenses = self.values_expenses[
                self.values_expenses["date"] >= self.filters.order_date_from
            ].reset_index(drop=True)
            self.counts_expenses = self.counts_expenses[
                self.counts_expenses["date"] >= self.filters.order_date_from
            ].reset_index(drop=True)
            self.channels_count = self.channels_count[
                self.channels_count["date"] >= self.filters.order_date_from
            ].reset_index(drop=True)
            self.roistat = self.roistat[
                self.roistat["date"] >= self.filters.order_date_from
            ].reset_index(drop=True)

        if self.filters.order_date_to:
            self.values_expenses = self.values_expenses[
                self.values_expenses["date"] <= self.filters.order_date_to
            ].reset_index(drop=True)
            self.counts_expenses = self.counts_expenses[
                self.counts_expenses["date"] <= self.filters.order_date_to
            ].reset_index(drop=True)
            self.channels_count = self.channels_count[
                self.channels_count["date"] <= self.filters.order_date_to
            ].reset_index(drop=True)
            self.roistat = self.roistat[
                self.roistat["date"] <= self.filters.order_date_to
            ].reset_index(drop=True)

        if self.filters.profit_date_from:
            self.values_expenses = self.values_expenses[
                self.values_expenses["profit_date"] >= self.filters.profit_date_from
            ].reset_index(drop=True)

        if self.filters.profit_date_to:
            self.values_expenses = self.values_expenses[
                self.values_expenses["profit_date"] <= self.filters.profit_date_to
            ].reset_index(drop=True)

    def get_extras(self) -> Dict[str, Any]:
        cyr_month = [
            "январь",
            "февраль",
            "март",
            "апрель",
            "май",
            "июнь",
            "июль",
            "август",
            "сентябрь",
            "октябрь",
            "ноябрь",
            "декабрь",
        ]
        date = datetime.datetime.now()
        year = date.year
        month = list(
            map(
                lambda item: ("%i-%02i" % (year, item), cyr_month[item - 1]),
                range(1, date.month + 1),
            )
        )
        self.extras = {
            "exclude_columns": ["is_total", "count"],
            "month": month,
        }

    def get_accounts_as_channel(self) -> pandas.DataFrame:
        channels = (
            PickleLoader()
            .roistat_statistics[["account", "account_title"]]
            .drop_duplicates()
        )
        channels.loc[channels["account"] == "", "account"] = "prjamye_vizity"
        channels.loc[channels["account_title"] == "Undefined", "account"] = ""
        channels = (
            channels.drop_duplicates(subset=["account"])
            .rename(columns={"account_title": "channel"})
            .reset_index(drop=True)
        )
        channels["channel_id"] = channels["channel"].apply(parse_slug)
        return channels

    def get(self):
        self.get_filters()

        self.roistat = PickleLoader().roistat_leads
        self.roistat = self.roistat[["date", "account", "ipl", "qa1"]].rename(
            columns={"qa1": "country"}
        )
        self.roistat["date"] = self.roistat["date"].apply(lambda item: item.date())
        self.roistat = self.roistat.merge(
            self.get_accounts_as_channel(),
            how="left",
            on="account",
        )

        self.channels_count = self.load_dataframe(self.channels_count_path)
        self.values_expenses = self.load_dataframe(self.values_expenses_path)
        self.counts_expenses = self.load_dataframe(self.counts_expenses_path)

        self.values_expenses = self.values_expenses[
            self.values_expenses["profit_date"] >= self.values_expenses["date"]
        ]

        self.filtering_values()

        self.get_extras()

        data_expenses = pandas.DataFrame(
            list(
                map(
                    lambda item: [item[0], item[1]["profit"].sum()],
                    self.values_expenses.groupby(by=["channel_id"]),
                )
            ),
            columns=["channel_id", "profit_from_expenses"],
        )

        data_expenses_ipl_list = []
        for channel_id, rows in self.values_expenses.groupby(by=["channel_id"]):
            roistat_leads = self.roistat[self.roistat["channel_id"] == channel_id]
            data_expenses_ipl_list.append(
                {
                    "channel_id": channel_id,
                    "ipl": roistat_leads["ipl"].sum() / len(roistat_leads),
                }
            )
        data_expenses_ipl = pandas.DataFrame(
            data=data_expenses_ipl_list, columns=["channel_id", "ipl"]
        )
        data_expenses: pandas.DataFrame = data_expenses.merge(
            data_expenses_ipl, how="left", on=["channel_id"]
        ).reset_index(drop=True)

        data_expenses_count = pandas.DataFrame(
            list(
                map(
                    lambda item: [item[0], item[1]["count"].sum()],
                    self.counts_expenses.groupby(by=["channel_id"]),
                )
            ),
            columns=["channel_id", "count_expenses"],
        )
        data_expenses: pandas.DataFrame = data_expenses.merge(
            data_expenses_count, how="outer", on=["channel_id"]
        ).reset_index(drop=True)

        data_expenses["ipl"] = data_expenses["ipl"].fillna(0).apply(parse_int)
        data_expenses["profit_from_expenses"] = (
            data_expenses["profit_from_expenses"].fillna(0).apply(parse_int)
        )
        data_expenses["count_expenses"] = (
            data_expenses["count_expenses"].fillna(0).apply(parse_int)
        )
        if len(data_expenses):
            data_expenses["profit_on_expenses"] = data_expenses.apply(
                lambda item: item["profit_from_expenses"] / item["count_expenses"]
                if item["count_expenses"]
                else 0,
                axis=1,
            ).apply(parse_float)
        data_expenses = data_expenses.merge(
            self.channels_count.groupby(by=["channel_id"]).sum(),
            how="left",
            left_on="channel_id",
            right_index=True,
        )
        data_expenses = data_expenses.merge(
            pandas.DataFrame(
                self.values_expenses.groupby(by=["channel_id"])["channel_id"].count(),
                columns=["channel_id"],
            ).rename(columns={"channel_id": "payment_count_expenses"}),
            how="left",
            left_on="channel_id",
            right_index=True,
        )
        data_expenses["payment_count_expenses"] = (
            data_expenses["payment_count_expenses"].apply(parse_int).fillna(0)
        )
        data_expenses["count"] = data_expenses["count"].apply(parse_int).fillna(0)
        data_expenses["conversion_expenses"] = data_expenses.apply(
            lambda item: item["payment_count_expenses"] / item["count"]
            if item["count"]
            else 0,
            axis=1,
        )
        data_expenses["average_payment_expenses"] = data_expenses.apply(
            lambda item: item["profit_from_expenses"] / item["payment_count_expenses"]
            if item["payment_count_expenses"]
            else 0,
            axis=1,
        )
        data_expenses["lead_price"] = data_expenses.apply(
            lambda item: item["count_expenses"] / item["count"] if item["count"] else 0,
            axis=1,
        )
        data_expenses["profit_on_lead"] = data_expenses.apply(
            lambda item: item["profit_from_expenses"] / item["count"]
            if item["count"]
            else 0,
            axis=1,
        )

        data_expenses.insert(0, "is_total", False)

        with open(Path(DATA_FOLDER) / "week" / "channels.pkl", "rb") as file_ref:
            channels: pandas.DataFrame = pickle.load(file_ref)
        data_expenses = data_expenses.merge(
            channels, how="left", on=["channel_id"]
        ).drop(columns=["channel_id"])

        count_expenses = data_expenses["count_expenses"].sum()
        count_total = data_expenses["count"].sum()
        profit_from_expenses = data_expenses["profit_from_expenses"].sum()
        profit_on_expenses = (
            profit_from_expenses / count_expenses if count_expenses else 0
        )
        payment_count_expenses = data_expenses["payment_count_expenses"].sum()
        conversion_expenses = payment_count_expenses / count_total if count_total else 0
        average_payment_expenses = (
            profit_from_expenses / payment_count_expenses
            if payment_count_expenses
            else 0
        )
        lead_price = count_expenses / count_total if count_total else 0
        profit_on_lead = profit_from_expenses / count_total if count_total else 0
        ipl_available = data_expenses[data_expenses["ipl"] > 0]
        ipl = (
            ipl_available["ipl"].sum() / len(ipl_available) if len(ipl_available) else 0
        )
        data = pandas.concat(
            [
                pandas.DataFrame(
                    [
                        {
                            "is_total": True,
                            "channel": "Всего",
                            "count_expenses": count_expenses,
                            "profit_from_expenses": profit_from_expenses,
                            "profit_on_expenses": profit_on_expenses,
                            "count": count_total,
                            "payment_count_expenses": payment_count_expenses,
                            "conversion_expenses": conversion_expenses,
                            "average_payment_expenses": average_payment_expenses,
                            "lead_price": lead_price,
                            "profit_on_lead": profit_on_lead,
                            "ipl": ipl,
                        }
                    ]
                ),
                data_expenses,
            ]
        )
        data["count_expenses"] = data["count_expenses"].apply(parse_int).fillna(0)
        data["profit_from_expenses"] = (
            data["profit_from_expenses"].apply(parse_int).fillna(0)
        )
        data["profit_on_expenses"] = (
            data["profit_on_expenses"].apply(parse_percent).fillna(0)
        )
        data["conversion_expenses"] = (
            data["conversion_expenses"].apply(parse_percent).fillna(0)
        )
        data.rename(
            columns={
                "channel": "Канал",
                "count_expenses": "Расход",
                "profit_from_expenses": "Оборот",
                "profit_on_expenses": "Процент",
                "count": "Лиды",
                "payment_count_expenses": "Оплаты",
                "conversion_expenses": "Конверсия",
                "average_payment_expenses": "Средний чек",
                "lead_price": "Цена лида",
                "profit_on_lead": "Оборот на лид",
                "ipl": "IPL",
            },
            inplace=True,
        )

        self.context("filters", self.filters)
        self.context("extras", self.extras)
        self.context("data", data)

        return super().get()


class SearchLeadsView(TemplateView):
    template_name = "search-leads/index.html"
    title = "Поиск лидов"
    in_updating: bool = False

    def get_filters(self, source: ImmutableMultiDict) -> SearchLeadsFiltersData:
        return SearchLeadsFiltersData(id=source.get("id", ""))

    def merge_columns(self, value):
        values = list(filter(lambda item: item != "", value))
        return str(values[0]) if len(values) else ""

    def get_data(self, filters: SearchLeadsFiltersData) -> pandas.DataFrame:
        data: pandas.DataFrame = pandas.DataFrame()

        if filters.id:
            target_file = Path(DATA_FOLDER) / "api" / "tilda" / "leads.pkl"
            try:
                with open(target_file, "rb") as file_ref:
                    source: pandas.DataFrame = pickle.load(file_ref)
            except FileNotFoundError:
                source: pandas.DataFrame = pandas.DataFrame()
            except Exception:
                self.in_updating = True
                return data

            source.rename(
                columns=dict(
                    map(lambda item: (item, re.sub(r"_+", " ", item)), source.columns)
                ),
                inplace=True,
            )
            data: pandas.DataFrame = source[
                source["requestid"].str.contains(filters.id, case=False, na=False)
            ]
            data.fillna("", inplace=True)
            if "name" not in data.columns:
                data["name"] = ""
            if "phone" not in data.columns:
                data["phone"] = ""
            if "email" not in data.columns:
                data["email"] = ""
            data["Name"] = data[["name", "Name"]].apply(self.merge_columns, axis=1)
            data["Phone"] = data[["phone", "Phone"]].apply(self.merge_columns, axis=1)
            data["Email"] = data[["email", "Email"]].apply(self.merge_columns, axis=1)
            if len(data):
                data["__id__"] = data.apply(
                    lambda item: item["requestid"] or item["tranid"], axis=1
                )
            data.drop(columns=["name", "phone", "email"], inplace=True)

        return data

    def get(self):
        filters = self.get_filters(request.args)

        self.context("filters", filters)
        self.context("data", self.get_data(filters))
        self.context("in_updating", self.in_updating)

        return super().get()


class ChangeZoomView(APIView):
    def post(self, manager_id: str, lead: str, date: str):
        file_path = Path(DATA_FOLDER) / "week" / "managers_zooms_controllable.pkl"
        available_values = {
            "manager_id": parse_slug,
            "lead": parse_int,
            "date": parse_date,
            "purchase_probability": parse_percent_value,
            "potential_order_amount": parse_positive_int_value,
            "expected_payment_date": parse_date,
            "on_control": parse_bool_from_int,
        }
        post = dict(
            filter(
                lambda item: item[0] in available_values.keys(),
                {
                    **dict(request.form),
                    "manager_id": manager_id,
                    "lead": lead,
                    "date": date,
                }.items(),
            )
        )
        values = pandas.DataFrame([post])
        for column in values.columns:
            values[column] = values[column].apply(available_values.get(column))

        source = pandas.DataFrame(columns=available_values.keys())
        try:
            with open(file_path, "rb") as file_ref:
                source = pandas.concat([source, pickle.load(file_ref)])
        except FileNotFoundError:
            pass

        source_one = source[
            (source["manager_id"] == values.loc[0, "manager_id"])
            & (source["lead"] == values.loc[0, "lead"])
            & (source["date"] == values.loc[0, "date"])
        ]
        if len(source_one):
            source_data = source_one.iloc[0].to_dict()
            source_data.update(**values.iloc[0].to_dict())
            source.drop(index=source_one.index, inplace=True)
            values = pandas.DataFrame([source_data])

        source = pandas.concat([source, values])
        source.fillna(pandas.NA, inplace=True)
        source.reset_index(drop=True, inplace=True)
        with open(file_path, "wb") as file_ref:
            pickle.dump(source, file_ref)

        info: pandas.Series = values.iloc[0]
        info_columns = list(info.keys())
        estimate = ""
        if (
            "purchase_probability" in info_columns
            and "potential_order_amount" in info_columns
            and not pandas.isna(info["purchase_probability"])
            and not pandas.isna(info["potential_order_amount"])
        ):
            estimate = f'{round(info["potential_order_amount"] * info["purchase_probability"] / 100):,} ₽'.replace(
                ",", " "
            )

        self.data = {"estimate": estimate}

        return super().post()


class IntensivesBaseView(FilteringBaseView):
    template_name = "intensives/index.html"

    filters_class = IntensivesFiltersData
    filters: IntensivesFiltersData

    extras: Dict[str, Any] = {}
    source: pandas.DataFrame
    source_type: str

    def get_filters(self):
        initial = self.filters_initial()

        date_from = request.args.get("date_from")
        if date_from is None:
            date_from = initial.get("date_from")
        if isinstance(date_from, str):
            date_from = (
                datetime.date.fromisoformat(date_from) if str(date_from) else None
            )

        date_to = request.args.get("date_to")
        if date_to is None:
            date_to = initial.get("date_to")
        if isinstance(date_to, str):
            date_to = datetime.date.fromisoformat(date_to) if str(date_to) else None

        data = self.filters_preprocess(
            date_from=date_from,
            date_to=date_to,
        )

        filters_class = self.get_filters_class()

        self.filters = filters_class(**data)

    def filtering_values(self):
        if self.source is None:
            return

        if self.filters.date_from:
            self.source = self.source[self.source["date"] >= self.filters.date_from]

        if self.filters.date_to:
            self.source = self.source[self.source["date"] <= self.filters.date_to]

        self.source.reset_index(drop=True, inplace=True)

    def get_extras(self):
        self.extras = {
            "exclude_columns": [],
        }

    def get(self, is_download=False):
        try:
            self.source = pickle_loader(self.source_type)
        except FileNotFoundError:
            self.source = None
        self.get_filters()
        self.filtering_values()
        self.get_extras()

        intensives = []
        if self.source is not None:
            for group_name, group in self.source.groupby(by=["date"]):
                intensives.append(
                    [group_name, group["deals"].sum(), group["profit"].sum(), 0]
                )

        data = pandas.DataFrame(intensives, columns=["date", "deals", "profit", "ppd"])
        if len(data):
            data["ppd"] = data.apply(
                lambda item: round(item["profit"] / item["deals"])
                if item["deals"]
                else 0,
                axis=1,
            )

        columns = {
            "date": "Дата интенсива",
            "deals": "Количество сделок",
            "profit": "Выручка",
            "ppd": "Выручка за сделку",
        }
        total_deals = data["deals"].sum()
        total_profit = data["profit"].sum()
        total = pandas.Series(
            {
                columns.get("date"): "Итого",
                columns.get("deals"): total_deals,
                columns.get("profit"): total_profit,
                columns.get("ppd"): round(total_profit / total_deals)
                if total_deals
                else 0,
            }
        )
        data.rename(columns=columns, inplace=True)

        self.context("filters", self.filters)
        self.context("extras", self.extras)
        self.context("data", data)
        self.context("total", total)

        return super().get()


class IntensivesRegistrationView(IntensivesBaseView):
    title = "Сделки с регистраций на интенсивы"
    source_type = "intensives_registration"


class IntensivesPreorderView(IntensivesBaseView):
    title = "Сделки с предзаказов на интенсивах"
    source_type = "intensives_preorder"
