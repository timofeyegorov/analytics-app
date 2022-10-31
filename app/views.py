import json

import pytz
import pandas
import requests
import tempfile

from enum import Enum
from math import ceil
from pathlib import Path
from typing import Tuple, List, Dict, Any, Optional
from datetime import datetime, timedelta, timezone
from collections import OrderedDict
from urllib.parse import urlparse, parse_qsl, urlencode
from pydantic import BaseModel, ConstrainedDate
from werkzeug.datastructures import ImmutableMultiDict

from xlsxwriter import Workbook

from flask import request, render_template, send_file, abort
from flask.views import MethodView

from app.plugins.ads import vk
from app.analytics.pickle_load import PickleLoader
from app.dags.vk import reader as vk_reader, data as vk_data
from app.data import (
    StatisticsProviderEnum,
    StatisticsGroupByEnum,
    StatisticsRoistatPackageEnum,
    StatisticsRoistatGroupByEnum,
    CalculateColumnEnum,
)


pickle_loader = PickleLoader()

UNDEFINED = "Undefined"


class ContextTemplate:
    data: Dict[str, Any] = {}

    def __call__(self, name: str, value: Any):
        self.data.update({name: value})


class TemplateView(MethodView):
    context: ContextTemplate = ContextTemplate()
    template_name: str
    title: str = ""

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


class StatisticsFiltersData(BaseModel):
    date: Tuple[Optional[ConstrainedDate], Optional[ConstrainedDate]]
    provider: Optional[str]
    account: Optional[int]
    campaign: Optional[int]
    group: Optional[int]
    groupby: str


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
        positive_month: StatusColor,
        activity_period: StatusColor,
        activity_month: StatusColor,
    ) -> Action:
        if activity_period == StatusColor.high:
            if positive_period in (StatusColor.high, StatusColor.middle):
                return Action.suppose
            elif positive_period == StatusColor.low:
                return Action.disable
        elif activity_period in (StatusColor.middle, StatusColor.low):
            if activity_month == StatusColor.high:
                if positive_month in (StatusColor.high, StatusColor.middle):
                    return Action.suppose
                elif positive_month == StatusColor.low:
                    return Action.disable
            elif activity_month in (StatusColor.middle, StatusColor.low):
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
        filters: StatisticsRoistatFiltersData,
    ):
        self._leads = leads
        self._statistics = statistics
        self._filters = filters

        stats = dict(
            map(
                lambda item: (str(item[0]), item[1]),
                self._statistics.groupby(by=self._filters.groupby, dropna=False),
            )
        )

        self._data = pandas.DataFrame(columns=self.columns.keys())
        count = 0
        for name, group in self._leads.groupby(by=self._filters.groupby, dropna=False):
            leads = len(group)
            if not leads:
                continue

            stats_group = stats.get(str(name))
            if stats_group is None:
                print("------------------------")
                print(name)
                print(stats.keys())
                stats.update(
                    {
                        StatisticsRoistatPackageEnum.undefined.name: pandas.DataFrame(
                            columns=list(self._statistics.columns)
                        )
                    }
                )
                print(stats.keys())
                print("------------------------")
            count += leads

            expenses = round(stats_group.expenses.sum())
            if not expenses:
                continue

            name = stats_group[self._filters.groupby].unique()[0]
            title = stats_group[f"{self._filters.groupby}_title"].unique()[0]
            income = int(group.ipl.sum())
            ipl = int(round(income / leads))
            profit = int(round(income - expenses))
            ppl = int(round(profit / leads))
            cpl = int(round(expenses / leads))
            ppl_range = detect_positive(ppl)
            ppl_30d = detect_positive(0)
            leads_range = detect_activity(leads)
            leads_30d = detect_activity(0)
            action = detect_action(ppl_range, ppl_30d, leads_range, leads_30d)
            self._data = self._data.append(
                {
                    CalculateColumnEnum.name.name: (
                        "" if title is None else title,
                        action.name,
                        name,
                    ),
                    CalculateColumnEnum.leads.name: leads,
                    CalculateColumnEnum.income.name: income,
                    CalculateColumnEnum.ipl.name: ipl,
                    CalculateColumnEnum.expenses.name: expenses,
                    CalculateColumnEnum.profit.name: profit,
                    CalculateColumnEnum.ppl.name: ppl,
                    CalculateColumnEnum.cpl.name: cpl,
                    CalculateColumnEnum.ppl_range.name: (ppl, ppl_range.value),
                    CalculateColumnEnum.ppl_30d.name: (0, ppl_30d.value),
                    CalculateColumnEnum.leads_range.name: (leads, leads_range.value),
                    CalculateColumnEnum.leads_30d.name: (0, leads_30d.value),
                    CalculateColumnEnum.action.name: (action.value, action.name),
                },
                ignore_index=True,
            )

        print(count)
        self._data = self._data.reset_index(drop=True)

    @property
    def columns(self) -> Dict[str, str]:
        return CalculateColumnEnum.dict()

    @property
    def data(self) -> pandas.DataFrame:
        return self._data


class StatisticsRoistatView(TemplateView):
    template_name: str = "statistics/roistat/index.html"
    title: str = "Статистика Roistat"
    filters: StatisticsRoistatFiltersData = None
    leads = None
    statistics = None
    extras = None

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
        leads = pickle_loader.roistat_leads
        statistics = pickle_loader.roistat_statistics

        tz = pytz.timezone("Europe/Moscow")

        if self.filters.date[0]:
            date_from = tz.localize(
                datetime(
                    year=self.filters.date[0].year,
                    month=self.filters.date[0].month,
                    day=self.filters.date[0].day,
                )
            )
            leads = leads[leads.date >= date_from]
            statistics = statistics[statistics.date >= date_from]

        if self.filters.date[1]:
            date_to = tz.localize(
                datetime(
                    year=self.filters.date[1].year,
                    month=self.filters.date[1].month,
                    day=self.filters.date[1].day,
                )
            )
            leads = leads[leads.date <= date_to]
            statistics = statistics[statistics.date <= date_to]

        if self.filters.only_ru:
            leads = leads[leads.qa1 == "Россия"]

        return leads, statistics

    def get_extras_group(self, group: str) -> List[Tuple[str, str]]:
        stats_groups = []
        for name, item in self.statistics.groupby(group):
            if not item.expenses.sum():
                continue
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
                }
            )
            .sort_values(by=["Дата"])
            .reset_index(drop=True)
        )

        return (
            {
                "title": "Дополнительная таблица",
                "data": pandas.DataFrame(),
            },
            {
                "title": f"Лиды в разбивке по {StatisticsRoistatGroupByEnum[self.filters.groupby].value} = {statistics[f'{self.filters.groupby}_title'].unique()[0]}",
                "data": leads,
            },
        )

    def get(self):
        self.filters = self.get_filters(request.args)
        self.leads, self.statistics = self.get_statistics()
        self.extras = self.get_extras()
        self.extras["columns"]["name"] = StatisticsRoistatGroupByEnum[
            self.filters.groupby
        ].value

        calc = Calculate(self.leads, self.statistics, self.filters)

        total_data = dict(zip(calc.columns.keys(), [None] * len(calc.columns.keys())))

        total_title = "Итого"
        total_leads = calc.data.leads.sum()
        total_income = calc.data.income.sum()
        total_ipl = int(round(total_income / total_leads)) if total_leads else 0
        total_expenses = calc.data.expenses.sum()
        total_profit = int(round(total_income - total_expenses))
        total_ppl = int(round(total_profit / total_leads)) if total_leads else 0
        total_cpl = int(round(total_expenses / total_leads)) if total_leads else 0
        total_ppl_range = detect_positive(total_ppl)
        total_ppl_30d = detect_positive(0)
        total_leads_range = detect_activity(total_leads)
        total_leads_30d = detect_activity(0)
        total_action = detect_action(
            total_ppl_range, total_ppl_30d, total_leads_range, total_leads_30d
        )

        total_data.update(
            {
                CalculateColumnEnum.name.name: (total_title, total_action.name),
                CalculateColumnEnum.leads.name: total_leads,
                CalculateColumnEnum.income.name: total_income,
                CalculateColumnEnum.ipl.name: total_ipl,
                CalculateColumnEnum.expenses.name: total_expenses,
                CalculateColumnEnum.profit.name: total_profit,
                CalculateColumnEnum.ppl.name: total_ppl,
                CalculateColumnEnum.cpl.name: total_cpl,
                CalculateColumnEnum.ppl_range.name: (total_ppl, total_ppl_range.value),
                CalculateColumnEnum.ppl_30d.name: (0, total_ppl_30d.value),
                CalculateColumnEnum.leads_range.name: (
                    total_leads,
                    total_leads_range.value,
                ),
                CalculateColumnEnum.leads_30d.name: (0, total_leads_30d.value),
                CalculateColumnEnum.action.name: (
                    total_action.value,
                    total_action.name,
                ),
            }
        )

        if "download" in request.args.keys():
            target = tempfile.NamedTemporaryFile(suffix=".xlsx")
            workbook = Workbook(target.name)
            worksheet = workbook.add_worksheet("Статистика")
            worksheet.write_row(0, 0, self.extras.get("columns").values())
            index = -1
            for index, row in calc.data.iterrows():
                item = list(row.values)
                item[0] = item[0][0]
                item[8] = item[8][1]
                item[9] = item[9][1]
                item[10] = item[10][1]
                item[11] = item[11][1]
                item[12] = item[12][1]
                worksheet.write_row(index + 1, 0, item)
            total_values = list(total_data.values())
            total_values[0] = total_values[0][0]
            total_values[8] = total_values[8][1]
            total_values[9] = total_values[9][1]
            total_values[10] = total_values[10][1]
            total_values[11] = total_values[11][1]
            total_values[12] = total_values[12][1]
            worksheet.write_row(index + 2, 0, total_values)
            worksheet.autofilter("A1:M1")
            workbook.close()
            return send_file(
                workbook.filename,
                download_name=f"statistics.xlsx",
                as_attachment=True,
            )

        url = urlparse(request.url)
        qs = dict(parse_qsl(url.query))
        link = f"{url.scheme}://{url.netloc}{url.path}"
        details = qs.pop("details", None)
        if details not in list(map(lambda item: item[2], calc.data.name.unique())):
            details = None
        details_extra, details_leads = self.get_details(details)
        if qs:
            link = f"{link}?{urlencode(qs)}"

        self.context("filters", self.filters)
        self.context("extras", self.extras)
        self.context("data", calc.data)
        self.context("total", pandas.Series(total_data))
        self.context("url", link)
        self.context("qs_tail", "&" if qs else "?")
        self.context("details", details)
        self.context("details_extra", details_extra)
        self.context("details_leads", details_leads)

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
        print(params)
        try:
            response = vk("ads.createAds", **params)
            print(response)
        except Exception as error:
            self.context("error", error)
        return self.render()


class VKXlsxAdsView(MethodView):
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
            download_name=f'analytic-nu-vk-ads-{datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%f")}.xlsx',
            as_attachment=True,
        )


class VKXlsxLeadsView(MethodView):
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
            download_name=f'analytic-nu-leads-{datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%f")}.xlsx',
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
        # print(len(list(leads["traffic_channel"].unique())))
        # print(len(list(ads["target_url"].unique())))
        print(ads[ads.target_url.isin(leads.traffic_channel.unique())])
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
        self, date: datetime, leads: pandas.DataFrame
    ) -> pandas.DataFrame:
        if date:
            leads = leads[leads.created_at >= datetime.strptime(date, "%Y-%m-%d")]
        return leads

    def _filter_date_to(
        self, date: datetime, leads: pandas.DataFrame
    ) -> pandas.DataFrame:
        if date:
            leads = leads[
                leads.created_at
                < (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1))
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
                datetime.strptime(filters.get("date_from"), "%Y-%m-%d")
                .replace(tzinfo=timezone.utc)
                .timestamp()
                * 1000
            )
        else:
            date_from = min(list(map(lambda item: min(item.get("dates")), output)))

        if filters.get("date_to"):
            date_to = int(
                datetime.strptime(filters.get("date_to"), "%Y-%m-%d")
                .replace(tzinfo=timezone.utc)
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
