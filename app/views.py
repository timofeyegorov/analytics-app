import json
import pandas
import requests
import tempfile

from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from urllib.parse import parse_qsl

from flask import request, render_template
from flask.views import MethodView

from app.plugins.ads import vk
from app.analytics.pickle_load import PickleLoader
from app.dags.vk import reader as vk_reader, data as vk_data


pickle_loader = PickleLoader()


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

    def get(self):
        return self.render()


class VKStatisticsView(TemplateView):
    template_name = "vk/statistics.html"
    title = "Статистика объявлений в ВК"

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
    def stats(self) -> pandas.DataFrame:
        stats = vk_reader("ads.getStatistics")
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
        return stats

    def get_args(self) -> Dict[str, Any]:
        return {
            "group_by": request.args.get("group_by", "ad_id"),
            "account": request.args.get("account_id") or None,
            "client": request.args.get("client_id") or None,
            "campaign": request.args.get("campaign_id") or None,
        }

    def set_context(self, data: Dict[str, Any]):
        for name, value in data.items():
            self.context(name, value)

    def form_context_add(self, **kwargs):
        self.set_context(
            {
                "fields": {
                    "group_by": kwargs.get("group_by", ""),
                }
            }
        )

    def get(self):
        args = self.get_args()

        self.form_context_add(**args)

        self.context("ads", self.ads)
        self.context("accounts", self.accounts)
        self.context("clients", self.clients)
        self.context("campaigns", self.campaigns)
        self.context("ads", self.ads)

        stats = self.stats
        self.context("stats", stats)

        return super().get()


class VKCreateAdView(TemplateView):
    template_name = "vk/create-ad.html"
    title = "Создать объявление в ВК"

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
        form_context = {
            "account_id": account_id,
            "campaign_id": campaign_id,
            "cost_type": cost_type,
            "ad_format": ad_format,
            "link_url": link_url,
            "title": title,
            "description": description,
        }
        try:
            photo = request.form.get(
                "photo", self.get_photo_url(request.files.get("photo_file"), ad_format)
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
                    }
                ]
            ),
        }
        try:
            response = vk("ads.createAds", **params)
            print(response)
        except Exception as error:
            self.context("error", error)
        return self.render()


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
        self.data = {
            "accounts": accounts,
            "campaigns": campaigns,
            "cost_type": cost_types,
            "ad_format": ad_formats,
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
