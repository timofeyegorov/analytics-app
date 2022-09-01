import json
import pandas

from typing import List, Dict, Any
from datetime import datetime, timedelta, timezone
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


class VKCreateAdView(TemplateView):
    template_name = "vk/create-ad.html"
    title = "Создать объявление в ВК"

    def post(self):
        params = {
            "account_id": request.form.get("account_id"),
            "data": json.dumps(
                {
                    "campaign_id": "",
                }
            ),
        }
        # response = vk("ads.createAds", **params)
        # print(response)
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
            print("sssss")
        self.data = {
            "accounts": accounts,
            "campaigns": campaigns,
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
