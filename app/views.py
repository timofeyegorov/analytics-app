import pandas

from typing import Tuple, List, Dict
from datetime import datetime, timedelta, timezone

from flask import request, render_template
from flask.views import MethodView

from app.analytics.pickle_load import PickleLoader


pickle_loader = PickleLoader()


class TemplateView(MethodView):
    template_name: str
    title: str = ""

    def get_template_name(self) -> str:
        return self.template_name

    def render(self, context: dict = None):
        if context is None:
            context = {}
        context.update(
            {
                "title": self.title,
            }
        )
        return render_template(self.get_template_name(), **context)


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

        return self.render(
            {
                "choices": choices,
                "filters": filters,
                "date_range": [date_from, date_to],
                "data": output,
            }
        )
