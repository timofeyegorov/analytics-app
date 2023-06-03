import pandas

from time import mktime

from flask import current_app, request

from analytic.views.base import FormView
from analytic.utils import (
    segments_calculate,
    turnover_calculate,
    clusters_calculate,
    landings_calculate,
)

from . import forms


class SegmentsView(FormView):
    template_name = "group1/segments/index.html"
    title = "Сегменты"
    form_class = forms.SegmentsForm

    def form_valid(self):
        pickle_app = current_app.ext("pickle")

        date_start = self.form.serializer.date_start
        date_end = self.form.serializer.date_end

        if date_start or date_end:
            table = pickle_app.load("leads")
            table.created_at = pandas.to_datetime(table.created_at).dt.date

            if date_start:
                table = table[table.created_at >= date_start]
            if date_end:
                table = table[table.created_at <= date_end]

            if len(table) == 0:
                self.form.set_error("Нет данных для заданного периода")
                tables = None
            else:
                tables = segments_calculate(table)

        else:
            tables = pickle_app.load("segments")

        self.context("tables", tables)

    def form_invalid(self):
        self.context("tables", None)


class TurnoverView(FormView):
    template_name = "group1/turnover/index.html"
    title = "Обороты"
    form_class = forms.TurnoverForm

    def get(self, *args, **kwargs):
        self.context("tab", request.args.get("tab"))
        return super().get(*args, **kwargs)

    def form_valid(self):
        pickle_app = current_app.ext("pickle")

        date_request_start = self.form.serializer.date_request_start
        date_request_end = self.form.serializer.date_request_end
        date_payment_start = self.form.serializer.date_payment_start
        date_payment_end = self.form.serializer.date_payment_end

        if (
            date_request_start
            or date_request_end
            or date_payment_start
            or date_payment_end
        ):
            table = pickle_app.load("leads")
            table.created_at = pandas.to_datetime(table.created_at).dt.date
            table.date_payment = pandas.to_datetime(
                table.date_payment.apply(lambda item: item if item else pandas.NA)
            ).dt.date

            if date_request_start:
                table = table[table.created_at >= date_request_start]
            if date_request_end:
                table = table[table.created_at <= date_request_end]
            if date_payment_start:
                table = table[table.date_payment >= date_payment_start]
            if date_payment_end:
                table = table[table.date_payment <= date_payment_end]

            if len(table) == 0:
                self.form.set_error("Нет данных для заданного периода")
                tables = None
            else:
                tables = turnover_calculate(table)

        else:
            tables = pickle_app.load("turnover")

        self.context("tables", tables)

    def form_invalid(self):
        self.context("tables", None)


class ClustersView(FormView):
    template_name = "group1/clusters/index.html"
    title = "Кластеры"
    form_class = forms.ClustersForm

    def get(self, *args, **kwargs):
        self.context("tab", request.args.get("tab"))
        return super().get(*args, **kwargs)

    def form_valid(self):
        pickle_app = current_app.ext("pickle")

        date_start = self.form.serializer.date_start
        date_end = self.form.serializer.date_end

        if date_start or date_end:
            table = pickle_app.load("leads")
            table.created_at = pandas.to_datetime(table.created_at).dt.date

            if date_start:
                table = table[table.created_at >= date_start]
            if date_end:
                table = table[table.created_at <= date_end]

            if len(table) == 0:
                self.form.set_error("Нет данных для заданного периода")
                tables = None
            else:
                tables = clusters_calculate(table)

        else:
            tables = pickle_app.load("clusters")

        self.context("tables", tables)
        self.context("tab", request.args.get("tab"))

    def form_invalid(self):
        self.context("tables", None)


class LandingsView(FormView):
    template_name = "group1/landings/index.html"
    title = "Лэндинги"
    form_class = forms.LandingsForm

    def form_valid(self):
        pickle_app = current_app.ext("pickle")

        date_start = self.form.serializer.date_start
        date_end = self.form.serializer.date_end

        if date_start or date_end:
            table = pickle_app.load("leads")
            table.created_at = pandas.to_datetime(table.created_at).dt.date

            if date_start:
                table = table[table.created_at >= date_start]
            if date_end:
                table = table[table.created_at <= date_end]

            if len(table) == 0:
                self.form.set_error("Нет данных для заданного периода")
                tables = None
            else:
                tables = landings_calculate(table)

        else:
            tables = pickle_app.load("landings")

        self.context("tables", tables)

    def form_invalid(self):
        self.context("tables", None)


class ChannelsView(FormView):
    template_name = "group1/channels/index.html"
    title = "Каналы"
    form_class = forms.ChannelsForm

    def form_valid(self):
        pickle_app = current_app.ext("pickle")

        leads = pickle_app.load("leads").sort_values(["created_at"])
        leads.created_at = pandas.to_datetime(leads.created_at).dt.date

        date_from = self.form.serializer.date_from
        date_to = self.form.serializer.date_to
        account = self.form.serializer.account

        if date_from:
            leads = leads[leads.created_at >= date_from]
        if date_to:
            leads = leads[leads.created_at <= date_to]

        self.form.account.update_choices(
            [("", "--- Выберите ---")]
            + list(
                map(
                    lambda item: (item, item),
                    leads.sort_values(["account"])["account"].unique(),
                )
            )
        )

        if account is not None:
            leads = leads[leads["account"] == account].groupby("utm_campaign")
        else:
            leads = leads.groupby("account")

        if not len(leads):
            self.context("date_range", [None, None])
            self.context("data", None)
            self.form.set_error("Нет данных для заданного периода")
            return

        data = list(
            map(
                lambda group: {
                    "name": group[0],
                    "dates": list(
                        map(
                            lambda item: mktime(item.timetuple()) * 1000,
                            group[1]["created_at"].unique().tolist(),
                        )
                    ),
                },
                leads,
            )
        )

        self.context(
            "date_range",
            [
                mktime(date_from.timetuple()) * 1000
                if date_from
                else min(list(map(lambda item: min(item.get("dates")), data))),
                mktime(date_to.timetuple()) * 1000
                if date_to
                else max(list(map(lambda item: max(item.get("dates")), data))),
            ],
        )
        self.context("data", data)

    def form_invalid(self):
        self.context("date_range", [None, None])
        self.context("data", None)
