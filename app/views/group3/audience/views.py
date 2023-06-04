import pandas

from typing import Dict, Callable

from flask import current_app, request

from app.views.base import FormView
from app.utils import (
    audience_tables_by_date_calculate,
    audience_type_result_calculate,
    audience_type_percent_result_calculate,
)

from . import forms


class AudienceBaseView(FormView):
    template_name = "group3/audience/index.html"
    pickle_file: str

    def get(self, *args, **kwargs):
        self.context("tab", request.args.get("tab"))
        return super().get(*args, **kwargs)

    def form_valid(self):
        pickle_app = current_app.app("pickle")

        date_start = self.form.serializer.date_start
        date_end = self.form.serializer.date_end
        utm = dict(zip(self.form.serializer.utm, self.form.serializer.utm_value))

        if date_start or date_end or utm:
            table = pickle_app.load("leads")
            table.created_at = pandas.to_datetime(table.created_at).dt.date

            if date_start:
                table = table[table.created_at >= date_start]
            if date_end:
                table = table[table.created_at <= date_end]

            for item in utm.items():
                table = table[
                    table["traffic_channel"].str.contains("=".join(item), case=False)
                ]

            if len(table) == 0:
                self.form.set_error("Нет данных для заданного периода")
                tables = None
            else:
                table = audience_tables_by_date_calculate(table)
                tables = self.calculate(table)

        else:
            tables = pickle_app.load(self.pickle_file)

        self.context("tables", tables)

    def form_invalid(self):
        self.context("tables", None)

    def calculate(self, table: pandas.DataFrame) -> Dict[str, pandas.DataFrame]:
        raise NotImplementedError()


class AbsoluteView(AudienceBaseView):
    title = "Типы ЦА по дням: абсолютные"
    form_class = forms.AbsoluteForm
    pickle_file = "audience_type"

    def calculate(self, table: pandas.DataFrame) -> Dict[str, pandas.DataFrame]:
        return audience_type_result_calculate(table)


class RelativeView(AudienceBaseView):
    title = "Типы ЦА по дням: относительные"
    form_class = forms.RelativeForm
    pickle_file = "audience_type_percent"

    def calculate(self, table: pandas.DataFrame) -> Dict[str, pandas.DataFrame]:
        return audience_type_percent_result_calculate(table)
