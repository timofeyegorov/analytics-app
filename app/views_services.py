import pandas

from typing import Dict, Any
from pathlib import Path
from pydantic import BaseModel

from flask import render_template
from flask.views import MethodView
from flask.typing import ResponseReturnValue

from config import DATA_FOLDER


WEEK_FOLDER = Path(DATA_FOLDER) / "week"


class FilteringBaseData(BaseModel):
    pass


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

    def get(self, *args, **kwargs):
        return self.render()


class FilteringBaseView(TemplateView):
    filters_class = FilteringBaseData
    filters: FilteringBaseData

    data_path: Path
    data: pandas.DataFrame
    extras: Dict[str, Any]

    def dispatch_request(self, *args, **kwargs) -> ResponseReturnValue:
        with open(self.data_path, "rb") as file_ref:
            self.data = pandas.read_pickle(file_ref)
        return super().dispatch_request(*args, **kwargs)

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

    def get(self):
        self.context("data", self.data.to_html())
        return super().get()


class ServicesSourcesWeekPaymentsView(FilteringBaseView):
    template_name = "services/sources/week/payments/index.html"
    title = "Аналитика по оплатам [Все оплаты]"
    data_path = WEEK_FOLDER / "source_payments.pkl"


class ServicesSourcesWeekExpensesView(FilteringBaseView):
    template_name = "services/sources/week/expenses/index.html"
    title = "Расходы"
    data_path = WEEK_FOLDER / "expenses_count.pkl"
