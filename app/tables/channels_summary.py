import os
import numpy as np
import pickle as pkl

from enum import Enum
from config import RESULTS_FOLDER
from pandas import DataFrame


class StatusColor(str, Enum):
    high = "8bc34a"
    middle = "ffeb3b"
    low = "f44336"


class Action(str, Enum):
    suppose = "Гипотезы"
    disable = "Отключить"
    pending = "Ждем статистику"


class DataFrameTable(DataFrame):
    _columns: list = [
        "Канал",
        "Лидов",
        "Оборот*",
        "Оборот на лида",
        "Трафик",
        "Остальное",
        "Прибыль",
        "Прибыль на лида",
        "ROI",
        "Маржинальность",
        "Цена лида",
        "Плюсовость за период",
        "Плюсовость за месяц",
        "Активность за период",
        "Активность за месяц",
        "Действие",
    ]

    def __init__(self, *args, **kwargs):
        kwargs.update({"columns": self._columns})
        if not kwargs.get("data"):
            kwargs.update({"data": []})
        super().__init__(*args, **kwargs)


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


detect_positive = DetectPositive()
detect_activity = DetectActivity()


class Calculate:
    _data: DataFrame = None
    _data_month = None
    _column: str = None

    _name: str = None
    _leads: int = None
    _turnover: int = None
    _turnover_on_lead: int = None
    _traffic: int = None
    _expenses: int = None
    _profit: int = None
    _profit_on_lead: int = None
    _roi: int = None
    _marginality: int = None
    _lead_price: int = None
    _positive_period: StatusColor = None
    _positive_month: StatusColor = None
    _activity_period: StatusColor = None
    _activity_month: StatusColor = None

    def __init__(self, data: DataFrame, name: str, column: str = None, data_month=None):
        self._data = data
        self._name = name
        self._column = column
        self._data_month = data_month

    def __iter__(self):
        return iter(self.list_columns)

    @property
    def list_columns(self) -> list:
        return [
            self.name,
            self.leads,
            self.turnover,
            self.turnover_on_lead,
            self.traffic,
            self.expenses,
            self.profit,
            self.profit_on_lead,
            self.roi,
            self.marginality,
            self.lead_price,
            ("background-color", f"#{self.positive_period.value}")
            if self.positive_period
            else "",
            ("background-color", f"#{self.positive_month.value}")
            if self.positive_month
            else "",
            ("background-color", f"#{self.activity_period.value}")
            if self.activity_period
            else "",
            ("background-color", f"#{self.activity_month.value}")
            if self.activity_month
            else "",
            self.action.value if self.action else "",
        ]

    @property
    def name(self) -> str:
        return self._name

    @property
    def turnover_on_lead(self) -> int:
        if self._turnover_on_lead is None:
            self._turnover_on_lead = (
                round(self.turnover / self.leads) if self.leads else 0
            )
        return self._turnover_on_lead

    @property
    def expenses(self) -> int:
        if self._expenses is None:
            self._expenses = round(self.leads * 250 + self.turnover * 0.35)
        return self._expenses

    @property
    def profit(self) -> int:
        if self._profit is None:
            self._profit = round(self.turnover - self.traffic - self.expenses)
        return self._profit

    @property
    def profit_on_lead(self) -> int:
        if self._profit_on_lead is None:
            self._profit_on_lead = round(self.profit / self.leads) if self.leads else 0
        return self._profit_on_lead

    @property
    def roi(self) -> int:
        if self._roi is None:
            self._roi = (
                round((self.turnover / (self.traffic + self.expenses) - 1) * 100)
                if (self.traffic + self.expenses) != 0
                else 0
            )
        return self._roi

    @property
    def marginality(self) -> int:
        if self._marginality is None:
            self._marginality = (
                round(
                    (self.profit_on_lead / 100)
                    / (1 + (self.profit_on_lead / 100))
                    * 100
                )
                if (1 + (self.profit_on_lead / 100)) != 0
                else 0
            )
        return self._marginality

    @property
    def lead_price(self) -> int:
        if self._lead_price is None:
            self._lead_price = round(self.traffic / self.leads)
        return self._lead_price


class CalculateTotal(Calculate):
    @property
    def leads(self) -> int:
        if self._leads is None:
            self._leads = self._data.shape[0]
        return self._leads

    @property
    def turnover(self) -> int:
        if self._turnover is None:
            self._turnover = round(self._data["turnover_on_lead"].sum())
        return self._turnover

    @property
    def traffic(self) -> int:
        if self._traffic is None:
            self._traffic = round(
                self._data["channel_expense"].sum() * 1.2
                + self._data[self._data["channel_expense"] == 0].shape[0] * 400 * 1.2
            )
        return self._traffic

    @property
    def positive_period(self) -> StatusColor:
        return

    @property
    def positive_month(self) -> StatusColor:
        return

    @property
    def activity_period(self) -> StatusColor:
        return

    @property
    def activity_month(self) -> StatusColor:
        return

    @property
    def action(self) -> Action:
        return


class CalculateTarget(Calculate):
    @property
    def leads(self) -> int:
        if self._leads is None:
            self._leads = self._data[self._data[self._column] == self.name].shape[0]
        return self._leads

    @property
    def turnover(self) -> int:
        if self._turnover is None:
            self._turnover = round(
                self._data[self._data[self._column] == self.name][
                    "turnover_on_lead"
                ].sum()
            )
        return self._turnover

    @property
    def traffic(self) -> int:
        if self._traffic is None:
            self._traffic = round(
                self._data[self._data[self._column] == self.name][
                    "channel_expense"
                ].sum()
                * 1.2
                + self._data[
                    (self._data[self._column] == self.name)
                    & (self._data["channel_expense"] == 0)
                ].shape[0]
                * 400
                * 1.2
            )
        return self._traffic

    @property
    def positive_period(self) -> StatusColor:
        if self._positive_period is None:
            self._positive_period = detect_positive(self.profit_on_lead)
        return self._positive_period

    @property
    def positive_month(self) -> StatusColor:
        if not self._data_month:
            self._positive_month = ""
        if self._positive_month is None:
            self._positive_month = detect_positive(self._data_month.profit_on_lead)
        return self._positive_month

    @property
    def activity_period(self) -> StatusColor:
        if self._activity_period is None:
            self._activity_period = detect_activity(self.leads)
        return self._activity_period

    @property
    def activity_month(self) -> StatusColor:
        if not self._data_month:
            self._activity_month = ""
        if self._activity_month is None:
            self._activity_month = detect_activity(self._data_month.leads)
        return self._activity_month

    @property
    def action(self) -> Action:
        if self.activity_period == StatusColor.high:
            if self.positive_period in (StatusColor.high, StatusColor.middle):
                return Action.suppose
            elif self.positive_period == StatusColor.low:
                return Action.disable
        elif self.activity_period in (StatusColor.middle, StatusColor.low):
            if self.activity_month == StatusColor.high:
                if self.positive_month in (StatusColor.high, StatusColor.middle):
                    return Action.suppose
                elif self.positive_month == StatusColor.low:
                    return Action.disable
            elif self.activity_month in (StatusColor.middle, StatusColor.low):
                return Action.pending
        return


def calculate_channels_summary(
    df, column_unique: str = "trafficologist", data_month: dict = None
):
    if not data_month:
        data_month = {}

    # Исходный режим подсчета, считает данные по трафикологам
    iterable_variable = df[column_unique].unique()

    values = [list(CalculateTotal(df, "СУММА:"))]
    for name in iterable_variable:
        values.append(
            list(
                CalculateTarget(
                    df,
                    name,
                    column_unique,
                    CalculateTarget(data_month.get(name), name, column_unique),
                )
            )
        )

    return {"Источники - сводная таблица": DataFrameTable(data=values)}
