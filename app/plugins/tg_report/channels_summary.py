import numpy
import pickle
import pandas
import urllib
import requests

from typing import Tuple, List, Dict, Optional
from pathlib import Path
from httplib2 import Http
from datetime import datetime, timedelta
from apiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials

from config import config, CREDENTIALS_FILE, RESULTS_FOLDER

from app.tables.channels_summary import calculate_channels_summary


def get_table_one_campaign(campaign, column_unique, table, date_to: datetime):
    table.created_at = pandas.to_datetime(table.created_at).dt.normalize()
    table = table[table[column_unique] == campaign]
    table = table[table.created_at > date_to - timedelta(days=30)]
    table = table[table.created_at <= date_to]
    return table


class ChannelsSummary:
    _data: pandas.DataFrame = None

    def __init__(self, daterange: Tuple[datetime, datetime]):
        with open(Path(RESULTS_FOLDER) / "leads.pkl", "rb") as leads_ref:
            table = pickle.load(leads_ref)

        table.created_at = pandas.to_datetime(table.created_at).dt.normalize()
        table = table[table.created_at > daterange[0]]
        table = table[table.created_at <= daterange[1]]

        if not len(table):
            return

        column_unique = "trafficologist"

        with open(Path(RESULTS_FOLDER) / "leads.pkl", "rb") as leads_ref:
            table_month_data = pickle.load(leads_ref)

        data_month = {}
        for campaign in table[column_unique].unique():
            data_month[campaign] = get_table_one_campaign(
                campaign,
                column_unique,
                table_month_data.copy(True),
                date_to=daterange[1],
            )

        self._data = calculate_channels_summary(
            table, column_unique=column_unique, data_month=data_month
        )

    @property
    def data(self) -> Optional[pandas.DataFrame]:
        return self._data

    def send(self):
        raise NotImplementedError(
            f"Необходимо определить метод 'send' в классе '{self.__class__}'"
        )


class TGReportChannelsSummary(ChannelsSummary):
    _groups: Dict[str, List[str]] = {}

    def __init__(self, *args, **kwargs):
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            CREDENTIALS_FILE,
            [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ],
        )
        httpAuth = credentials.authorize(Http())
        service = discovery.build("sheets", "v4", http=httpAuth)
        response = (
            service.spreadsheets()
            .values()
            .get(
                spreadsheetId="1oQuX7eA5eijO98Ht_wO9O1noR20yJB73YiKfAnvQQ1g",
                range="Трафикологи - Телеграм-группы",
                majorDimension="ROWS",
            )
            .execute()
        )
        groups = response["values"][0]
        channels = numpy.transpose(
            numpy.array(
                [
                    channel
                    + [None] * (max(map(len, response["values"][1:])) - len(channel))
                    for channel in response["values"][1:]
                ]
            )
        )
        for index, group in enumerate(groups):
            self._groups[group] = list(filter(None, channels[index]))
        super().__init__(*args, **kwargs)

    @property
    def groups(self) -> Dict[str, List[str]]:
        return self._groups

    def send(self):
        if not self.data:
            return

        data = self.data[list(self.data.keys())[0]]
        for group, channels in self._groups.items():
            message = ""
            for channel in data[data["Канал"].isin(channels)].iterrows():
                message += f"""\n\n{channel[1]["Канал"]}
=======================================
Лидов {channel[1]["Лидов"]:>33}
Оборот* {channel[1]["Оборот*"]:>31}
Оборот на лида {channel[1]["Оборот на лида"]:>24}
Трафик {channel[1]["Трафик"]:>32}
Остальное {channel[1]["Остальное"]:>29}
Прибыль {channel[1]["Прибыль"]:>31}
Прибыль на лида {channel[1]["Прибыль на лида"]:>23}
ROI {channel[1]["ROI"]:>35}
Маржинальность {channel[1]["Маржинальность"]:>24}
Цена лида {channel[1]["Цена лида"]:>29}
Плюсовость за период {channel[1]["Плюсовость за период"][0]:>18}
Плюсовость за месяц {channel[1]["Плюсовость за месяц"][0]:>19}
Активность за период {channel[1]["Активность за период"][0]:>18}
Активность за месяц {channel[1]["Активность за месяц"][0]:>19}
---------------------------------------
Действие {channel[1]["Действие"][0]:>30}
=======================================\n"""
            if message:
                requests.get(
                    f'https://api.telegram.org/bot{config.get("tg_bot").get("api_token")}/sendMessage?chat_id=@{group}&text=<code>{urllib.parse.quote(message)}</code>&parse_mode=HTML'
                )
