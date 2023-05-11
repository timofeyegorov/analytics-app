#!/usr/bin/env python

import pickle

from pathlib import Path
from datetime import datetime, time, timedelta
from argparse import ArgumentParser

from config import RESULTS_FOLDER

from app.analytics import pickle_loader
from app.dags.utils import RoistatDetectLevels


class ArgumentParserService(ArgumentParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_argument(
            "-df",
            "--date-from",
            type=lambda value: datetime.strptime(value, "%d.%m.%Y").date(),
            required=True,
            help="From date. Format %%d.%%m.%%Y",
        )
        self.add_argument(
            "-dt",
            "--date-to",
            type=lambda value: datetime.strptime(value, "%d.%m.%Y").date(),
            required=True,
            help="To date. Format %%d.%%m.%%Y",
        )

    @property
    def args(self):
        args = self.parse_args()
        assert (
            args.date_from <= args.date_to
        ), "From date must be less or equivalent date to"
        return args


def run():
    statistics = pickle_loader.roistat_statistics
    columns = ["account", "campaign", "group", "ad"]
    args = ArgumentParserService().args
    leads = pickle_loader.roistat_leads
    leads["d"] = leads["date"].apply(lambda item: item.date())
    leads = leads[(leads["d"] >= args.date_from) & (leads["d"] <= args.date_to)]
    leads = leads.loc[:, leads.columns != "d"]
    leads.rename(columns={"url": "traffic_channel"}, inplace=True)
    for index, lead in leads.iterrows():
        stats = statistics[statistics.date == lead.date]
        levels = RoistatDetectLevels(lead, stats)
        leads.loc[index, columns] = [
            levels.account,
            levels.campaign,
            levels.group,
            levels.ad,
        ]
    leads.rename(columns={"traffic_channel": "url"}, inplace=True)

    source = pickle_loader.roistat_leads
    source.loc[leads.index, columns] = leads[columns].values
    with open(Path(RESULTS_FOLDER, "roistat_leads.pkl"), "wb") as file_ref:
        pickle.dump(source, file_ref)


if __name__ == "__main__":
    run()
