import pickle
import pandas

from typing import Any
from pathlib import Path
from transliterate import slugify


class RoistatReader:
    path: Path

    def __init__(self, path: Path):
        self.path = path

    def __call__(self, method: str) -> Any:
        try:
            with open(self.path / f"{method}.pkl", "rb") as file_ref:
                data = pickle.load(file_ref)
        except FileNotFoundError:
            data = None

        try:
            return getattr(self, slugify(method, "ru"))(data)
        except AttributeError:
            return data

    def analytics(self, analytics: pandas.DataFrame = None):
        if analytics is None:
            analytics = pandas.DataFrame(
                columns=[
                    "package",
                    "marker_level_1",
                    "marker_level_2",
                    "marker_level_3",
                    "marker_level_4",
                    "marker_level_5",
                    "marker_level_6",
                    "marker_level_7",
                    "marker_level_1_title",
                    "marker_level_2_title",
                    "marker_level_3_title",
                    "marker_level_4_title",
                    "marker_level_5_title",
                    "marker_level_6_title",
                    "marker_level_7_title",
                    "visitsCost",
                    "date",
                ]
            )
        return analytics

    def statistics(self, statistics: pandas.DataFrame = None):
        if statistics is None:
            statistics = pandas.DataFrame(
                columns=[
                    "date",
                    "package",
                    "account",
                    "campaign",
                    "group",
                    "ad",
                    "package_title",
                    "account_title",
                    "campaign_title",
                    "group_title",
                    "ad_title",
                    "expenses",
                ]
            )
        return statistics
