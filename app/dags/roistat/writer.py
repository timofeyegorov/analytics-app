import pandas
import pickle

from pathlib import Path
from typing import Any
from transliterate import slugify


class RoistatWriter:
    path: Path

    def __init__(self, path: Path):
        self.path = path

    def __call__(self, method: str, data: Any):
        with open(self.path / f"{method}.pkl", "wb") as file_ref:
            getattr(self, slugify(method, "ru"))(file_ref, data)

    def analytics(self, file, analytics: pandas.DataFrame):
        data = (
            analytics.drop_duplicates(
                subset=[
                    "date",
                    "marker_level_1",
                    "marker_level_2",
                    "marker_level_3",
                    "marker_level_4",
                    "marker_level_5",
                    "marker_level_6",
                    "marker_level_7",
                ],
                keep="last",
                ignore_index=True,
            )
            .sort_values(
                by=[
                    "date",
                    "marker_level_1",
                    "marker_level_2",
                    "marker_level_3",
                    "marker_level_4",
                    "marker_level_5",
                    "marker_level_6",
                    "marker_level_7",
                ]
            )
            .reset_index(drop=True)
        )
        pickle.dump(data, file)

    def statistics(self, file, statistics: pandas.DataFrame):
        data = statistics.reset_index(drop=True)
        pickle.dump(data, file)
