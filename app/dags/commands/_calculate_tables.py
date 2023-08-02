import os
import pytz
import pandas
import pickle
import datetime

from time import sleep
from typing import Dict

from app import db
from app.data import StatisticsRoistatPackageEnum, PACKAGES_COMPARE
from app.analytics import pickle_loader
from app.database import models
from app.plugins.ads import roistat
from app.dags.data import roistat_statistics_columns

from config import RESULTS_FOLDER

from . import utils


def roistat_to_db(date_from: datetime.date, date_to: datetime.date):
    while date_from <= date_to:
        print("Collect analytic:", date_from)
        current_datetime = datetime.datetime.combine(
            date_from, datetime.datetime.min.time()
        )
        time_now = datetime.datetime.now()
        response = roistat(
            "analytics",
            dimensions=[
                "marker_level_1",
                "marker_level_2",
                "marker_level_3",
                "marker_level_4",
                "marker_level_5",
                "marker_level_6",
                "marker_level_7",
            ],
            period={
                "from": current_datetime.strftime("%Y-%m-%dT00:00:00+0300"),
                "to": current_datetime.strftime("%Y-%m-%dT23:59:59.9999+0300"),
            },
            metrics=["visitsCost", "leadCount", "visitCount", "impressions"],
            interval="1d",
        )
        for item_data in response.get("data"):
            models.Roistat.query.filter_by(date=date_from).delete()
            db.session.commit()
            analytics_date = []
            for item in item_data.get("items"):
                levels = utils.roistat_get_levels(item.get("dimensions"))
                metrics = utils.roistat_get_metrics(item.get("metrics"), ["visitsCost"])
                package = levels.pop("package")
                package_instance = models.RoistatPackages.get_or_create(
                    package, StatisticsRoistatPackageEnum[package].value
                )
                levels_instances = {}
                for level in range(1, 8):
                    levels_instances[level] = models.RoistatLevels.get_or_create(
                        levels.get(f"marker_level_{level}") or "undefined",
                        levels.get(f"marker_level_{level}_title"),
                        level,
                    )
                analytics_date.append(
                    models.Roistat(
                        **{
                            "visits_cost": metrics.get("visitsCost", 0),
                            "date": date_from,
                            "package_id": package_instance.id,
                            **dict(
                                [
                                    (f"level_{index}_id", item.id)
                                    for index, item in levels_instances.items()
                                ]
                            ),
                        }
                    )
                )
            db.session.add_all(analytics_date)
            db.session.commit()
            print("---", datetime.datetime.now() - time_now)
            sleep(1)

        date_from = date_from + datetime.timedelta(days=1)


def statistics_processing():
    def analytics_row_to_dict(
        rel,
        row,
        package: models.RoistatPackages,
        levels: Dict[int, models.RoistatLevels],
    ):
        value = row.__dict__
        output = {}
        for field, target in rel.items():
            value_field = value.get(field)
            if field.startswith("package_"):
                output[target] = package.name
            elif field.startswith("level_"):
                output[target] = levels.get(value_field).name
                output[f"{target}_title"] = levels.get(value_field).title
            else:
                output[target] = value_field
        return output

    tz = pytz.timezone("Europe/Moscow")
    try:
        statistics = pickle_loader.roistat_statistics
    except Exception:
        statistics = pandas.DataFrame(columns=roistat_statistics_columns)
    if "db" not in list(statistics.columns):
        statistics["db"] = pandas.NA
    roistat_ids = [
        row[0] for row in models.Roistat.query.with_entities(models.Roistat.id).all()
    ]

    statistics.drop(statistics[~statistics["db"].isin(roistat_ids)].index, inplace=True)
    exclude_ids = list(statistics["db"])
    query = models.Roistat.query
    if exclude_ids:
        query = query.filter(models.Roistat.id.notin_(exclude_ids))
    levels = dict([(row.id, row) for row in models.RoistatLevels.query.all()])
    time_now = datetime.datetime.now()
    for package in models.RoistatPackages.query.all():
        rel = PACKAGES_COMPARE.get(package.name)
        if not rel:
            continue
        rows = query.filter_by(package_id=package.id).limit(500000).all()
        print(f"--- Update {package.name}: {len(rows)}")
        data_package = pandas.concat(
            [
                pandas.DataFrame(columns=roistat_statistics_columns),
                pandas.DataFrame.from_dict(
                    [analytics_row_to_dict(rel, row, package, levels) for row in rows]
                ),
            ]
        )
        data_package["account"].fillna("undefined", inplace=True)
        data_package["campaign"].fillna("undefined", inplace=True)
        data_package["group"].fillna("undefined", inplace=True)
        data_package["ad"].fillna("undefined", inplace=True)
        data_package["account_title"].fillna("Undefined", inplace=True)
        data_package["campaign_title"].fillna("Undefined", inplace=True)
        data_package["group_title"].fillna("Undefined", inplace=True)
        data_package["ad_title"].fillna("Undefined", inplace=True)
        data_package["date"] = data_package["date"].apply(
            lambda item: tz.localize(
                datetime.datetime.combine(item, datetime.datetime.min.time())
            )
        )
        statistics = pandas.concat([statistics, data_package])
    statistics["db"] = statistics["db"].apply(int)
    statistics.reset_index(drop=True, inplace=True)
    print("--- Execution time:", datetime.datetime.now() - time_now)
    with open(os.path.join(RESULTS_FOLDER, "roistat_statistics.pkl"), "wb") as file_ref:
        pickle.dump(statistics, file_ref)
