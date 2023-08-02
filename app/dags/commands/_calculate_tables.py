import os
import pytz
import pandas
import pickle
import datetime

from time import sleep
from typing import Dict
from pathlib import Path

from app import db
from app.data import StatisticsRoistatPackageEnum, PACKAGES_COMPARE
from app.analytics import pickle_loader
from app.database import models
from app.plugins.ads import roistat
from app.dags.data import roistat_statistics_columns, roistat_leads_columns
from app.dags.utils import RoistatDetectLevels

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


def roistat_statistics():
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
    with open(Path(RESULTS_FOLDER, "roistat_statistics.pkl"), "wb") as file_ref:
        pickle.dump(statistics, file_ref)


def roistat_leads():
    columns = ["account", "campaign", "group", "ad"]
    try:
        statistics = pickle_loader.roistat_statistics
    except Exception:
        statistics = pandas.DataFrame(columns=roistat_statistics_columns)
    try:
        leads = pickle_loader.leads_np
        os.remove(f"{RESULTS_FOLDER}/leads_np.pkl")
    except FileNotFoundError:
        return
    for column in columns:
        leads[column] = ""
    for index, lead in leads.iterrows():
        stats = statistics[statistics.date == lead.date]
        levels = RoistatDetectLevels(lead, stats)
        leads.loc[index, columns] = [
            levels.account,
            levels.campaign,
            levels.group,
            levels.ad,
        ]

    leads = leads[
        [
            "traffic_channel",
            "quiz_answers1",
            "quiz_answers2",
            "quiz_answers3",
            "quiz_answers4",
            "quiz_answers5",
            "quiz_answers6",
            "turnover_on_lead",
            "target_class",
            "email",
            "phone",
            "date",
            "channel_expense",
            "utm_source",
            "utm_medium",
            "utm_campaign",
            "utm_content",
            "utm_term",
        ]
        + columns
    ].rename(
        columns={
            "traffic_channel": "url",
            "quiz_answers1": "qa1",
            "quiz_answers2": "qa2",
            "quiz_answers3": "qa3",
            "quiz_answers4": "qa4",
            "quiz_answers5": "qa5",
            "quiz_answers6": "qa6",
            "turnover_on_lead": "ipl",
            "channel_expense": "expenses",
        }
    )
    try:
        data = pickle_loader.roistat_leads
    except Exception:
        data = pandas.DataFrame(columns=roistat_leads_columns)
    leads = (
        pandas.concat([data, leads])
        .drop_duplicates(keep="last", ignore_index=True)
        .reset_index(drop=True)
    )
    with open(Path(RESULTS_FOLDER, "roistat_leads.pkl"), "wb") as file_ref:
        pickle.dump(leads, file_ref)


def roistat_update_levels():
    statistics = pickle_loader.roistat_statistics
    columns = ["account", "campaign", "group", "ad"]
    date_to = datetime.datetime.now()
    date_from = date_to - datetime.timedelta(weeks=1)
    leads = pickle_loader.roistat_leads
    leads["d"] = leads["date"].apply(lambda item: item.date())
    leads = leads[(leads["d"] >= date_from.date()) & (leads["d"] <= date_to.date())]
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
