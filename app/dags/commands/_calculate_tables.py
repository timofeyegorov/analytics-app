import pytz
import pandas
import pickle
import datetime

from time import sleep
from pathlib import Path

from app import db
from app.data import StatisticsRoistatPackageEnum, PACKAGES_COMPARE
from app.analytics import pickle_loader
from app.database import models
from app.plugins.ads import roistat
from app.dags.utils import RoistatDetectLevels

from config import RESULTS_FOLDER

from . import utils


def roistat_to_db(date_from: datetime.date, date_to: datetime.date):
    while date_from <= date_to:
        print("Collect analytic:", date_from)
        current_datetime = datetime.datetime.combine(date_from, datetime.time.min)
        time_now = datetime.datetime.now()
        dimensions = [
            "marker_level_1",
            "marker_level_2",
            "marker_level_3",
            "marker_level_4",
            "marker_level_5",
            "marker_level_6",
            "marker_level_7",
        ]
        period = {
            "from": current_datetime.strftime("%Y-%m-%dT00:00:00+0300"),
            "to": current_datetime.strftime("%Y-%m-%dT23:59:59.9999+0300"),
        }
        metrics = ["visitsCost", "leadCount", "visitCount", "impressions"]
        response = roistat(
            "analytics",
            dimensions=dimensions,
            period=period,
            metrics=metrics,
            interval="1d",
        )
        for item_data in response.get("data"):
            models.Roistat.query.filter_by(date=date_from).delete()
            db.session.commit()
            analytics_data = []
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
                rel = PACKAGES_COMPARE.get(package_instance.name)
                if not rel:
                    continue
                instance = models.Roistat(
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
                        **dict(
                            [
                                (
                                    f"{item[1]}_id",
                                    levels_instances.get(int(item[0][6])).id,
                                )
                                for item in dict(
                                    filter(
                                        lambda item: item[0].startswith("level_"),
                                        rel.items(),
                                    )
                                ).items()
                            ]
                        ),
                    }
                )
                analytics_data.append(instance)
            if analytics_data:
                db.session.add_all(analytics_data)
                db.session.commit()
            print("---", datetime.datetime.now() - time_now)
            sleep(1)

        date_from = date_from + datetime.timedelta(days=1)

    print("Save dataframes")
    time_now = datetime.datetime.now()
    data = pandas.DataFrame(
        models.Roistat.query.with_entities(
            models.Roistat.date,
            models.Roistat.visits_cost,
            models.Roistat.package_id,
            models.Roistat.account_id,
            models.Roistat.campaign_id,
            models.Roistat.group_id,
            models.Roistat.ad_id,
        ).all(),
        columns=[
            "date",
            "expenses",
            "package",
            "account",
            "campaign",
            "group",
            "ad",
        ],
    )
    tz = pytz.timezone("Europe/Moscow")
    data["date"] = data["date"].apply(
        lambda item: tz.localize(datetime.datetime.combine(item, datetime.time.min))
    )
    data["package"] = data["package"].fillna(0).apply(int)
    data["account"] = data["account"].fillna(0).apply(int)
    data["campaign"] = data["campaign"].fillna(0).apply(int)
    data["group"] = data["group"].fillna(0).apply(int)
    data["ad"] = data["ad"].fillna(0).apply(int)
    with open(Path(RESULTS_FOLDER, "roistat_db.pkl"), "wb") as file_ref:
        pickle.dump(data, file_ref)

    levels = pandas.DataFrame(
        models.RoistatLevels.query.with_entities(
            models.RoistatLevels.id,
            models.RoistatLevels.name,
            models.RoistatLevels.title,
            models.RoistatLevels.level,
        ).all(),
        columns=["id", "name", "title", "level"],
    ).set_index("id")
    levels["level"] = levels["level"].fillna(0).apply(int)
    with open(Path(RESULTS_FOLDER, "roistat_levels.pkl"), "wb") as file_ref:
        pickle.dump(levels, file_ref)

    packages = pandas.DataFrame(
        models.RoistatPackages.query.with_entities(
            models.RoistatPackages.id,
            models.RoistatPackages.name,
            models.RoistatPackages.title,
        ).all(),
        columns=["id", "name", "title"],
    ).set_index("id")
    with open(Path(RESULTS_FOLDER, "roistat_packages.pkl"), "wb") as file_ref:
        pickle.dump(packages, file_ref)

    print("---", datetime.datetime.now() - time_now)


def roistat_leads(date_from: datetime.date, date_to: datetime.date):
    columns = ["account", "campaign", "group", "ad"]

    packages = dict([(row.id, row) for row in models.RoistatPackages.query.all()])
    levels = dict([(row.id, row) for row in models.RoistatLevels.query.all()])

    leads = pickle_loader.leads
    leads["date"] = leads["date"].apply(lambda row: row.date())
    try:
        leads = leads[(leads["date"] >= date_from) & (leads["date"] <= date_to)]
    except AttributeError:
        pass
    for column in columns:
        leads[column] = ""
    leads.sort_values(by=["date"], inplace=True)

    roistat_leads = pickle_loader.roistat_leads
    try:
        roistat_leads["date"] = roistat_leads["date"].apply(lambda row: row.date())
    except AttributeError:
        pass
    roistat_leads.drop(
        roistat_leads[
            (roistat_leads["date"] >= date_from) & (roistat_leads["date"] <= date_to)
        ].index,
        inplace=True,
    )

    lead_query = {"date": None, "rows": None}
    for index, lead in leads.iterrows():
        if lead_query.get("date") != lead.date:
            print("Collect leads:", lead.date)
            time_now = datetime.datetime.now()
            lead_query.update(
                {
                    "date": lead.date,
                    "rows": pandas.DataFrame(
                        [
                            {
                                "date": row.date,
                                "package": packages.get(row.package_id).name,
                                "expenses": row.visits_cost,
                                "account": levels.get(row.account_id).name
                                or "undefined"
                                if levels.get(row.account_id)
                                else "undefined",
                                "campaign": levels.get(row.campaign_id).name
                                or "undefined"
                                if levels.get(row.campaign_id)
                                else "undefined",
                                "group": levels.get(row.group_id).name or "undefined"
                                if levels.get(row.group_id)
                                else "undefined",
                                "ad": levels.get(row.ad_id).name or "undefined"
                                if levels.get(row.ad_id)
                                else "undefined",
                                "account_title": levels.get(row.account_id).title
                                or "Undefined"
                                if levels.get(row.account_id)
                                else "Undefined",
                                "campaign_title": levels.get(row.campaign_id).title
                                or "Undefined"
                                if levels.get(row.campaign_id)
                                else "Undefined",
                                "group_title": levels.get(row.group_id).title
                                or "Undefined"
                                if levels.get(row.group_id)
                                else "Undefined",
                                "ad_title": levels.get(row.ad_id).title or "Undefined"
                                if levels.get(row.ad_id)
                                else "Undefined",
                            }
                            for row in models.Roistat.query.filter_by(
                                date=lead.date
                            ).all()
                        ]
                    ),
                }
            )
            print("---", datetime.datetime.now() - time_now)
        detected_levels = RoistatDetectLevels(lead, lead_query.get("rows"))
        leads.loc[index, columns] = [
            detected_levels.account or "undefined",
            detected_levels.campaign or "undefined",
            detected_levels.group or "undefined",
            detected_levels.ad or "undefined",
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
    roistat_leads = (
        pandas.concat([roistat_leads, leads])
        .sort_values(by=["date"])
        .reset_index(drop=True)
    )
    roistat_leads.loc[roistat_leads["account"] == "", "account"] = "undefined"
    roistat_leads.loc[roistat_leads["campaign"] == "", "campaign"] = "undefined"
    roistat_leads.loc[roistat_leads["group"] == "", "group"] = "undefined"
    roistat_leads.loc[roistat_leads["ad"] == "", "ad"] = "undefined"
    tz = pytz.timezone("Europe/Moscow")
    roistat_leads["date"] = roistat_leads["date"].apply(
        lambda item: tz.localize(datetime.datetime.combine(item, datetime.time.min))
    )
    with open(Path(RESULTS_FOLDER, "roistat_leads.pkl"), "wb") as file_ref:
        pickle.dump(roistat_leads, file_ref)


# def roistat_update_levels():
#     statistics = pickle_loader.roistat_statistics
#     columns = ["account", "campaign", "group", "ad"]
#     date_to = datetime.datetime.now()
#     date_from = date_to - datetime.timedelta(weeks=1)
#     leads = pickle_loader.roistat_leads
#     leads["d"] = leads["date"].apply(lambda item: item.date())
#     leads = leads[(leads["d"] >= date_from.date()) & (leads["d"] <= date_to.date())]
#     leads = leads.loc[:, leads.columns != "d"]
#     leads.rename(columns={"url": "traffic_channel"}, inplace=True)
#
#     for index, lead in leads.iterrows():
#         stats = statistics[statistics.date == lead.date]
#         levels = RoistatDetectLevels(lead, stats)
#         leads.loc[index, columns] = [
#             levels.account,
#             levels.campaign,
#             levels.group,
#             levels.ad,
#         ]
#     leads.rename(columns={"traffic_channel": "url"}, inplace=True)
#
#     source = pickle_loader.roistat_leads
#     source.loc[leads.index, columns] = leads[columns].values
#     with open(Path(RESULTS_FOLDER, "roistat_leads.pkl"), "wb") as file_ref:
#         pickle.dump(source, file_ref)
