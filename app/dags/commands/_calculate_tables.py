import pytz
import datetime

from time import sleep

from app import db
from app.data import StatisticsRoistatPackageEnum
from app.database import models
from app.plugins.ads import roistat

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
