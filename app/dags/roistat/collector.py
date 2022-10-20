import re
import sys
import pytz

from time import sleep
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

sys.path.append(Variable.get("APP_FOLDER"))

from app.data import StatisticsRoistatPackageEnum
from app.plugins.ads import roistat
from app.dags.roistat import reader, writer, data
from app.dags.decorators import log_execution_time


def validate_response(response: Dict[str, Any], name: str):
    if (
        data.ResponseStatusEnum[response.get("status")]
        != data.ResponseStatusEnum.success
    ):
        raise Exception(f"Error while getting response `{name}`: {response}")


def get_levels(
    dimensions: Dict[str, Dict[str, str]],
) -> Dict[str, Optional[str]]:
    def get_package(value) -> Optional[str]:
        if re.match(r"^vk.+$", value):
            return StatisticsRoistatPackageEnum.vk.name
        if (
            re.match(r"^direct\d+.*$", value)
            or re.match(r"^:openstat:direct\.yandex\.ru$", value)
            or re.match(r"^direct$", value)
        ):
            return StatisticsRoistatPackageEnum.yandex_direct.name
        if re.match(r"^ya\.master$", value) or re.match(r"^yulyayamaster$", value):
            return StatisticsRoistatPackageEnum.yandex_master.name
        if re.match(r"^facebook\d+.*$", value):
            return StatisticsRoistatPackageEnum.facebook.name
        if re.match(r"^mytarget\d+$", value):
            return StatisticsRoistatPackageEnum.mytarget.name
        if re.match(r"^google\d+$", value) or re.match(r"^g-adwords\d+$", value):
            return StatisticsRoistatPackageEnum.google.name
        if re.match(r"^site$", value):
            return StatisticsRoistatPackageEnum.site.name
        if re.match(r"^seo$", value):
            return StatisticsRoistatPackageEnum.seo.name
        if re.match(r"^:utm:.+$", value):
            return StatisticsRoistatPackageEnum.utm.name
        if value:
            print("Undefined package:", value)
        return StatisticsRoistatPackageEnum.undefined.name

    output = {
        "package": None,
        "marker_level_1": None,
        "marker_level_2": None,
        "marker_level_3": None,
        "marker_level_4": None,
        "marker_level_5": None,
        "marker_level_6": None,
        "marker_level_7": None,
        "marker_level_1_title": None,
        "marker_level_2_title": None,
        "marker_level_3_title": None,
        "marker_level_4_title": None,
        "marker_level_5_title": None,
        "marker_level_6_title": None,
        "marker_level_7_title": None,
    }
    levels = dict(sorted(dimensions.items()))
    for name, level in levels.items():
        level_value = level.get("value", "")
        level_title = level.get("title", "")
        if level_value or level_title:
            output.update({name: level_value, f"{name}_title": level_title})
        else:
            output.update({name: None})
    output.update({"package": get_package(output.get("marker_level_1"))})
    return output


def get_metrics(metrics: List[Dict[str, Any]]) -> Dict[str, str]:
    return dict(map(lambda item: (item.get("metric_name"), item.get("value")), metrics))


@log_execution_time("analytics")
def analytics():
    tz = pytz.timezone("Europe/Moscow")
    analytics = reader("analytics")
    from_date = max(
        list(analytics.date.unique())
        or [tz.localize(datetime.strptime("2020-02-01", "%Y-%m-%d"))]
    ) - timedelta(days=1)
    to_date = from_date + timedelta(days=30)
    datetime_now = datetime.now(tz=pytz.timezone("Europe/Moscow")).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    if to_date > datetime_now:
        to_date = datetime_now
    for day in range((to_date - from_date).days + 1):
        current_date: datetime = from_date + timedelta(days=day)
        print("Collect analytic:", current_date)
        time_now = datetime.now()
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
                "from": current_date.strftime("%Y-%m-%dT00:00:00+0300"),
                "to": current_date.strftime("%Y-%m-%dT23:59:59+0300"),
            },
            metrics=["visitsCost"],
            interval="1d",
        )
        for item_data in response.get("data"):
            date = tz.localize(
                datetime.strptime(
                    item_data.get("dateFrom"),
                    "%Y-%m-%dT%H:%M:%S+0000",
                )
                + timedelta(seconds=3600 * 3)
            )
            for item in item_data.get("items"):
                levels = get_levels(item.get("dimensions"))
                metrics = get_metrics(item.get("metrics"))
                analytics = analytics.append(
                    {**levels, **metrics, "date": date}, ignore_index=True
                )
        print("---", datetime.now() - time_now)
        sleep(1)
    writer("analytics", data=analytics)


@log_execution_time("statistics")
def statistics():
    pass


dag = DAG(
    "api_data_roistat",
    description="Collect Roistat API data",
    schedule_interval="0 * * * *",
    start_date=datetime(2017, 3, 20),
    catchup=False,
)


analytics_po = PythonOperator(
    task_id="analytics",
    python_callable=analytics,
    dag=dag,
)
statistics_po = PythonOperator(
    task_id="statistics",
    python_callable=statistics,
    dag=dag,
)


analytics_po >> statistics_po
