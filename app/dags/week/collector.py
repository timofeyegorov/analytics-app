import re
import csv
import pickle
import pandas
import httplib2
import apiclient

from typing import Tuple
from pathlib import Path
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from transliterate import slugify
from oauth2client.service_account import ServiceAccountCredentials

from config import CREDENTIALS_FILE
from app.dags.decorators import log_execution_time

from . import DATA_PATH


def parse_str(value: str) -> str:
    if str(value) == "":
        return pandas.NA
    return re.sub(r"\s+", " ", str(value).strip())


def parse_int(value: str) -> int:
    value = re.sub(r"\s", "", str(value))
    if not re.search(r"^-?\d+\.?\d*$", str(value)):
        return pandas.NA
    return round(float(value))


def parse_bool(value: str) -> bool:
    if not value:
        return pandas.NA
    value = str(value).lower()
    if value == "да":
        return True
    elif value == "нет":
        return False
    else:
        return pandas.NA


def parse_date(value: str) -> date:
    match = re.search(r"^(\d{2})\.(\d{2})\.(\d{4})$", str(value))
    if not match:
        return pandas.NA
    groups = list(match.groups())
    return date.fromisoformat("-".join(list(reversed(groups))))


@log_execution_time("get_stats")
def get_stats():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE,
        [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    http_auth = credentials.authorize(httplib2.Http())
    service = apiclient.discovery.build("sheets", "v4", http=http_auth)
    values = (
        service.spreadsheets()
        .values()
        .get(
            spreadsheetId="1YzFWth__V57czEC-je0va4X7Li6unJI0Pvm_ZgaKIAo",
            range="A1:U100000",
            majorDimension="ROWS",
        )
        .execute()
    )

    items = values.get("values")
    items[0] = list(map(lambda item: slugify(item, "ru").replace("-", "_"), items[0]))
    data = pandas.DataFrame(columns=items[0], data=items[1:])

    rel = {
        "menedzher": parse_str,
        "summa_oplachennaja_klientom": parse_int,
        "summa_vyruchki": parse_int,
        "nomer_zakaza": parse_str,
        "istochnik_oplaty": parse_str,
        "data_sozdanija_sdelki": parse_date,
        "data_poslednej_zajavki_ljuboj": parse_date,
        "data_poslednej_zajavki_platnoj": parse_date,
        "data_oplaty": parse_date,
        "dnej_do_prodazhi_ot_codanija": parse_int,
        "dnej_do_prodazhi_ot_poslednej_zajavki_ljuboj": parse_int,
        "dnej_do_prodazhi_ot_poslednej_zajavki_platnoj": parse_int,
        "voronka": parse_str,
        "kurs": parse_str,
        "uii_uii_terra_terra_ejaj": parse_str,
        "byl_na_intensive": parse_bool,
        "byl_na_vebinarah_shtuchnyh": parse_bool,
        "ostavljal_prjamuju_zajavku": parse_bool,
        "marzhinalnost": parse_int,
        "zoom": parse_str,
    }
    for column, parse_fn in rel.items():
        data[column] = data[column].apply(parse_fn)

    data.insert(
        0,
        "menedzher_id",
        data.menedzher.apply(lambda item: slugify(item, "ru").replace("-", "_")),
    )

    data.drop(columns=["mesjats_doplata"], inplace=True)

    with open(Path(DATA_PATH / "sources.pkl"), "wb") as file_ref:
        pickle.dump(data, file_ref)


@log_execution_time("calculate")
def calculate():
    def detect_week(value: date) -> Tuple[date, date]:
        start_week = 3
        date_from = value - timedelta(
            days=value.weekday()
            + (7 if value.weekday() < start_week else 0)
            - start_week
        )
        date_to = date_from + timedelta(days=6)
        return date_from, date_to

    with open(Path(DATA_PATH / "sources.pkl"), "rb") as file_ref:
        data: pandas.DataFrame = pickle.load(file_ref)
        data = data[
            ~(data.data_poslednej_zajavki_platnoj.isna() | data.data_oplaty.isna())
        ]
        data.reset_index(drop=True, inplace=True)

        items = []
        for index, item in data.iterrows():
            order_week = detect_week(item.data_poslednej_zajavki_platnoj)
            payment_week = detect_week(item.data_oplaty)
            items.append(
                {
                    "manager": item.menedzher_id,
                    "manager_title": item.menedzher,
                    "order_from": order_week[0],
                    "order_to": order_week[1],
                    "payment_from": payment_week[0],
                    "payment_to": payment_week[1],
                    "income": item.summa_vyruchki,
                }
            )

        output = pandas.DataFrame(data=items)

    with open(Path(DATA_PATH / "stats.pkl"), "wb") as file_ref:
        pickle.dump(output, file_ref)


dag = DAG(
    "week_stats",
    description="Collect week statistics",
    schedule_interval="0 * * * *",
    start_date=datetime(2017, 3, 20),
    catchup=False,
)


get_stats_operator = PythonOperator(
    task_id="get_stats", python_callable=get_stats, dag=dag
)
calculate_operator = PythonOperator(
    task_id="calculate", python_callable=calculate, dag=dag
)

get_stats_operator >> calculate_operator
