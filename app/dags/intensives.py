import re
import sys
import pandas
import pickle
import datetime
import requests

from io import BytesIO
from typing import Tuple, List, Callable
from pathlib import Path
from httplib2 import Http
from openpyxl import load_workbook
from apiclient import discovery
from transliterate import slugify
from googleapiclient.discovery import Resource as GoogleAPIClientResource
from oauth2client.service_account import ServiceAccountCredentials

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

sys.path.append(Variable.get("APP_FOLDER"))

from app.dags.decorators import log_execution_time

from config import RESULTS_FOLDER, CREDENTIALS_FILE


def parse_slug(value: str) -> str:
    if str(value) == "" or pandas.isna(value):
        return pandas.NA
    return slugify(str(value), "ru").replace("-", "_")


def slugify_columns(columns: List[str]) -> List[str]:
    return list(map(parse_slug, columns))


def parse_str(value: str) -> str:
    if pandas.isna(value):
        return pandas.NA
    value = re.sub(r"\s+", " ", str(value).strip())
    if value == "":
        value = pandas.NA
    return value


def parse_int(value: str) -> int:
    value = parse_float(value)
    if pandas.isna(value):
        return pandas.NA
    return round(value)


def parse_float(value: str) -> float:
    if pandas.isna(value):
        return pandas.NA
    value = re.sub(r"\s", "", str(value))
    if not re.search(r"^-?\d+\.?\d*$", str(value)):
        return pandas.NA
    return float(value)


def parse_date(value: str) -> datetime.date:
    if pandas.isna(value):
        return pandas.NA
    if isinstance(value, datetime.date):
        return value
    if isinstance(value, datetime.datetime):
        return value.date()
    match = re.search(r"^(\d{1,2})\.(\d{1,2})\.(\d{4})$", str(value))
    if not match:
        return pandas.NA
    groups = list(match.groups())
    if len(groups[0]) == 1:
        groups[0] = f"0{groups[0]}"
    if len(groups[1]) == 1:
        groups[1] = f"0{groups[1]}"
    return datetime.date.fromisoformat("-".join(list(reversed(groups))))


def get_google_service() -> GoogleAPIClientResource:
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE,
        [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    http_auth = credentials.authorize(Http())
    return discovery.build("sheets", "v4", http=http_auth)


def get_intensives_payments(service: GoogleAPIClientResource) -> pandas.DataFrame:
    def processing_source(
        values: List[List[str]], columns_info: List[Tuple[str, str, Callable]]
    ) -> pandas.DataFrame:
        columns_count = max(list(map(lambda item: len(item), values[1:])))
        headers_count = len(values[0])
        if columns_count > headers_count:
            values[0] += list(
                map(
                    lambda item: f"Undefined {item}",
                    range(columns_count - headers_count),
                )
            )
        source = pandas.DataFrame(data=values[1:], columns=slugify_columns(values[0]))

        undefined_columns = list(
            set(map(lambda item: item[0], columns_info)) - set(source.columns)
        )
        assert len(undefined_columns) == 0, f"Undefined columns: {undefined_columns}"

        source = source[map(lambda item: item[0], columns_info)].rename(
            columns=dict(map(lambda item: item[0:2], columns_info))
        )
        parser_relation = dict(map(lambda item: item[1:3], columns_info))
        source = source.apply(lambda item: item.apply(parser_relation.get(item.name)))

        return source

    values = (
        service.spreadsheets()
        .values()
        .get(
            spreadsheetId="1C4TnjTkSIsHs2svSgyFduBpRByA7M_i2sa6hrsX84EE",
            range="Все оплаты",
            majorDimension="ROWS",
        )
        .execute()
    ).get("values")
    dataframe: pandas.DataFrame = processing_source(
        values,
        [
            ("pochta", "email", parse_str),
            ("data_oplaty", "profit_date", parse_date),
            ("summa_vyruchki", "profit", parse_int),
        ],
    ).sort_values(by=["profit_date"], ignore_index=True)
    return dataframe


def get_intensives_emails(spreadsheet_id: str) -> pandas.DataFrame:
    response = requests.get(
        f"https://spreadsheets.google.com/feeds/download/spreadsheets/Export?key={spreadsheet_id}&exportFormat=xlsx"
    )
    dataframe = pandas.DataFrame()
    xlsx = load_workbook(filename=BytesIO(response.content))
    for worksheet in xlsx.worksheets:
        if worksheet.sheet_state.lower() == "hidden":
            continue
        title = str(worksheet.title).strip()
        try:
            date = datetime.datetime.strptime(title, "%d.%m.%Y").date()
        except ValueError:
            continue
        values = []
        for row in worksheet.rows:
            row = list(map(lambda cell: cell.value, row))
            if not len(list(filter(None, row))):
                continue
            values.append(row)
        columns = slugify_columns(["Дата интенсива"] + values[0])
        indexes = [i for i, e in enumerate(columns) if not pandas.isna(e)]
        columns = [columns[i] for i in indexes]
        values = list(map(lambda item: [date] + item, values[1:]))
        for index, value in enumerate(values):
            values[index] = [value[i] for i in indexes]
        dataframe = pandas.concat(
            [
                dataframe,
                pandas.DataFrame(values, columns=columns)[columns].drop_duplicates(
                    keep="last"
                ),
            ],
            ignore_index=True,
        )
    return dataframe.sort_values(by=["data_intensiva"], ignore_index=True)


def processing_intensives_stats(source: pandas.DataFrame) -> pandas.DataFrame:
    service = get_google_service()
    payments = get_intensives_payments(service)
    values = []
    for group_name, group in source.groupby(by=["e_mail"], sort=False):
        rows = list(group.itertuples())
        for index, row in enumerate(rows):
            try:
                row_next = rows[index + 1]
            except IndexError:
                row_next = None
            row_payments = payments[
                (payments["profit_date"] >= getattr(row, "data_intensiva"))
                & (payments["email"] == getattr(row, "e_mail"))
            ]
            if row_next is not None:
                row_payments = row_payments[
                    row_payments["profit_date"] < getattr(row_next, "data_intensiva")
                ]
            values.append(
                (
                    getattr(row, "data_intensiva"),
                    getattr(row, "e_mail"),
                    len(row_payments),
                    row_payments["profit"].sum(),
                )
            )
    return pandas.DataFrame(values, columns=["date", "email", "deals", "profit"])


@log_execution_time("get_intensives_registration_stats")
def get_intensives_registration_stats():
    source = get_intensives_emails("1kNVxlBWFwiK6jGktyPQAlqVqzKB5mM_NLmwriaYfv1Q")
    dataframe = processing_intensives_stats(source)
    with open(Path(RESULTS_FOLDER, "intensives_registration.pkl"), "wb") as file_ref:
        pickle.dump(dataframe, file_ref)


@log_execution_time("get_intensives_preorder_stats")
def get_intensives_preorder_stats():
    source = get_intensives_emails("1kNVxlBWFwiK6jGktyPQAlqVqzKB5mM_NLmwriaYfv1Q")
    dataframe = processing_intensives_stats(source)
    with open(Path(RESULTS_FOLDER, "intensives_preorder.pkl"), "wb") as file_ref:
        pickle.dump(dataframe, file_ref)


dag = DAG(
    "intensives",
    description="Collect intensives profit",
    schedule_interval="30 * * * *",
    start_date=datetime.datetime(2017, 3, 20),
    catchup=False,
)

get_intensives_registration_stats_operator = PythonOperator(
    task_id="get_intensives_registration_stats",
    python_callable=get_intensives_registration_stats,
    dag=dag,
)
get_intensives_preorder_stats_operator = PythonOperator(
    task_id="get_intensives_preorder_stats",
    python_callable=get_intensives_preorder_stats,
    dag=dag,
)
