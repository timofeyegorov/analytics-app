import re
import os
import sys
import pickle
import pandas
import httplib2
import apiclient

from pathlib import Path
from datetime import date, datetime
from typing import List, Union
from xlsxwriter import Workbook

from transliterate import slugify
from oauth2client.service_account import ServiceAccountCredentials

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

sys.path.append(Variable.get("APP_FOLDER"))

from config import DATA_FOLDER, CREDENTIALS_FILE
from app.dags.decorators import log_execution_time
from app.utils import detect_week


DATA_PATH = Path(DATA_FOLDER) / "week"
os.makedirs(DATA_PATH, exist_ok=True)


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
    match = re.search(r"^(\d{1,2})\.(\d{1,2})\.(\d{4})$", str(value))
    if not match:
        return pandas.NA
    groups = list(match.groups())
    if len(groups[0]) == 1:
        groups[0] = f"0{groups[0]}"
    if len(groups[1]) == 1:
        groups[1] = f"0{groups[1]}"
    return date.fromisoformat("-".join(list(reversed(groups))))


def get_lead_id(value: str) -> str:
    lead_id = ""
    if str(value).startswith("https://neuraluniversity.amocrm.ru/leads/detail/"):
        match = re.search(r"^(\d+)", value[48:])
        lead_id = match.group(1)
    return lead_id


def rename_so_columns(value: str) -> str:
    map_values = {
        "menedzher": "Менеджер",
        "gruppa": "Группа",
        "sdelka": "Сделка",
        "fio_klienta": "ФИО клиента",
        "email": "Email",
        "telefon": "Телефон",
        "data_zoom": "Дата Zoom",
        "data_so": "Дата SO",
        "do_ili_posle_zoom": "До или после Zoom",
        "data_oplaty_": "Дата оплаты ",
        "summa_oplaty_": "Сумма оплаты ",
    }
    if value in map_values.keys():
        return map_values.get(value)

    match = re.search(r"^(\D+)(\d+)$", value)
    if match and match.group(1) in map_values.keys():
        return f"{map_values.get(match.group(1))}{match.group(2)}"

    return value


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
    spreadsheet_id = "1C4TnjTkSIsHs2svSgyFduBpRByA7M_i2sa6hrsX84EE"
    rel_fields = {
        "fio_platelschika": parse_str,
        "fio_studenta": parse_str,
        "pochta": parse_str,
        "telefon": parse_str,
        "ssylka_na_amocrm": parse_str,
        "id_sdelki": parse_int,
        "menedzher": parse_str,
        "gr": parse_int,
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
        "data_zoom": parse_date,
        "kurs": parse_str,
        "uii_uii_terra_terra_ejaj": parse_str,
        "byl_na_intensive": parse_bool,
        "byl_na_vebinarah_shtuchnyh": parse_bool,
        "ostavljal_prjamuju_zajavku": parse_bool,
        "marzhinalnost": parse_int,
        "zoom": parse_str,
    }
    sources = []
    for sheet in (
        service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute().get("sheets")
    ):
        title = sheet.get("properties").get("title")
        if title == "Все оплаты":
            values = (
                service.spreadsheets()
                .values()
                .get(spreadsheetId=spreadsheet_id, range=title, majorDimension="ROWS")
                .execute()
            )
            items = values.get("values")
            items[0] = list(
                map(lambda item: slugify(item, "ru").replace("-", "_"), items[0])
            )
            data = pandas.DataFrame(columns=items[0], data=items[1:])
            data.insert(
                5,
                "id_sdelki",
                data["ssylka_na_amocrm"].apply(get_lead_id),
                allow_duplicates=True,
            )
            for column, parse_fn in rel_fields.items():
                if column not in data.columns:
                    data[column] = ""
                data[column] = data[column].apply(parse_fn)
            sources.append(data)
            break
    data = pandas.concat(sources, ignore_index=True)

    data.insert(
        0,
        "menedzher_id",
        data.menedzher.apply(lambda item: slugify(item, "ru").replace("-", "_")),
    )
    data = data[["menedzher_id"] + list(rel_fields.keys())]

    with open(Path(DATA_PATH / "sources.pkl"), "wb") as file_ref:
        pickle.dump(data, file_ref)


@log_execution_time("calculate")
def calculate():
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


@log_execution_time("get_zoom")
def get_zoom():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE,
        [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    http_auth = credentials.authorize(httplib2.Http())
    service = apiclient.discovery.build("sheets", "v4", http=http_auth)
    spreadsheet_id = "1xKcTwITOBVNTarxciMo6gEJNZuMMxsWr4CS9eDnYiA8"
    for sheet in (
        service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute().get("sheets")
    ):
        values = (
            service.spreadsheets()
            .values()
            .get(
                spreadsheetId=spreadsheet_id,
                range=sheet.get("properties").get("title"),
                majorDimension="ROWS",
            )
            .execute()
        )
        items = values.get("values")
        for index, item in enumerate(items[1:]):
            diff = len(items[0]) - len(item)
            if diff < 0:
                items[index + 1] = item[:diff]
            elif diff > 0:
                items[index + 1] = item + [0] * diff
        data = (
            pandas.DataFrame(data=items[1:], columns=items[0])
            .fillna(0)
            .replace("", 0)
            .rename(columns={"Менеджер": "manager_title", "Группа": "group"})
        )
        data.insert(
            0,
            "manager",
            data["manager_title"].apply(
                lambda item: slugify(item, "ru").replace("-", "_")
            ),
        )
        sources = []
        for manager, group in data.groupby("manager"):
            group_index = group["group"].iloc[0]
            group_index_title = f'Группа "{group_index}"'
            manager_title = group["manager_title"].iloc[0]
            group.drop(columns=["manager", "manager_title", "group"], inplace=True)
            group = group.T
            group.reset_index(inplace=True)
            group.rename(
                columns={"index": "date", group.columns[1]: "count"}, inplace=True
            )
            group.insert(0, "manager", manager)
            group.insert(1, "manager_title", manager_title)
            group.insert(2, "group", group_index)
            group.insert(3, "group_title", group_index_title)
            group["date"] = group["date"].apply(parse_date)
            group["count"] = group["count"].astype(int)
            sources.append(group)

    zoom = pandas.concat(sources, ignore_index=True)

    with open(Path(DATA_PATH / "sources_zoom.pkl"), "wb") as file_ref:
        pickle.dump(zoom, file_ref)


@log_execution_time("calculate_zoom")
def calculate_zoom():
    with open(Path(DATA_PATH / "sources.pkl"), "rb") as sources_ref:
        data: pandas.DataFrame = pickle.load(sources_ref)
        data = data[~(data.data_zoom.isna() | data.data_oplaty.isna())]
        data.reset_index(drop=True, inplace=True)

        items = []
        for index, item in data.iterrows():
            order_week = detect_week(item.data_zoom)
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

    with open(Path(DATA_PATH / "stats_zoom.pkl"), "wb") as file_ref:
        pickle.dump(output, file_ref)


@log_execution_time("update_so")
def update_so():
    def write_xlsx(data: List[List[Union[str, int]]]):
        workbook = Workbook(DATA_PATH / "so.xlsx")
        worksheet = workbook.add_worksheet("SpecialOffers")
        for index, row in enumerate(data):
            worksheet.write_row(index, 0, row)
        worksheet.autofilter(0, 0, len(data) - 1, len(data[0]) - 1)
        workbook.close()

    with open(Path(DATA_PATH / "sources.pkl"), "rb") as file_ref:
        sources: pandas.DataFrame = pickle.load(file_ref)

    rel_fields = {
        "menedzher": parse_str,
        "gruppa": parse_str,
        "sdelka": parse_str,
        "id_sdelki": parse_int,
        "fio_klienta": parse_str,
        "email": parse_str,
        "telefon": parse_str,
        "data_zoom": parse_date,
        "data_so": parse_date,
        "do_ili_posle_zoom": parse_str,
    }

    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE,
        [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    http_auth = credentials.authorize(httplib2.Http())
    service = apiclient.discovery.build("sheets", "v4", http=http_auth)
    spreadsheet_id = "1C4TnjTkSIsHs2svSgyFduBpRByA7M_i2sa6hrsX84EE"
    sheet_id = None
    data = None

    for sheet in (
        service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute().get("sheets")
    ):
        title = sheet.get("properties").get("title")
        if title == "SpecialOffers":
            sheet_id = sheet.get("properties").get("sheetId")
            values = (
                service.spreadsheets()
                .values()
                .get(spreadsheetId=spreadsheet_id, range=title, majorDimension="ROWS")
                .execute()
            )
            items = values.get("values", [])
            if len(items):
                items[0] = list(
                    map(lambda item: slugify(item, "ru").replace("-", "_"), items[0])
                )
                data = pandas.DataFrame(columns=items[0], data=items[1:])
                data.insert(
                    3,
                    "id_sdelki",
                    data["sdelka"].apply(get_lead_id),
                    allow_duplicates=True,
                )
                for column, parse_fn in rel_fields.items():
                    if column not in data.columns:
                        data[column] = ""
                    data[column] = data[column].apply(parse_fn)
            break

    if data is not None:
        data = data[rel_fields.keys()]
        for index, row in data.iterrows():
            if not pandas.isna(row["id_sdelki"]):
                row_sources = sources[
                    sources["id_sdelki"] == row["id_sdelki"]
                ].sort_values(by=["data_oplaty"])
                num = 1
                for _, payment in row_sources.iterrows():
                    data.loc[index, [f"data_oplaty_{num}", f"summa_oplaty_{num}"]] = [
                        payment["data_oplaty"],
                        payment["summa_vyruchki"],
                    ]
                    num += 1

        if sheet_id is not None:
            data.drop(columns=["id_sdelki"], inplace=True)
            data.fillna("", inplace=True)
            data["do_ili_posle_zoom"] = data["do_ili_posle_zoom"].apply(
                lambda item: "" if item == "None" else item
            )
            for column in data.columns:
                if column in ["data_zoom", "data_so"] or str(column).startswith(
                    "data_oplaty_"
                ):
                    data[column] = data[column].apply(
                        lambda item: str(item.strftime("%d.%m.%Y")) if item else ""
                    )
                elif str(column).startswith("summa_oplaty_"):
                    data[column] = data[column].apply(
                        lambda item: "" if str(item) == "" else int(item)
                    )
                else:
                    data[column] = data[column].apply(lambda item: f"'{item}")
            data.rename(
                columns=dict(
                    zip(data.columns, list(map(rename_so_columns, data.columns)))
                ),
                inplace=True,
            )
            values = [list(data.columns)] + data.values.tolist()
            write_xlsx(values)
            requests = [
                {
                    "deleteSheet": {
                        "sheetId": sheet_id,
                    }
                },
                {
                    "addSheet": {
                        "properties": {
                            "title": "SpecialOffers",
                        }
                    }
                },
            ]
            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id, body={"requests": requests}
            ).execute()
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"SpecialOffers!A1:ZZ{len(data)+1}",
                valueInputOption="USER_ENTERED",
                body={"values": values},
            ).execute()


@log_execution_time("get_so")
def get_so():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE,
        [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    http_auth = credentials.authorize(httplib2.Http())
    service = apiclient.discovery.build("sheets", "v4", http=http_auth)
    spreadsheet_id = "1C4TnjTkSIsHs2svSgyFduBpRByA7M_i2sa6hrsX84EE"
    for sheet in (
        service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute().get("sheets")
    ):
        title = sheet.get("properties").get("title")
        if title == "SpecialOffers":
            values = (
                service.spreadsheets()
                .values()
                .get(spreadsheetId=spreadsheet_id, range=title, majorDimension="ROWS")
                .execute()
            )
            items = values.get("values")
            items[0] = list(
                map(lambda item: slugify(item, "ru").replace("-", "_"), items[0])
            )
            data: pandas.DataFrame = (
                pandas.DataFrame(data=items[1:], columns=items[0])
                .fillna("")
                .rename(columns={"menedzher": "menedzher_title"})
            )
            data.insert(
                0,
                "menedzher",
                data["menedzher_title"].apply(
                    lambda item: slugify(item, "ru").replace("-", "_")
                ),
            )
            data.insert(
                3,
                "gruppa_title",
                data["gruppa"].apply(
                    lambda item: "" if str(item) == "" else f'Группа "{item}"'
                ),
            )
            data["data_so"] = data["data_so"].apply(parse_date)
            data = data[(data["data_so"] != "") & (data["menedzher"] != "")]
            sources = []
            for (manager, date_so), group in data.groupby(["menedzher", "data_so"]):
                first = group.iloc[0]
                sources.append(
                    [
                        manager,
                        first["menedzher_title"],
                        first["gruppa"],
                        first["gruppa_title"],
                        date_so,
                        len(group),
                    ]
                )
            so = pandas.DataFrame(
                sources,
                columns=[
                    "manager",
                    "manager_title",
                    "group",
                    "group_title",
                    "date",
                    "count",
                ],
            )
            with open(Path(DATA_PATH / "sources_so.pkl"), "wb") as file_ref:
                pickle.dump(so, file_ref)
            break


@log_execution_time("calculate_so")
def calculate_so():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE,
        [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    http_auth = credentials.authorize(httplib2.Http())
    service = apiclient.discovery.build("sheets", "v4", http=http_auth)
    spreadsheet_id = "1C4TnjTkSIsHs2svSgyFduBpRByA7M_i2sa6hrsX84EE"
    for sheet in (
        service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute().get("sheets")
    ):
        title = sheet.get("properties").get("title")
        if title == "SpecialOffers":
            values = (
                service.spreadsheets()
                .values()
                .get(spreadsheetId=spreadsheet_id, range=title, majorDimension="ROWS")
                .execute()
            )
            items = values.get("values")
            items[0] = list(
                map(lambda item: slugify(item, "ru").replace("-", "_"), items[0])
            )
            data: pandas.DataFrame = (
                pandas.DataFrame(data=items[1:], columns=items[0])
                .fillna("")
                .rename(columns={"menedzher": "menedzher_title"})
            )
            data.insert(
                0,
                "menedzher",
                data["menedzher_title"].apply(
                    lambda item: slugify(item, "ru").replace("-", "_")
                ),
            )
            data.insert(
                3,
                "gruppa_title",
                data["gruppa"].apply(
                    lambda item: "" if str(item) == "" else f'Группа "{item}"'
                ),
            )
            data = data[(data["data_so"] != "") & (data["menedzher"] != "")]
            payment_date = sorted(
                list(
                    filter(
                        lambda item: str(item).startswith("data_oplaty_"), data.columns
                    )
                )
            )
            payment_profit = sorted(
                list(
                    filter(
                        lambda item: str(item).startswith("summa_oplaty_"), data.columns
                    )
                )
            )
            payments = tuple(zip(payment_date, payment_profit))
            items = []
            for index, item in data.iterrows():
                order_week = detect_week(datetime.strptime(item["data_so"], "%d.%m.%Y"))
                for day, profit in payments:
                    if item[day] and item[profit]:
                        payment_week = detect_week(
                            datetime.strptime(item[day], "%d.%m.%Y")
                        )
                        items.append(
                            {
                                "manager": item["menedzher"],
                                "manager_title": item["menedzher_title"],
                                "order_from": order_week[0],
                                "order_to": order_week[1],
                                "payment_from": payment_week[0],
                                "payment_to": payment_week[1],
                                "income": int(item[profit]),
                            }
                        )
            output = pandas.DataFrame(data=items)
            with open(Path(DATA_PATH / "stats_so.pkl"), "wb") as file_ref:
                pickle.dump(output, file_ref)
            break


dag = DAG(
    "week_stats",
    description="Collect week statistics",
    schedule_interval="0 * * * *",
    start_date=datetime(2017, 3, 20),
    catchup=False,
)


get_stats_operator = PythonOperator(
    task_id="get_stats",
    python_callable=get_stats,
    dag=dag,
)
calculate_operator = PythonOperator(
    task_id="calculate",
    python_callable=calculate,
    dag=dag,
)
get_zoom_operator = PythonOperator(
    task_id="get_zoom",
    python_callable=get_zoom,
    dag=dag,
)
calculate_zoom_operator = PythonOperator(
    task_id="calculate_zoom",
    python_callable=calculate_zoom,
    dag=dag,
)
update_so_operator = PythonOperator(
    task_id="update_so",
    python_callable=update_so,
    dag=dag,
)
get_so_operator = PythonOperator(
    task_id="get_so",
    python_callable=get_so,
    dag=dag,
)
calculate_so_operator = PythonOperator(
    task_id="calculate_so",
    python_callable=calculate_so,
    dag=dag,
)

get_stats_operator >> calculate_operator
get_stats_operator >> update_so_operator
get_stats_operator >> calculate_zoom_operator
get_zoom_operator >> calculate_zoom_operator
update_so_operator >> get_so_operator
update_so_operator >> calculate_so_operator
