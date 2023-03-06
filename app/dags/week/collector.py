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
    if str(value) == "" or pandas.isna(value):
        return pandas.NA
    return re.sub(r"\s+", " ", str(value).strip())


def parse_int(value: str) -> int:
    value = re.sub(r"\s", "", str(value))
    if not re.search(r"^-?\d+\.?\d*$", str(value)) or pandas.isna(value):
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
    if pandas.isna(value):
        return pandas.NA
    if isinstance(value, date):
        return value
    match = re.search(r"^(\d{1,2})\.(\d{1,2})\.(\d{4})$", str(value))
    if not match:
        return pandas.NA
    groups = list(match.groups())
    if len(groups[0]) == 1:
        groups[0] = f"0{groups[0]}"
    if len(groups[1]) == 1:
        groups[1] = f"0{groups[1]}"
    return date.fromisoformat("-".join(list(reversed(groups))))


def parse_lead_id(value: str) -> int:
    if pandas.isna(value):
        return pandas.NA
    lead_id = pandas.NA
    if str(value).startswith("https://neuraluniversity.amocrm.ru/leads/detail/"):
        match = re.search(r"^(\d+)", value[48:])
        lead_id = match.group(1)
    return lead_id


def parse_slug(value: str) -> str:
    if pandas.isna(value):
        return pandas.NA
    return slugify(value, "ru").replace("-", "_")


def merge_columns(value):
    values = list(filter(lambda item: not pandas.isna(item), value))
    return values[0] if len(values) else pandas.NA


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
        "id_oplaty_": "ID оплаты ",
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
        "order_id": parse_int,
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
            data.insert(
                6,
                "order_id",
                list(map(lambda item: item + 1, data.index)),
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
    spreadsheet_id = "1C4TnjTkSIsHs2svSgyFduBpRByA7M_i2sa6hrsX84EE"
    sources = []
    for sheet in (
        service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute().get("sheets")
    ):
        title = sheet.get("properties").get("title")
        if title == "Количество Zoom":
            values = (
                service.spreadsheets()
                .values()
                .get(spreadsheetId=spreadsheet_id, range=title, majorDimension="ROWS")
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
            break

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
                    data.loc[
                        index,
                        [
                            f"id_oplaty_{num}",
                            f"data_oplaty_{num}",
                            f"summa_oplaty_{num}",
                        ],
                    ] = [
                        payment["order_id"],
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
                elif str(column).startswith("id_oplaty_") or str(column).startswith(
                    "summa_oplaty_"
                ):
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
                order_week = detect_week(
                    datetime.strptime(item["data_so"], "%d.%m.%Y").date()
                )
                for day, profit in payments:
                    if item[day] and item[profit]:
                        payment_week = detect_week(
                            datetime.strptime(item[day], "%d.%m.%Y").date()
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


@log_execution_time("get_managers")
def get_managers():
    def processing_source_payments(source: pandas.DataFrame) -> pandas.DataFrame:
        source = source[
            [
                "menedzher",
                "gr",
                "ssylka_na_amocrm",
                "summa_vyruchki",
                "data_oplaty",
                "data_zoom",
            ]
        ].rename(
            columns={
                "menedzher": "manager",
                "gr": "group",
                "ssylka_na_amocrm": "lead",
                "summa_vyruchki": "payment",
                "data_oplaty": "payment_date",
                "data_zoom": "zoom_date",
            }
        )
        source["manager"] = source["manager"].apply(parse_str)
        source["manager_id"] = source["manager"].apply(parse_slug)
        source["group"] = source["group"].apply(parse_str)
        source["lead"] = source["lead"].apply(parse_lead_id)
        source["order_id"] = list(map(lambda item: item + 1, source.index))
        source["payment"] = source["payment"].apply(parse_int)
        source["payment_date"] = source["payment_date"].apply(parse_date)
        source["zoom_date"] = source["zoom_date"].apply(parse_date)
        return source[~source["lead"].isna()]

    def processing_source_zoom(source: pandas.DataFrame) -> pandas.DataFrame:
        source = (
            source.fillna(0)
            .replace("", 0)
            .rename(columns={"menedzher": "manager", "gruppa": "group"})
        )
        for column in source.columns:
            source[column] = source[column].apply(
                parse_str if column in ["manager", "group"] else parse_int
            )
        data = []
        for manager, items in source.groupby("manager"):
            group = items["group"].iloc[0]
            items.drop(columns=["manager", "group"], inplace=True)
            items = items.T
            items.reset_index(inplace=True)
            items.rename(
                columns={"index": "date", items.columns[1]: "count"}, inplace=True
            )
            items.insert(0, "manager", manager)
            items.insert(1, "group", group)
            items["date"] = (
                items["date"]
                .apply(lambda item: f"{item[:2]}.{item[2:4]}.{item[4:]}")
                .apply(parse_date)
            )
            items["manager_id"] = items["manager"].apply(parse_slug)
            data.append(items)
        data = pandas.concat(data, ignore_index=True)
        return data[data["count"] != 0].reset_index(drop=True)

    def processing_source_so(source: pandas.DataFrame) -> pandas.DataFrame:
        def column_is_payment(column: str) -> bool:
            return (
                column.startswith("id_oplaty_")
                or column.startswith("data_oplaty_")
                or column.startswith("summa_oplaty_")
            )

        columns_source = ["menedzher", "gruppa", "sdelka", "data_so"]
        columns_target = {
            "menedzher": "manager",
            "gruppa": "group",
            "sdelka": "lead",
            "data_so": "so_date",
        }
        columns_payment = list(filter(column_is_payment, source.columns))
        columns_source += columns_payment
        source = source[columns_source].rename(columns=columns_target).fillna("")
        source = source[~source["lead"].isna()]
        source["manager"] = source["manager"].apply(parse_str)
        source["manager_id"] = source["manager"].apply(parse_slug)
        source["group"] = source["group"].apply(parse_str)
        source["lead"] = source["lead"].apply(parse_lead_id)
        source["so_date"] = source["so_date"].apply(parse_date)
        for column in columns_payment:
            parse_method = None
            if column.startswith("data_oplaty_"):
                parse_method = parse_date
            elif column.startswith("id_oplaty_") or column.startswith("summa_oplaty_"):
                parse_method = parse_int
            if parse_method:
                source[column] = source[column].apply(parse_method)
        data = []
        columns_base = list(set(source.columns) - set(columns_payment))
        for index, row in source.iterrows():
            row_base: pandas.Series = row[columns_base]
            payment_len = len(
                list(
                    filter(
                        lambda num: not (
                            pandas.isna(row[f"id_oplaty_{num}"])
                            or pandas.isna(row[f"data_oplaty_{num}"])
                            or pandas.isna(row[f"summa_oplaty_{num}"])
                        ),
                        range(1, int(len(columns_payment) / 3) + 1),
                    )
                )
            )
            if payment_len:
                for num in range(1, payment_len + 1):
                    item = row_base.copy()
                    item["order_id"] = row[f"id_oplaty_{num}"]
                    item["payment_date"] = row[f"data_oplaty_{num}"]
                    item["payment"] = row[f"summa_oplaty_{num}"]
                    data.append(item.to_dict())
            else:
                item = row_base.copy()
                item["order_id"] = pandas.NA
                item["payment_date"] = pandas.NA
                item["payment"] = pandas.NA
                data.append(item.to_dict())
        if len(data):
            data = pandas.DataFrame(data)
            data = data[data["manager_id"] != ""]
            for _, group in data.groupby(by=["manager_id", "so_date"]):
                data.loc[group.index, "so_count"] = len(
                    group[~group["lead"].isna()]["lead"].unique()
                )
            data["so_count"] = data["so_count"].apply(parse_int)
        else:
            data = None
        return data

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
    source_payments: pandas.DataFrame = None
    source_zoom: pandas.DataFrame = None
    source_so: pandas.DataFrame = None
    for sheet in (
        service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute().get("sheets")
    ):
        title = sheet.get("properties").get("title")
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
        if title == "Все оплаты":
            source_payments: pandas.DataFrame = processing_source_payments(
                pandas.DataFrame(data=items[1:], columns=items[0])
            )
        elif title == "Количество Zoom":
            source_zoom: pandas.DataFrame = processing_source_zoom(
                pandas.DataFrame(data=items[1:], columns=items[0])
            )
        elif title == "SpecialOffers":
            source_so: pandas.DataFrame = processing_source_so(
                pandas.DataFrame(data=items[1:], columns=items[0])
            )

    group_payments = source_payments[["manager_id", "group"]]
    group_zoom = source_zoom[["manager_id", "group"]]
    group_so = source_so[["manager_id", "group"]]
    groups = pandas.concat([group_payments, group_zoom, group_so])
    managers_group = {}
    for manager_id, group in groups.groupby(by=["manager_id"]):
        groups_count = dict(
            map(lambda item: (item[0], len(item[1])), group.groupby(by=["group"]))
        )
        groups_count = sorted(
            groups_count.items(), key=lambda item: item[1], reverse=True
        )
        managers_group[manager_id] = (
            groups_count[0][0] if len(groups_count) else pandas.NA
        )

    data_zoom = pandas.merge(
        source_payments[~source_payments["zoom_date"].isna()],
        source_zoom.rename(columns={"date": "zoom_date", "count": "zoom_count"}),
        how="outer",
        on=["manager_id", "zoom_date"],
    ).fillna("")
    data_zoom["manager_id"] = data_zoom["manager_id"].apply(parse_str)
    data_zoom["zoom_date"] = data_zoom["zoom_date"].apply(parse_date)
    data_zoom["zoom_count"] = data_zoom["zoom_count"].apply(parse_int)
    data_zoom["group_x"] = data_zoom["group_x"].apply(parse_str)
    data_zoom["group_y"] = data_zoom["group_y"].apply(parse_str)
    data_zoom["manager_x"] = data_zoom["manager_x"].apply(parse_str)
    data_zoom["manager_y"] = data_zoom["manager_y"].apply(parse_str)
    data_zoom["lead"] = data_zoom["lead"].apply(parse_int)
    data_zoom["order_id"] = data_zoom["order_id"].apply(parse_int)
    data_zoom["payment"] = data_zoom["payment"].apply(parse_int)
    data_zoom["payment_date"] = data_zoom["payment_date"].apply(parse_date)
    data_zoom["group"] = data_zoom[["group_x", "group_y"]].apply(merge_columns, axis=1)
    data_zoom["manager"] = data_zoom[["manager_x", "manager_y"]].apply(
        merge_columns, axis=1
    )
    data_zoom = data_zoom[
        [
            "manager_id",
            "manager",
            "group",
            "lead",
            "order_id",
            "payment",
            "payment_date",
            "zoom_date",
            "zoom_count",
        ]
    ]

    data_so = pandas.merge(
        source_payments, source_so, how="outer", on=["manager_id", "order_id"]
    ).fillna("")
    data_so["manager_id"] = data_so["manager_id"].apply(parse_str)
    data_so["order_id"] = data_so["order_id"].apply(parse_int)
    data_so["so_count"] = data_so["so_count"].apply(parse_int)
    data_so["so_date"] = data_so["so_date"].apply(parse_date)
    data_so["group_x"] = data_so["group_x"].apply(parse_str)
    data_so["group_y"] = data_so["group_y"].apply(parse_str)
    data_so["manager_x"] = data_so["manager_x"].apply(parse_str)
    data_so["manager_y"] = data_so["manager_y"].apply(parse_str)
    data_so["payment_x"] = data_so["payment_x"].apply(parse_int)
    data_so["payment_y"] = data_so["payment_y"].apply(parse_int)
    data_so["payment_date_x"] = data_so["payment_date_x"].apply(parse_date)
    data_so["payment_date_y"] = data_so["payment_date_y"].apply(parse_date)
    data_so["lead_x"] = data_so["lead_x"].apply(parse_int)
    data_so["lead_y"] = data_so["lead_y"].apply(parse_int)
    data_so["group"] = data_so[["group_x", "group_y"]].apply(merge_columns, axis=1)
    data_so["manager"] = data_so[["manager_x", "manager_y"]].apply(
        merge_columns, axis=1
    )
    data_so["payment"] = data_so[["payment_x", "payment_y"]].apply(
        merge_columns, axis=1
    )
    data_so["payment_date"] = data_so[["payment_date_x", "payment_date_y"]].apply(
        merge_columns, axis=1
    )
    data_so["lead"] = data_so[["lead_x", "lead_y"]].apply(merge_columns, axis=1)
    data_so = data_so[
        [
            "manager_id",
            "manager",
            "group",
            "lead",
            "order_id",
            "payment",
            "payment_date",
            "so_date",
            "so_count",
        ]
    ]

    data = pandas.merge(
        data_zoom, data_so, how="outer", on=["manager_id", "order_id", "lead"]
    )
    data["manager_id"] = data["manager_id"].apply(parse_str)
    data["order_id"] = data["order_id"].apply(parse_int)
    data["lead"] = data["lead"].apply(parse_int)
    data["group_x"] = data["group_x"].apply(parse_str)
    data["group_y"] = data["group_y"].apply(parse_str)
    data["manager_x"] = data["manager_x"].apply(parse_str)
    data["manager_y"] = data["manager_y"].apply(parse_str)
    data["payment_x"] = data["payment_x"].apply(parse_int)
    data["payment_y"] = data["payment_y"].apply(parse_int)
    data["payment_date_x"] = data["payment_date_x"].apply(parse_date)
    data["payment_date_y"] = data["payment_date_y"].apply(parse_date)
    data["group"] = data[["group_x", "group_y"]].apply(merge_columns, axis=1)
    data["manager"] = data[["manager_x", "manager_y"]].apply(merge_columns, axis=1)
    data["payment"] = data[["payment_x", "payment_y"]].apply(merge_columns, axis=1)
    data["payment_date"] = data[["payment_date_x", "payment_date_y"]].apply(
        merge_columns, axis=1
    )
    data = data[
        [
            "manager_id",
            "manager",
            "group",
            "lead",
            "order_id",
            "payment",
            "payment_date",
            "zoom_date",
            "zoom_count",
            "so_date",
            "so_count",
        ]
    ]
    for manager_id, group in managers_group.items():
        data.loc[data[data["manager_id"] == manager_id].index, "group"] = group
    data["payment"].fillna(0, inplace=True)
    data["zoom_count"].fillna(0, inplace=True)
    data["so_count"].fillna(0, inplace=True)
    data["group"].fillna("", inplace=True)

    with open(Path(DATA_PATH / "managers.pkl"), "wb") as file_ref:
        pickle.dump(data, file_ref)


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
get_managers_operator = PythonOperator(
    task_id="get_managers",
    python_callable=get_managers,
    dag=dag,
)

get_stats_operator >> calculate_operator
get_stats_operator >> update_so_operator
get_stats_operator >> calculate_zoom_operator
get_zoom_operator >> calculate_zoom_operator
update_so_operator >> get_so_operator
get_so_operator >> calculate_so_operator
get_stats_operator >> get_managers_operator
get_zoom_operator >> get_managers_operator
get_so_operator >> get_managers_operator
