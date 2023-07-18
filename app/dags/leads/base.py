import os
import sys
import pandas
import pickle
import httplib2
import apiclient

from pathlib import Path
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

sys.path.append(Variable.get("APP_FOLDER"))

from config import DATA_FOLDER, CREDENTIALS_FILE
from app.dags.decorators import log_execution_time


@log_execution_time("update")
def update():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE,
        [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    http_auth = credentials.authorize(httplib2.Http())
    service = apiclient.discovery.build("sheets", "v4", http=http_auth)
    spreadsheet_id = "1YhNHABZ99jiiB7_zHmIBlEDzigT9n0-gICmDWbivuFo"
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

        target_path = Path(DATA_FOLDER) / "api" / "tilda"
        os.makedirs(target_path, exist_ok=True)
        target_file = target_path / "leads.pkl"

        with open(target_file, "wb") as file_ref:
            pickle.dump(pandas.DataFrame(items[1:], columns=items[0]), file_ref)


dag = DAG(
    "leads",
    description="Update leads",
    schedule_interval="*/10 9-22 * * *",
    start_date=datetime(2017, 3, 20),
    catchup=False,
)


update_operator = PythonOperator(
    task_id="update",
    python_callable=update,
    dag=dag,
)
