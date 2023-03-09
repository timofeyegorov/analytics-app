import os
import sys
import json
import pandas
import pickle

from pathlib import Path
from typing import Tuple, List, Dict, Any

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

sys.path.append(Variable.get("APP_FOLDER"))

from config import DATA_FOLDER
from app.dags.decorators import log_execution_time


@log_execution_time("update")
def update():
    def read_json(value: Path) -> Tuple[Dict[str, Any], Path]:
        with open(value, "r") as file_ref:
            return json.load(file_ref), value

    target_path = Path(DATA_FOLDER) / "api" / "tilda"
    os.makedirs(target_path, exist_ok=True)

    source_path = target_path / "sources"
    os.makedirs(source_path, exist_ok=True)

    queue = []
    for item in os.listdir(source_path):
        json_path: Path = source_path / item
        if json_path.suffix == ".json":
            queue.append(json_path)

    if not len(queue):
        return

    sources_list: List[Tuple[Dict[str, Any], Path]] = list(map(read_json, queue))

    target_file = target_path / "leads.pkl"
    try:
        with open(target_file, "rb") as file_ref:
            data: pandas.DataFrame = pickle.load(file_ref)
    except FileNotFoundError:
        data: pandas.DataFrame = pandas.DataFrame()

    data = pandas.concat(
        [data, pandas.DataFrame(list(map(lambda item: item[0], sources_list)))],
        ignore_index=True,
    )

    with open(target_file, "wb") as file_ref:
        pickle.dump(data, file_ref)

    list(map(os.remove, list(map(lambda item: item[1], sources_list))))


dag = DAG(
    "leads",
    description="Update leads",
    schedule_interval="*/10 * * * *",
    start_date=datetime(2017, 3, 20),
    catchup=False,
)


update_operator = PythonOperator(
    task_id="update",
    python_callable=update,
    dag=dag,
)
