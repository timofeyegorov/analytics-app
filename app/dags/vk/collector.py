import sys
import time

from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

sys.path.append(Variable.get("APP_FOLDER"))

from app.plugins.ads import vk
from app.dags.decorators import log_execution_time
from app.dags.vk import reader, writer


@log_execution_time("ads.getAccounts")
def ads_get_accounts():
    method = "ads.getAccounts"
    response = vk(method)
    writer(method, response)


@log_execution_time("ads.getClients")
def ads_get_clients():
    method = "ads.getClients"
    accounts = reader("ads.getAccounts")
    response = []
    for account in accounts:
        response += list(
            map(
                lambda client: {"account_id": account.account_id, **client},
                vk(method, account_id=account.account_id),
            )
        )
        time.sleep(1)
    writer(method, response)


dag = DAG(
    "api_data_vk",
    description="Collect VK API data",
    schedule_interval="30 */4 * * *",
    start_date=datetime(2017, 3, 20),
    catchup=False,
)

ads_get_accounts_operator = PythonOperator(
    task_id="ads_get_accounts", python_callable=ads_get_accounts, dag=dag
)
ads_get_clients_operator = PythonOperator(
    task_id="ads_get_clients", python_callable=ads_get_clients, dag=dag
)

ads_get_accounts_operator >> ads_get_clients_operator
