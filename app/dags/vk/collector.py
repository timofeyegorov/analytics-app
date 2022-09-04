import sys
import time

from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

sys.path.append(Variable.get("APP_FOLDER"))

from app.plugins.ads import vk
from app.dags.decorators import log_execution_time
from app.dags.vk import reader, writer, data


@log_execution_time("ads.getAccounts")
def ads_get_accounts():
    method = "ads.getAccounts"
    response = vk(method)
    writer(method, response)


@log_execution_time("ads.getClients")
def ads_get_clients():
    method = "ads.getClients"
    response = []
    for account in reader("ads.getAccounts"):
        response += list(
            map(
                lambda client: {"account_id": account.account_id, **client},
                vk(method, account_id=account.account_id),
            )
        )
        time.sleep(1)
    writer(method, response)


@log_execution_time("ads.getCampaigns")
def ads_get_campaigns():
    method = "ads.getCampaigns"
    accounts = reader("ads.getAccounts")
    clients = reader("ads.getClients")
    response = []
    for account in accounts:
        if account.account_type == data.AccountTypeEnum.agency:
            account_clients = list(
                filter(lambda client: client.account_id == account.account_id, clients)
            )
            for client in account_clients:
                response += list(
                    map(
                        lambda campaign: {
                            "account_id": client.account_id,
                            "client_id": client.id,
                            **campaign,
                        },
                        vk(method, account_id=account.account_id, client_id=client.id),
                    )
                )
                time.sleep(1)
        else:
            response += list(
                map(
                    lambda campaign: {
                        "account_id": account.account_id,
                        **campaign,
                    },
                    vk(method, account_id=account.account_id),
                )
            )
            time.sleep(1)
    writer(method, response)


@log_execution_time("ads.getTargetGroups")
def ads_get_target_groups():
    method = "ads.getTargetGroups"
    accounts = reader("ads.getAccounts")
    clients = reader("ads.getClients")
    response = []
    for account in accounts:
        if account.account_type == data.AccountTypeEnum.agency:
            account_clients = list(
                filter(lambda client: client.account_id == account.account_id, clients)
            )
            for client in account_clients:
                response += list(
                    map(
                        lambda target_group: {
                            "account_id": client.account_id,
                            "client_id": client.id,
                            **target_group,
                        },
                        vk(method, account_id=client.account_id, client_id=client.id),
                    )
                )
                time.sleep(1)
        else:
            response += list(
                map(
                    lambda target_group: {
                        "account_id": account.account_id,
                        **target_group,
                    },
                    vk(method, account_id=account.account_id),
                )
            )
            time.sleep(1)
    writer(method, response)


@log_execution_time("ads.getAds")
def ads_get_ads():
    method = "ads.getAds"
    accounts = reader("ads.getAccounts")
    clients = reader("ads.getClients")
    response = []
    for account in accounts:
        if account.account_type == data.AccountTypeEnum.agency:
            account_clients = list(
                filter(lambda client: client.account_id == account.account_id, clients)
            )
            for client in account_clients:
                response += list(
                    map(
                        lambda ad: {
                            "account_id": client.account_id,
                            "client_id": client.id,
                            **ad,
                        },
                        vk(method, account_id=client.account_id, client_id=client.id),
                    )
                )
                time.sleep(1)
        else:
            response += list(
                map(
                    lambda ad: {
                        "account_id": account.account_id,
                        **ad,
                    },
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
ads_get_campaigns_operator = PythonOperator(
    task_id="ads_get_campaigns", python_callable=ads_get_campaigns, dag=dag
)
ads_get_target_groups_operator = PythonOperator(
    task_id="ads_get_target_groups", python_callable=ads_get_target_groups, dag=dag
)
ads_get_ads_operator = PythonOperator(
    task_id="ads_get_ads", python_callable=ads_get_ads, dag=dag
)

ads_get_accounts_operator >> ads_get_clients_operator
ads_get_accounts_operator >> ads_get_campaigns_operator
ads_get_accounts_operator >> ads_get_target_groups_operator
ads_get_accounts_operator >> ads_get_ads_operator
ads_get_clients_operator >> ads_get_campaigns_operator
ads_get_clients_operator >> ads_get_target_groups_operator
ads_get_clients_operator >> ads_get_ads_operator
ads_get_target_groups_operator >> ads_get_ads_operator
