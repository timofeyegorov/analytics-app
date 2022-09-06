import json
import re
import sys
import time
import pandas

from datetime import datetime
from typing import Tuple, List, Dict, Any

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

sys.path.append(Variable.get("APP_FOLDER"))

from app.plugins.ads import vk
from app.dags.decorators import log_execution_time
from app.dags.vk import reader, writer, data


def get_full_period(
    method: str, request_params: Dict[str, Any]
) -> Tuple[datetime, datetime]:
    def map_dates(name: str, dates) -> map:
        return map(
            lambda item: int(re.sub(r"-+", "", item.get("stats")[0].get(name)))
            if item.get("stats")
            else None,
            dates,
        )

    dates = vk(
        method,
        period="overall",
        date_from=0,
        date_to=0,
        **request_params,
    )
    date_from = min(list(set(filter(None, map_dates("day_from", dates)))))
    date_to = max(list(set(filter(None, map_dates("day_to", dates)))))
    return (
        datetime.strptime(str(date_from), "%Y%m%d"),
        datetime.strptime(str(date_to), "%Y%m%d"),
    )


@log_execution_time("ads.getAccounts")
def ads_get_accounts():
    method = "ads.getAccounts"
    response = vk(method)
    writer(method, response)


@log_execution_time("ads.getClients")
def ads_get_clients():
    method = "ads.getClients"
    accounts = reader("ads.getAccounts")
    output = []
    for account in accounts:
        response = vk(
            method,
            account_id=account.account_id,
        )
        output += list(
            map(lambda client: {"account_id": account.account_id, **client}, response)
        )
        time.sleep(1)
    writer(method, output)


@log_execution_time("ads.getCampaigns")
def ads_get_campaigns():
    method = "ads.getCampaigns"
    accounts = reader("ads.getAccounts")
    clients = reader("ads.getClients")
    output = []
    for account in accounts:
        if account.account_type == data.AccountTypeEnum.agency:
            account_clients = list(
                filter(lambda client: client.account_id == account.account_id, clients)
            )
            for client in account_clients:
                response = vk(
                    method,
                    include_deleted=1,
                    account_id=account.account_id,
                    client_id=client.id,
                )
                output += list(
                    map(
                        lambda campaign: {
                            "account_id": client.account_id,
                            "client_id": client.id,
                            **campaign,
                        },
                        response,
                    )
                )
                time.sleep(1)
        else:
            response = vk(
                method,
                include_deleted=1,
                account_id=account.account_id,
            )
            output += list(
                map(
                    lambda campaign: {
                        "account_id": account.account_id,
                        **campaign,
                    },
                    response,
                )
            )
            time.sleep(1)
    writer(method, output)


@log_execution_time("ads.getTargetGroups")
def ads_get_target_groups():
    method = "ads.getTargetGroups"
    accounts = reader("ads.getAccounts")
    clients = reader("ads.getClients")
    output = []
    for account in accounts:
        if account.account_type == data.AccountTypeEnum.agency:
            account_clients = list(
                filter(lambda client: client.account_id == account.account_id, clients)
            )
            for client in account_clients:
                response = vk(
                    method,
                    account_id=client.account_id,
                    client_id=client.id,
                    extended=1,
                )
                output += list(
                    map(
                        lambda target_group: {
                            "account_id": client.account_id,
                            "client_id": client.id,
                            **target_group,
                        },
                        response,
                    )
                )
                time.sleep(1)
        else:
            response = vk(
                method,
                account_id=account.account_id,
                extended=1,
            )
            output += list(
                map(
                    lambda target_group: {
                        "account_id": account.account_id,
                        **target_group,
                    },
                    response,
                )
            )
            time.sleep(1)
    writer(method, output)


@log_execution_time("ads.getAds")
def ads_get_ads():
    method = "ads.getAds"
    accounts = reader("ads.getAccounts")
    clients = reader("ads.getClients")
    output = []
    for account in accounts:
        if account.account_type == data.AccountTypeEnum.agency:
            account_clients = list(
                filter(lambda client: client.account_id == account.account_id, clients)
            )
            for client in account_clients:
                response = vk(
                    method,
                    include_deleted=1,
                    account_id=client.account_id,
                    client_id=client.id,
                )
                output += list(
                    map(
                        lambda ad: {
                            "account_id": client.account_id,
                            "client_id": client.id,
                            **ad,
                        },
                        response,
                    )
                )
                time.sleep(1)
        else:
            response = vk(
                method,
                include_deleted=1,
                account_id=account.account_id,
            )
            output += list(
                map(
                    lambda ad: {
                        "account_id": account.account_id,
                        **ad,
                    },
                    response,
                )
            )
            time.sleep(1)
    writer(method, output)


@log_execution_time("ads.getAdsLayout")
def ads_get_ads_layout():
    method = "ads.getAdsLayout"
    accounts = reader("ads.getAccounts")
    clients = reader("ads.getClients")
    output = []
    for account in accounts:
        if account.account_type == data.AccountTypeEnum.agency:
            account_clients = list(
                filter(lambda client: client.account_id == account.account_id, clients)
            )
            for client in account_clients:
                output += vk(
                    method,
                    include_deleted=1,
                    account_id=client.account_id,
                    client_id=client.id,
                )
                time.sleep(1)
        else:
            output += vk(
                method,
                include_deleted=1,
                account_id=account.account_id,
            )
            time.sleep(1)
    writer(method, output)


@log_execution_time("ads.getDemographics")
def ads_get_demographics():
    method = "ads.getDemographics"
    ads = reader("ads.getAds")
    ads_dict = dict(map(lambda ad: (ad.id, ad), ads))
    ads_accounts = list(map(lambda ad: (ad.id, ad.account_id), ads))
    groups = pandas.DataFrame(
        ads_accounts,
        columns=("id", "account_id"),
    ).groupby("account_id")
    output = []
    for account_id, group in groups:
        ids = list(group["id"].astype(str))
        daterange = get_full_period(
            method,
            {
                "account_id": account_id,
                "ids_type": "ad",
                "ids": ",".join(ids),
            },
        )
        time.sleep(1)

        for _id in ids[:1]:
            statistics = vk(
                method,
                account_id=account_id,
                ids_type="ad",
                ids=_id,
                period="day",
                date_from=daterange[0].strftime("%Y-%m-%d"),
                date_to=daterange[1].strftime("%Y-%m-%d"),
            )
            for statistic in statistics:
                ad_id = statistic.get("id")
                for stat in statistic.get("stats", []):
                    sex = stat.get("sex", [])
                    output += {
                        "ad_id": ad_id,
                        "date": datetime.strptime(stat.get("day"), "%Y-%m-%d"),
                        "sex": dict(map(lambda item: (item.get("value"), item), sex)),
                    }
            time.sleep(1)

        # time.sleep(1)
        # statistics = vk(
        #     method,
        #     period="day",
        #     date_from=daterange[0].strftime("%Y-%m-%d"),
        #     date_to=daterange[1].strftime("%Y-%m-%d"),
        #     **request_params,
        # )
        # for statistic in statistics:
        #     print(statistic)
        #     ad = ads_dict.get(statistic.get("id"))
        # time.sleep(1)
    writer(method, output)


@log_execution_time("ads.getStatistics")
def ads_get_statistics():
    pass
    # method = "ads.getStatistics"
    # ads = reader("ads.getAds")
    # ads_dict = dict(map(lambda ad: (ad.id, ad), ads))
    # response = []
    # groups = list(map(lambda ad: (ad.id, ad.account_id), ads))
    # data = pandas.DataFrame(groups, columns=("id", "account_id"))
    # for account_id, group in data.groupby("account_id"):
    #     ids = ",".join(list(group["id"].astype(str)))
    #     request_params = {
    #         "account_id": account_id,
    #         "ids_type": "ad",
    #         "ids": ids,
    #     }
    #     daterange_match = vk(
    #         method,
    #         period="overall",
    #         date_from=0,
    #         date_to=0,
    #         **request_params,
    #     )
    #     date_from = str(min(list(set(get_map_dates("day_from", daterange_match)))))
    #     date_to = str(max(list(set(get_map_dates("day_to", daterange_match)))))
    #     date_from = f"{date_from[:4]}-{date_from[4:6]}-{date_from[6:8]}"
    #     date_to = f"{date_to[:4]}-{date_to[4:6]}-{date_to[6:8]}"
    #     time.sleep(1)
    #     statistics = vk(
    #         method,
    #         period="day",
    #         date_from=date_from,
    #         date_to=date_to,
    #         **request_params,
    #     )
    #     for statistic in statistics:
    #         ad = ads_dict.get(statistic.get("id"))
    #         response += list(
    #             map(
    #                 lambda stat: {
    #                     **stat,
    #                     "ad_id": ad.id,
    #                     "account_id": ad.account_id,
    #                     "client_id": ad.client_id,
    #                     "campaign_id": ad.campaign_id,
    #                     "date": datetime.strptime(stat.get("day"), "%Y-%m-%d"),
    #                 },
    #                 statistic.get("stats"),
    #             )
    #         )
    #     time.sleep(1)
    # writer(method, response)


dag = DAG(
    "api_data_vk",
    description="Collect VK API data",
    schedule_interval="0 2,6,10,14,18,22 * * *",
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
ads_get_ads_layout_operator = PythonOperator(
    task_id="ads_get_ads_layout", python_callable=ads_get_ads_layout, dag=dag
)
ads_get_demographics_operator = PythonOperator(
    task_id="ads_get_demographics", python_callable=ads_get_demographics, dag=dag
)
ads_get_statistics_operator = PythonOperator(
    task_id="ads_get_statistics", python_callable=ads_get_statistics, dag=dag
)

ads_get_accounts_operator >> ads_get_clients_operator

ads_get_accounts_operator >> ads_get_campaigns_operator
ads_get_clients_operator >> ads_get_campaigns_operator

ads_get_accounts_operator >> ads_get_target_groups_operator
ads_get_clients_operator >> ads_get_target_groups_operator

ads_get_accounts_operator >> ads_get_ads_operator
ads_get_clients_operator >> ads_get_ads_operator

ads_get_accounts_operator >> ads_get_ads_layout_operator
ads_get_clients_operator >> ads_get_ads_layout_operator

ads_get_ads_operator >> ads_get_demographics_operator

ads_get_ads_operator >> ads_get_statistics_operator
