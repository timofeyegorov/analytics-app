import re
import sys
import time
import numpy
import pandas
import requests

from math import ceil
from urllib.parse import urlparse, parse_qsl
from datetime import datetime
from html.parser import HTMLParser
from typing import Tuple, List, Dict, Any

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

sys.path.append(Variable.get("APP_FOLDER"))

from app.plugins.ads import vk
from app.dags.decorators import log_execution_time
from app.dags.vk import reader, writer, data


def get_full_period(
    method: str, ids: List[str], request_params: Dict[str, Any]
) -> Tuple[datetime, datetime]:
    def map_dates(name: str, dates) -> map:
        return map(
            lambda item: int(re.sub(r"-+", "", item.get("stats")[0].get(name)))
            if item.get("stats")
            else None,
            dates,
        )

    dates_from = []
    dates_to = []
    ids_list = numpy.array_split(ids, ceil(len(ids) / +200))
    for item in ids_list:
        print("dates", item)
        dates = vk(
            method,
            period="overall",
            date_from=0,
            date_to=0,
            ids=",".join(list(item)),
            **request_params,
        )
        dates_from += list(set(filter(None, map_dates("day_from", dates))))
        dates_to += list(set(filter(None, map_dates("day_to", dates))))
    return (
        datetime.strptime(str(min(dates_from)), "%Y%m%d"),
        datetime.strptime(str(max(dates_to)), "%Y%m%d"),
    )


def chr_convert(text: str) -> str:
    match = list(set(re.findall(r"(&#\d+;)", text)))
    for item in match:
        text = text.replace(item, chr(int(re.findall(r"&#(\d+);", text)[0])))
    return text


def string_to_list_int(text: str) -> List[int]:
    return list(
        map(lambda value: int(value), list(filter(None, re.split(r"[\D^-]+", text))))
    )


class PreviewPageParser(HTMLParser):
    in_title: bool = False
    in_text: bool = False

    title: str = ""
    text: str = ""
    image: str = ""
    target_url: str = ""

    def has_class(self, attrs: List[Tuple[str, str]], name: str) -> bool:
        classes = list(
            filter(
                lambda item: item[0] == "class" and name in re.split(r"\s+", item[1]),
                attrs,
            )
        )
        return len(classes) > 0

    def handle_starttag(self, tag, attrs):
        if tag == "a" and self.has_class(attrs, "media_link__title"):
            self.in_title = True

        if tag == "div" and self.has_class(attrs, "wall_post_text"):
            self.in_text = True
        if tag == "br" and self.in_text:
            self.text += "<br>"

        if tag in ["a", "span"] and self.has_class(attrs, "image_cover"):
            self.image = re.findall(
                r"url\(([^)]+)\)",
                list(filter(lambda item: item[0] == "style", attrs))[0][1],
            )[0]
        if tag == "img" and self.has_class(attrs, "media_link__photo"):
            self.image = list(filter(lambda item: item[0] == "src", attrs))[0][1]

        if tag == "a":
            if self.has_class(attrs, "media_link__button"):
                url = list(filter(lambda item: item[0] == "href", attrs))[0][1]
                if url.startswith("/away.php?"):
                    qs = dict(parse_qsl(urlparse(url).query))
                    target_url = qs.get("to", "")
                else:
                    target_url = url
                if target_url:
                    self.target_url = target_url

    def handle_endtag(self, tag):
        if tag == "a" and self.in_title:
            self.in_title = False
        if tag == "div" and self.in_text:
            self.in_text = False

    def handle_data(self, data):
        if self.in_title:
            self.title += str(data)
        if self.in_text:
            self.text += str(data)


def parse_ad_preview_page(url: str) -> Dict[str, str]:
    data = {"title": "", "text": "", "image": "", "target_url": ""}
    if url:
        response = requests.get(url)
        content = response.content.decode("cp1251")
        parser = PreviewPageParser()
        parser.feed(content)
        data.update(
            {
                "title": parser.title,
                "text": parser.text,
                "image": parser.image,
                "target_url": parser.target_url,
            }
        )
    return data


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
                            "account_id": account.account_id,
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
    output_wall = []
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
                for ad_layout in response:
                    output_wall.append(
                        {
                            "ad_id": ad_layout.get("id"),
                            **parse_ad_preview_page(ad_layout.get("preview_link")),
                        }
                    )
                    time.sleep(1)
                output += response
                time.sleep(1)
        else:
            response = vk(
                method,
                include_deleted=1,
                account_id=account.account_id,
            )
            for ad_layout in response:
                output_wall.append(
                    {
                        "ad_id": ad_layout.get("id"),
                        **parse_ad_preview_page(ad_layout.get("preview_link")),
                    }
                )
                time.sleep(1)
            output += response
            time.sleep(1)
    writer(method, output)
    writer("wall.get", output_wall)


@log_execution_time("ads.getAdsTargeting")
def ads_get_ads_targeting():
    def update_data(item: dict) -> dict:
        item.update(
            {
                "sex": int(item.get("sex")) if item.get("sex") else None,
                "cities": string_to_list_int(item.get("cities", "")),
                "cities_not": string_to_list_int(item.get("cities_not", "")),
                "user_os": string_to_list_int(item.get("user_os", "")),
                "apps": string_to_list_int(item.get("apps", "")),
                "retargeting_groups": string_to_list_int(
                    item.get("retargeting_groups", "")
                ),
                "retargeting_groups_not": string_to_list_int(
                    item.get("retargeting_groups_not", "")
                ),
                "groups": string_to_list_int(item.get("groups_active_formula", "")),
                "positions": string_to_list_int(item.get("positions", "")),
                "interest_categories": string_to_list_int(
                    item.get("interest_categories_formula", "")
                ),
            }
        )
        return item

    def write_countries(data: List[Dict[str, Any]]):
        ids = list(
            set(
                list(
                    filter(
                        None, list(map(lambda item: str(item.get("country", "")), data))
                    )
                )
            )
        )
        writer(
            "ads.getSuggestions.countries",
            vk("ads.getSuggestions", section="countries", ids=",".join(ids))
            if ids
            else [],
        )

    method = "ads.getAdsTargeting"
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
                for index, item in enumerate(response):
                    response[index] = update_data(item)
                output += response
                time.sleep(1)
        else:
            response = vk(
                method,
                include_deleted=1,
                account_id=account.account_id,
            )
            for index, item in enumerate(response):
                response[index] = update_data(item)
            output += response
            time.sleep(1)
    write_countries(output)
    writer(method, output)


@log_execution_time("ads.getDemographics")
def ads_get_demographics():
    method = "ads.getDemographics"
    ads = reader("ads.getAds")
    ads_accounts = list(map(lambda ad: (ad.id, ad.account_id), ads))
    groups = pandas.DataFrame(
        ads_accounts,
        columns=("id", "account_id"),
    ).groupby("account_id")
    output = []
    for account_id, group in groups:
        ids = list(group["id"].astype(str))
        print(ids)
        daterange = get_full_period(
            method,
            ids,
            {
                "account_id": account_id,
                "ids_type": "ad",
            },
        )
        time.sleep(1)

        for _id in ids:
            print(
                {
                    "account_id": account_id,
                    "ids_type": "ad",
                    "ids": _id,
                    "period": "day",
                    "date_from": daterange[0].strftime("%Y-%m-%d"),
                    "date_to": daterange[1].strftime("%Y-%m-%d"),
                }
            )
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
                    age = stat.get("age", [])
                    # cities = stat.get("cities", [])
                    output.append(
                        {
                            "id": ad_id,
                            "date": datetime.strptime(stat.get("day"), "%Y-%m-%d"),
                            "sex": dict(
                                map(lambda item: (item.get("value"), item), sex)
                            ),
                            "age": dict(
                                map(
                                    lambda item: (
                                        f'id_{re.sub(r"-+", "_", item.get("value"))}',
                                        item,
                                    ),
                                    age,
                                )
                            ),
                            # "cities": dict(
                            #     map(
                            #         lambda item: (f'id_{item.get("value")}', item),
                            #         cities,
                            #     )
                            # ),
                        }
                    )
            time.sleep(1)

    writer(method, output)


@log_execution_time("ads.getStatistics")
def ads_get_statistics():
    method = "ads.getStatistics"
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
            ids,
            {
                "account_id": account_id,
                "ids_type": "ad",
            },
        )
        time.sleep(1)

        for _id in ids:
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
                ad = ads_dict.get(ad_id)
                for stat in statistic.get("stats", []):
                    output.append(
                        {
                            **stat,
                            "id": ad_id,
                            "account_id": ad.account_id,
                            "client_id": ad.client_id,
                            "campaign_id": ad.campaign_id,
                            "date": datetime.strptime(stat.get("day"), "%Y-%m-%d"),
                        }
                    )
            time.sleep(1)

    writer(method, output)


@log_execution_time("ads.getSuggestions.positions")
def ads_get_suggestions_positions():
    writer(
        "ads.getSuggestions.positions", vk("ads.getSuggestions", section="positions")
    )


@log_execution_time("ads.getSuggestions.interest_categories_v2")
def ads_get_suggestions_interest_categories_v2():
    writer(
        "ads.getSuggestions.interest_categories_v2",
        vk("ads.getSuggestions", section="interest_categories_v2"),
    )


@log_execution_time("collectStatisticsDataFrame")
def collect_statistics_dataframe():
    demographics = reader("ads.getDemographics")
    statistics = reader("ads.getStatistics")
    demographics_dict = {}
    # cities_dict = {}

    for item in demographics:
        info = item.dict()

        sex = info.pop("sex")
        sex_dict = {}
        for sex_id, sex_info in sex.items():
            sex_dict.update(
                dict(
                    map(
                        lambda stat: (f"sex__{sex_id}__{stat[0]}", stat[1]),
                        sex_info.items(),
                    )
                )
            )

        age = info.pop("age")
        age_dict = {}
        for age_id, age_info in age.items():
            age_id = re.sub(r"^id_", "", age_id)
            age_dict.update(
                dict(
                    map(
                        lambda stat: (f"age__{age_id}__{stat[0]}", stat[1]),
                        age_info.items(),
                    )
                )
            )

        # cities = info.pop("cities")
        # cities_dict_info = {}
        # for city_id, city_info in cities.items():
        #     city_id = int(re.sub(r"^id_", "", city_id))
        #     city_name = city_info.pop("name")
        #     cities_dict[city_id] = city_name
        #     cities_dict_info.update(
        #         dict(
        #             map(
        #                 lambda stat: (f"city__{city_id}__{stat[0]}", stat[1]),
        #                 city_info.items(),
        #             )
        #         )
        #     )

        demographics_dict[f'{item.date.strftime("%Y%m%d")}_{item.id}'] = {
            **info,
            **sex_dict,
            **age_dict,
            # **cities_dict_info,
        }

    # writer(
    #     "collectCities",
    #     list(map(lambda item: {"id": item[0], "name": item[1]}, cities_dict.items())),
    # )
    writer(
        "collectStatisticsDataFrame",
        list(
            map(
                lambda item: {
                    **demographics_dict.get(
                        f'{item.date.strftime("%Y%m%d")}_{item.id}', {}
                    ),
                    **item.dict(),
                },
                statistics,
            )
        ),
    )


dag = DAG(
    "api_data_vk",
    description="Collect VK API data",
    schedule_interval="0 2,6,10,14,18,22 * * *",
    start_date=datetime(2017, 3, 20),
    catchup=False,
)


# ads_get_accounts_operator = PythonOperator(
#     task_id="ads_get_accounts", python_callable=ads_get_accounts, dag=dag
# )
# ads_get_clients_operator = PythonOperator(
#     task_id="ads_get_clients", python_callable=ads_get_clients, dag=dag
# )
# ads_get_campaigns_operator = PythonOperator(
#     task_id="ads_get_campaigns", python_callable=ads_get_campaigns, dag=dag
# )
# ads_get_target_groups_operator = PythonOperator(
#     task_id="ads_get_target_groups", python_callable=ads_get_target_groups, dag=dag
# )
# ads_get_ads_operator = PythonOperator(
#     task_id="ads_get_ads", python_callable=ads_get_ads, dag=dag
# )
# ads_get_ads_layout_operator = PythonOperator(
#     task_id="ads_get_ads_layout", python_callable=ads_get_ads_layout, dag=dag
# )
# ads_get_ads_targeting_operator = PythonOperator(
#     task_id="ads_get_ads_targeting", python_callable=ads_get_ads_targeting, dag=dag
# )
ads_get_demographics_operator = PythonOperator(
    task_id="ads_get_demographics", python_callable=ads_get_demographics, dag=dag
)
# ads_get_statistics_operator = PythonOperator(
#     task_id="ads_get_statistics", python_callable=ads_get_statistics, dag=dag
# )
# collect_statistics_dataframe_operator = PythonOperator(
#     task_id="collect_statistics_dataframe",
#     python_callable=collect_statistics_dataframe,
#     dag=dag,
# )
# ads_get_suggestions_positions_operator = PythonOperator(
#     task_id="ads_get_suggestions_positions",
#     python_callable=ads_get_suggestions_positions,
#     dag=dag,
# )
# ads_get_suggestions_interest_categories_v2_operator = PythonOperator(
#     task_id="ads_get_suggestions_interest_categories_v2",
#     python_callable=ads_get_suggestions_interest_categories_v2,
#     dag=dag,
# )


# ads_get_accounts_operator >> ads_get_clients_operator
#
# ads_get_accounts_operator >> ads_get_campaigns_operator
# ads_get_clients_operator >> ads_get_campaigns_operator
#
# ads_get_accounts_operator >> ads_get_target_groups_operator
# ads_get_clients_operator >> ads_get_target_groups_operator
#
# ads_get_accounts_operator >> ads_get_ads_operator
# ads_get_clients_operator >> ads_get_ads_operator
#
# ads_get_accounts_operator >> ads_get_ads_layout_operator
# ads_get_clients_operator >> ads_get_ads_layout_operator
#
# ads_get_accounts_operator >> ads_get_ads_targeting_operator
# ads_get_clients_operator >> ads_get_ads_targeting_operator
#
# ads_get_ads_operator >> ads_get_demographics_operator
#
# ads_get_ads_operator >> ads_get_statistics_operator
#
# ads_get_demographics_operator >> collect_statistics_dataframe_operator
# ads_get_statistics_operator >> collect_statistics_dataframe_operator
