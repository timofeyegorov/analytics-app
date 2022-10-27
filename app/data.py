from enum import Enum
from typing import Dict, Union
from pandas import DataFrame
from pydantic import BaseModel, PositiveInt


class StatisticsProviderEnum(Enum):
    vk = "ВКонтакте"
    yandex = "Яндекс"
    tg = "Телеграм"

    @classmethod
    def dict(cls) -> Dict[str, str]:
        return dict(map(lambda item: (item.name, item.value), cls))


class StatisticsGroupByEnum(Enum):
    provider = "Провайдер"
    account = "Кабинет"
    campaign = "Кампания"
    group = "Группа объявлений"
    ad = "Объявление"

    @classmethod
    def dict(cls) -> Dict[str, str]:
        return dict(map(lambda item: (item.name, item.value), cls))


class StatisticsRoistatGroupByEnum(Enum):
    account = "Кабинет"
    campaign = "Кампания"
    group = "Группа объявлений"
    ad = "Объявление"

    @classmethod
    def dict(cls) -> Dict[str, str]:
        return dict(map(lambda item: (item.name, item.value), cls))


class StatisticsRoistatPackageEnum(Enum):
    vk = "ВКонтакте"
    yandex_direct = "Яндекс.Директ"
    yandex_master = "Яндекс.Мастер"
    facebook = "Facebook"
    mytarget = "MyTarget"
    google = "Google"
    site = "Сайт"
    seo = "SEO"
    utm = "UTM"
    undefined = "Undefined"

    @classmethod
    def dict(cls) -> Dict[str, str]:
        return dict(map(lambda item: (item.name, item.value), cls))


class CalculateColumnEnum(Enum):
    name = "Название"
    leads = "Лиды"
    income = "Оборот"
    ipl = "IPL"
    expenses = "Расход"
    profit = "Прибыль"
    ppl = "PPL"
    cpl = "CPL"
    ppl_range = "PPL range"
    ppl_30d = "PPL 30d"
    leads_range = "Лиды range"
    leads_30d = "Лиды 30d"
    action = "Действие"

    @classmethod
    def dict(cls) -> Dict[str, str]:
        return dict(map(lambda item: (item.name, item.value), cls))


PACKAGES_COMPARE = {
    StatisticsRoistatPackageEnum.vk: (
        [
            "date",
            "package",
            "marker_level_1",
            "marker_level_2",
            "marker_level_3",
            "marker_level_1_title",
            "marker_level_2_title",
            "marker_level_3_title",
            "visitsCost",
        ],
        {
            "marker_level_1": "account",
            "marker_level_2": "campaign",
            "marker_level_3": "ad",
            "marker_level_1_title": "account_title",
            "marker_level_2_title": "campaign_title",
            "marker_level_3_title": "ad_title",
            "visitsCost": "expenses",
        },
    ),
    StatisticsRoistatPackageEnum.yandex_direct: (
        [
            "date",
            "package",
            "marker_level_1",
            "marker_level_3",
            "marker_level_4",
            "marker_level_5",
            "marker_level_1_title",
            "marker_level_3_title",
            "marker_level_4_title",
            "marker_level_5_title",
            "visitsCost",
        ],
        {
            "marker_level_1": "account",
            "marker_level_3": "campaign",
            "marker_level_4": "group",
            "marker_level_5": "ad",
            "marker_level_1_title": "account_title",
            "marker_level_3_title": "campaign_title",
            "marker_level_4_title": "group_title",
            "marker_level_5_title": "ad_title",
            "visitsCost": "expenses",
        },
    ),
    StatisticsRoistatPackageEnum.yandex_master: (
        [
            "date",
            "package",
            "marker_level_1",
            "marker_level_2",
            "marker_level_3",
            "marker_level_4",
            "marker_level_1_title",
            "marker_level_2_title",
            "marker_level_3_title",
            "marker_level_4_title",
            "visitsCost",
        ],
        {
            "marker_level_1": "account",
            "marker_level_2": "campaign",
            "marker_level_3": "group",
            "marker_level_4": "ad",
            "marker_level_1_title": "account_title",
            "marker_level_2_title": "campaign_title",
            "marker_level_3_title": "group_title",
            "marker_level_4_title": "ad_title",
            "visitsCost": "expenses",
        },
    ),
    StatisticsRoistatPackageEnum.facebook: (
        [
            "date",
            "package",
            "marker_level_1",
            "marker_level_2",
            "marker_level_3",
            "marker_level_4",
            "marker_level_1_title",
            "marker_level_2_title",
            "marker_level_3_title",
            "marker_level_4_title",
            "visitsCost",
        ],
        {
            "marker_level_1": "account",
            "marker_level_2": "campaign",
            "marker_level_3": "group",
            "marker_level_4": "ad",
            "marker_level_1_title": "account_title",
            "marker_level_2_title": "campaign_title",
            "marker_level_3_title": "group_title",
            "marker_level_4_title": "ad_title",
            "visitsCost": "expenses",
        },
    ),
    StatisticsRoistatPackageEnum.mytarget: (
        [
            "date",
            "package",
            "marker_level_1",
            "marker_level_2",
            "marker_level_3",
            "marker_level_1_title",
            "marker_level_2_title",
            "marker_level_3_title",
            "visitsCost",
        ],
        {
            "marker_level_1": "account",
            "marker_level_2": "campaign",
            "marker_level_3": "ad",
            "marker_level_1_title": "account_title",
            "marker_level_2_title": "campaign_title",
            "marker_level_3_title": "ad_title",
            "visitsCost": "expenses",
        },
    ),
    StatisticsRoistatPackageEnum.google: (
        [
            "date",
            "package",
            "marker_level_1",
            "marker_level_3",
            "marker_level_4",
            "marker_level_5",
            "marker_level_1_title",
            "marker_level_3_title",
            "marker_level_4_title",
            "marker_level_5_title",
            "visitsCost",
        ],
        {
            "marker_level_1": "account",
            "marker_level_3": "campaign",
            "marker_level_4": "group",
            "marker_level_5": "ad",
            "marker_level_1_title": "account_title",
            "marker_level_3_title": "campaign_title",
            "marker_level_4_title": "group_title",
            "marker_level_5_title": "ad_title",
            "visitsCost": "expenses",
        },
    ),
    StatisticsRoistatPackageEnum.site: (
        [
            "date",
            "package",
            "marker_level_1",
            "marker_level_2",
            "marker_level_3",
            "marker_level_1_title",
            "marker_level_2_title",
            "marker_level_3_title",
            "visitsCost",
        ],
        {
            "marker_level_1": "account",
            "marker_level_2": "campaign",
            "marker_level_3": "ad",
            "marker_level_1_title": "account_title",
            "marker_level_2_title": "campaign_title",
            "marker_level_3_title": "ad_title",
            "visitsCost": "expenses",
        },
    ),
    StatisticsRoistatPackageEnum.seo: (
        [
            "date",
            "package",
            "marker_level_1",
            "marker_level_2",
            "marker_level_1_title",
            "marker_level_2_title",
            "visitsCost",
        ],
        {
            "marker_level_1": "account",
            "marker_level_2": "ad",
            "marker_level_1_title": "account_title",
            "marker_level_2_title": "ad_title",
            "visitsCost": "expenses",
        },
    ),
    StatisticsRoistatPackageEnum.utm: (
        [
            "date",
            "package",
            "marker_level_1",
            "marker_level_2",
            "marker_level_3",
            "marker_level_4",
            "marker_level_1_title",
            "marker_level_2_title",
            "marker_level_3_title",
            "marker_level_4_title",
            "visitsCost",
        ],
        {
            "marker_level_1": "account",
            "marker_level_2": "campaign",
            "marker_level_3": "group",
            "marker_level_4": "ad",
            "marker_level_1_title": "account_title",
            "marker_level_2_title": "campaign_title",
            "marker_level_4_title": "group_title",
            "marker_level_3_title": "ad_title",
            "visitsCost": "expenses",
        },
    ),
    StatisticsRoistatPackageEnum.undefined: (
        [
            "date",
            "package",
            "marker_level_1",
            "marker_level_2",
            "marker_level_3",
            "marker_level_4",
            "marker_level_1_title",
            "marker_level_2_title",
            "marker_level_3_title",
            "marker_level_4_title",
            "visitsCost",
        ],
        {
            "marker_level_1": "account",
            "marker_level_2": "campaign",
            "marker_level_3": "group",
            "marker_level_4": "ad",
            "marker_level_1_title": "account_title",
            "marker_level_2_title": "campaign_title",
            "marker_level_4_title": "group_title",
            "marker_level_3_title": "ad_title",
            "visitsCost": "expenses",
        },
    ),
}


class StatisticsBaseModel(BaseModel):
    class Config:
        arbitrary_types_allowed = True


class StatisticsData(StatisticsBaseModel):
    data: DataFrame
    accounts: Dict[str, Dict[PositiveInt, Dict[str, Union[int, str]]]]
    campaigns: Dict[str, Dict[PositiveInt, Dict[str, Union[int, str]]]]
    groups: Dict[str, Dict[PositiveInt, Dict[str, Union[int, str]]]]
    ads: Dict[str, Dict[PositiveInt, Dict[str, Union[int, str]]]]
