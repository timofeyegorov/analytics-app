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


class StatisticsUTMGroupByEnum(Enum):
    utm_source = "UTM source"
    utm_medium = "UTM medium"
    utm_campaign = "UTM campaign"
    utm_term = "UTM term"
    utm_content = "UTM content"

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
    leads_month = "Лиды за месяц"
    income = "Оборот"
    income_month = "Оборот за месяц"
    ipl = "IPL"
    expenses = "Расход"
    expenses_month = "Расход за месяц"
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


class StatisticsUTMColumnEnum(Enum):
    name = "Название"
    leads = "Лиды"
    income = "Оборот"
    ipl = "IPL"

    @classmethod
    def dict(cls) -> Dict[str, str]:
        return dict(map(lambda item: (item.name, item.value), cls))


PACKAGES_COMPARE = {
    StatisticsRoistatPackageEnum.vk.name: {
        "id": "db",
        "visits_cost": "expenses",
        "date": "date",
        "package_id": "package",
        "level_1_id": "account",
        "level_2_id": "campaign",
        "level_3_id": "ad",
    },
    StatisticsRoistatPackageEnum.yandex_direct.name: {
        "id": "db",
        "visits_cost": "expenses",
        "date": "date",
        "package_id": "package",
        "level_1_id": "account",
        "level_3_id": "campaign",
        "level_4_id": "group",
        "level_5_id": "ad",
    },
    StatisticsRoistatPackageEnum.yandex_master.name: {
        "id": "db",
        "visits_cost": "expenses",
        "date": "date",
        "package_id": "package",
        "level_1_id": "account",
        "level_2_id": "campaign",
        "level_3_id": "group",
        "level_4_id": "ad",
    },
    StatisticsRoistatPackageEnum.facebook.name: {
        "id": "db",
        "visits_cost": "expenses",
        "date": "date",
        "package_id": "package",
        "level_1_id": "account",
        "level_2_id": "campaign",
        "level_3_id": "group",
        "level_4_id": "ad",
    },
    StatisticsRoistatPackageEnum.mytarget.name: {
        "id": "db",
        "visits_cost": "expenses",
        "date": "date",
        "package_id": "package",
        "level_1_id": "account",
        "level_2_id": "campaign",
        "level_3_id": "ad",
    },
    StatisticsRoistatPackageEnum.google.name: {
        "id": "db",
        "visits_cost": "expenses",
        "date": "date",
        "package_id": "package",
        "level_1_id": "account",
        "level_3_id": "campaign",
        "level_4_id": "group",
        "level_5_id": "ad",
    },
    StatisticsRoistatPackageEnum.site.name: {
        "id": "db",
        "visits_cost": "expenses",
        "date": "date",
        "package_id": "package",
        "level_1_id": "account",
        "level_2_id": "campaign",
        "level_3_id": "ad",
    },
    StatisticsRoistatPackageEnum.seo.name: {
        "id": "db",
        "visits_cost": "expenses",
        "date": "date",
        "package_id": "package",
        "level_1_id": "account",
        "level_2_id": "ad",
    },
    StatisticsRoistatPackageEnum.utm.name: {
        "id": "db",
        "visits_cost": "expenses",
        "date": "date",
        "package_id": "package",
        "level_1_id": "account",
        "level_2_id": "campaign",
        "level_3_id": "group",
        "level_4_id": "ad",
    },
    StatisticsRoistatPackageEnum.undefined.name: {
        "id": "db",
        "visits_cost": "expenses",
        "date": "date",
        "package_id": "package",
        "level_1_id": "account",
        "level_2_id": "campaign",
        "level_3_id": "group",
        "level_4_id": "ad",
    },
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
