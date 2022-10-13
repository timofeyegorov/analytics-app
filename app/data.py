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


class StatisticsBaseModel(BaseModel):
    class Config:
        arbitrary_types_allowed = True


class StatisticsData(StatisticsBaseModel):
    data: DataFrame
    accounts: Dict[str, Dict[PositiveInt, Dict[str, Union[int, str]]]]
    campaigns: Dict[str, Dict[PositiveInt, Dict[str, Union[int, str]]]]
    groups: Dict[str, Dict[PositiveInt, Dict[str, Union[int, str]]]]
    ads: Dict[str, Dict[PositiveInt, Dict[str, Union[int, str]]]]
