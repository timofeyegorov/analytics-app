from enum import Enum
from pydantic import BaseModel, PositiveInt, EmailStr, NonNegativeInt


class AccountAccessRoleTitleEnum(Enum):
    admin = "Главный администратор"
    manager = "Администратор"
    reports = "Наблюдатель"


class AccountAccessRoleEnum(Enum):
    admin = "admin"
    manager = "manager"
    reports = "reports"

    @property
    def title(self) -> str:
        return AccountAccessRoleTitleEnum[self.name].value


class AccountStatusTitleEnum(Enum):
    _1 = "Активен"
    _0 = "Неактивен"


class AccountStatusEnum(Enum):
    _1 = 1
    _0 = 0

    @property
    def title(self) -> str:
        return AccountStatusTitleEnum[self.name].value


class AccountTypeTitleEnum(Enum):
    general = "Обычный"
    agency = "Агентский"


class AccountTypeEnum(Enum):
    general = "general"
    agency = "agency"

    @property
    def title(self) -> str:
        return AccountTypeTitleEnum[self.name].value


class CampaignTypeTitleEnum(Enum):
    normal = "Обычная кампания, в которой можно создавать любые объявления, кроме описанных в следующих пунктах"
    vk_apps_managed = "Кампания, в которой можно рекламировать только администрируемые Вами приложения и у которой есть отдельный бюджет"
    mobile_apps = "Кампания, в которой можно рекламировать только мобильные приложения"
    promoted_posts = (
        "Кампания, в которой можно рекламировать только записи в сообществе"
    )
    adaptive_ads = (
        "Кампания, в которой можно рекламировать только объявления адаптивного формата"
    )


class CampaignTypeEnum(Enum):
    normal = "normal"
    vk_apps_managed = "vk_apps_managed"
    mobile_apps = "mobile_apps"
    promoted_posts = "promoted_posts"
    adaptive_ads = "adaptive_ads"

    @property
    def title(self) -> str:
        return CampaignTypeTitleEnum[self.name].value


class CampaignStatusTitleEnum(Enum):
    _0 = "кампания остановлена"
    _1 = "кампания запущена"
    _2 = "кампания удалена"


class CampaignStatusEnum(Enum):
    _0 = 0
    _1 = 1
    _2 = 2

    @property
    def title(self) -> str:
        return CampaignStatusTitleEnum[self.name].value


class AccountData(BaseModel):
    access_role: AccountAccessRoleEnum
    account_id: PositiveInt
    account_status: AccountStatusEnum
    account_type: AccountTypeEnum
    account_name: str
    can_view_budget: bool
    ad_network_allowed_potentially: bool


class ClientData(BaseModel):
    account_id: PositiveInt
    id: PositiveInt
    name: EmailStr
    day_limit: NonNegativeInt
    all_limit: NonNegativeInt


class CampaignData(BaseModel):
    account_id: PositiveInt
    client_id: PositiveInt
    id: PositiveInt
    type: CampaignTypeEnum
    name: str
    status: CampaignStatusEnum
    day_limit: NonNegativeInt
    all_limit: NonNegativeInt
    start_time: NonNegativeInt
    stop_time: NonNegativeInt
