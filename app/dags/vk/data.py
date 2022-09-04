from enum import Enum
from typing import Optional
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


class CampaignCostTypeTitleEnum(Enum):
    _0 = "Оплата за переходы"
    _1 = "Оплата за показы (включая цель «Максимум показов»)"
    _3 = "Оптимизированная оплата за показы"


class CampaignCostTypeEnum(Enum):
    _0 = 0
    _1 = 1
    _3 = 3

    @property
    def title(self) -> str:
        return CampaignCostTypeTitleEnum[self.name].value


class CampaignAdFormatTitleEnum(Enum):
    _1 = "Изображение и текст"
    _2 = "Большое изображение"
    _4 = "Продвижение сообществ или приложений, квадратное изображение"
    _8 = "Специальный формат сообществ"
    _9 = "Запись в сообществе"
    _11 = "Адаптивный формат"


class CampaignAdFormatEnum(Enum):
    _1 = 1
    _2 = 2
    _4 = 4
    _8 = 8
    _9 = 9
    _11 = 11

    @property
    def title(self) -> str:
        return CampaignAdFormatTitleEnum[self.name].value


class AdFormatTitleEnum(Enum):
    _1 = "Изображение и текст"
    _2 = "Большое изображение"
    _3 = "Эксклюзивный формат"
    _4 = "Продвижение сообществ или приложений, квадратное изображение"
    _5 = "Приложение в новостной ленте(устаревший)"
    _6 = "Мобильное приложение"
    _9 = "Запись в сообществе"
    _11 = "Адаптивный формат"
    _12 = "Истории"


class AdFormatEnum(Enum):
    _1 = 1
    _2 = 2
    _3 = 3
    _4 = 4
    _5 = 5
    _6 = 6
    _9 = 9
    _11 = 11
    _12 = 12

    @property
    def title(self) -> str:
        return AdFormatTitleEnum[self.name].value


class AdCostTypeTitleEnum(Enum):
    _0 = "Оплата за переходы"
    _1 = "Оплата за показы"
    _3 = "Оптимизированная оплата за показы"


class AdCostTypeEnum(Enum):
    _0 = 0
    _1 = 1
    _2 = 2

    @property
    def title(self) -> str:
        return AdCostTypeTitleEnum[self.name].value


class AdGoalTypeTitleEnum(Enum):
    _1 = "Показы"
    _2 = "Переходы"
    _3 = "Отправка заявок"
    _5 = "Вступления в сообщество"
    _6 = "Добавление в корзину"
    _7 = "Добавление в список желаний"
    _8 = "Уточнение сведений"
    _9 = "Начало оформления заказа"
    _10 = "Добавление платёжной информации"
    _11 = "Покупка"
    _12 = "Контакт"
    _13 = "Получение потенциального клиента"
    _14 = "Запись на приём"
    _15 = "Регистрация"
    _16 = "Подача заявки"
    _17 = "Использование пробной версии"
    _18 = "Оформление подписки"
    _19 = "Посещение страницы"
    _20 = "Просмотр контента"
    _21 = "Использование поиска"
    _22 = "Поиск местонахождения"
    _23 = "Пожертвование средств"
    _24 = "Конверсия"


class AdGoalTypeEnum(Enum):
    _1 = 1
    _2 = 2
    _3 = 3
    _5 = 5
    _6 = 6
    _7 = 7
    _8 = 8
    _9 = 9
    _10 = 10
    _11 = 11
    _12 = 12
    _13 = 13
    _14 = 14
    _15 = 15
    _16 = 16
    _17 = 17
    _18 = 18
    _19 = 19
    _20 = 20
    _21 = 21
    _22 = 22
    _23 = 23
    _24 = 24

    @property
    def title(self) -> str:
        return AdGoalTypeTitleEnum[self.name].value


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
    client_id: Optional[PositiveInt]
    id: PositiveInt
    type: CampaignTypeEnum
    name: str
    status: CampaignStatusEnum
    day_limit: NonNegativeInt
    all_limit: NonNegativeInt
    start_time: NonNegativeInt
    stop_time: NonNegativeInt


class AdData(BaseModel):
    account_id: PositiveInt
    client_id: Optional[PositiveInt]
    id: PositiveInt
    campaign_id: PositiveInt
    ad_format: AdFormatEnum
    cost_type: AdCostTypeEnum
    cpc: PositiveInt
    cpm: PositiveInt
    ocpm: PositiveInt
    goal_type: AdGoalTypeEnum
