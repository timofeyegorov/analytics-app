from .clients.vk import VKClient
from .clients.yandex import YandexClient
from .clients.roistat import RoistatClient


__all__ = ["vk", "yandex", "roistat"]


vk = VKClient()
yandex = YandexClient()
roistat = RoistatClient()
