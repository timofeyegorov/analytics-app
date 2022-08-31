from .clients.vk import VKClient
from .clients.yandex import YandexClient


__all__ = ["vk", "yandex"]


vk = VKClient()
yandex = YandexClient()
