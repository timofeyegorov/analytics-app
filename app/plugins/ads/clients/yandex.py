import requests

from typing import Union
from requests.exceptions import JSONDecodeError

from config import config


cfg = config.get("yandex", {}).get("api", {})


class YandexAPI:
    @property
    def host(self) -> str:
        return cfg.get("url", "")

    def get_url(self, path: str) -> str:
        return f"{self.host}{path}"

    def request(self, service: str, **kwargs) -> Union[dict, str]:
        headers = {
            "Authorization": f'Bearer {cfg.get("token", "")}',
            "Accept-Language": "ru",
        }
        response = requests.post(self.get_url(service), json=kwargs, headers=headers)
        try:
            return response.json()
        except JSONDecodeError:
            return {"result": response.content.decode("utf8")}


class YandexClient:
    api: YandexAPI

    def __init__(self):
        self.api = YandexAPI()

    def __call__(self, service: str, **kwargs) -> dict:
        response = self.api.request(service, **kwargs)
        error = response.get("error")
        if error:
            raise Exception(
                f'[{error.get("error_code") or "Unknown"}] {error.get("error_string") or "Unknown"}: {error.get("error_detail") or "Unknown"}'
            )
        return response.get("result", {})
