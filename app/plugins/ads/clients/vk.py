import requests

from typing import Union, Any
from requests.exceptions import JSONDecodeError

from config import config


cfg = config.get("vk", {}).get("api", {})


class VKAPI:
    @property
    def host(self) -> str:
        return cfg.get("url", "")

    def get_url(self, path: str) -> str:
        return f'{self.host}{path}?access_token={cfg.get("token", "")}&v={cfg.get("version", "")}&lang=ru'

    def request(self, method: str, **kwargs) -> Union[dict, str]:
        response = requests.post(self.get_url(method), data=kwargs)
        try:
            output = response.json()
            error = output.get("response")
            if isinstance(error, list) and error and error[0].get("error_code"):
                raise Exception(
                    f'VKAPI [{error[0].get("error_code")}]: {error[0].get("error_desc")}'
                )
            return output
        except JSONDecodeError:
            return {"response": response.content.decode("utf8")}


class VKClient:
    api: VKAPI

    def __init__(self):
        self.api = VKAPI()

    def __call__(self, method: str, **kwargs) -> Any:
        response = self.api.request(method, **kwargs)
        error = response.get("error")
        if error:
            raise Exception(
                f'[{error.get("error_code") or "Unknown"}]: {error.get("error_msg") or "Unknown"}'
            )
        return response.get("response")
