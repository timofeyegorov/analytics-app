import requests

from enum import Enum
from typing import Union, Any
from requests.exceptions import JSONDecodeError

from config import config


cfg = config.get("roistat", {})


class ApiMethod(Enum):
    analytics = "/project/analytics/data"
    leads = "/project/proxy-leads"
    dimensions = "/project/analytics/dimensions"
    dimension_values = "/project/analytics/dimension-values"
    metrics = "/project/analytics/metrics-new"


class RoistatAPI:
    @property
    def host(self) -> str:
        return cfg.get("url", "")

    def get_url(self, path: str) -> str:
        return f"{self.host}{path}"

    def request(
        self, method: ApiMethod, is_get: bool = False, params: dict = None, **kwargs
    ) -> Union[dict, str]:
        headers = {"Api-key": cfg.get("api_key", "")}
        url = self.get_url(method.value)
        if params is None:
            params = {}
        params.update({"project": cfg.get("project_id", "")})
        request_method = requests.get if is_get else requests.post
        response = request_method(url, params=params, json=kwargs, headers=headers)
        try:
            output = response.json()
            status = output.get("status")
            if status == "error":
                raise Exception(
                    f'RoistatAPI [{output.get("error")}]: {output.get("description")}'
                )
            return output
        except JSONDecodeError:
            return {"response": response.content.decode("utf8")}


class RoistatClient:
    api: RoistatAPI

    def __init__(self):
        self.api = RoistatAPI()

    def __call__(self, method: str, **kwargs) -> Any:
        try:
            api_method = ApiMethod[method]
        except KeyError:
            raise Exception(f'Method "{method}" is undefined')
        is_get = kwargs.pop("is_get", False)
        params = kwargs.pop("params", None)
        return self.api.request(api_method, is_get, params, **kwargs)
