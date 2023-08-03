from enum import Enum
from pydantic import BaseModel, HttpUrl, NonNegativeInt


class PathInfoTypeEnum(Enum):
    file: str = "file"
    directory: str = "directory"


class EnvS3(BaseModel):
    endpoint: HttpUrl
    bucket: str
    account: int
    username: str
    password: str
    link_ttl: NonNegativeInt


class PathInto(BaseModel):
    name: str
    type: PathInfoTypeEnum
    size: NonNegativeInt

    def __init__(self, *args, **kwargs):
        name = kwargs.get("name", "")
        names = name.split("/")
        names.pop(0)
        kwargs.update({"name": "/".join(names)})
        super().__init__(*args, **kwargs)


class Auth(BaseModel):
    token: str
    expire: int
    url: HttpUrl
