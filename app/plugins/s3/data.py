from enum import Enum
from pydantic import BaseModel, HttpUrl, conint, NonNegativeInt


class PathInfoTypeEnum(Enum):
    file = "file"
    directory = "directory"


class EnvS3(BaseModel):
    endpoint: HttpUrl
    bucket: str
    username: str
    password: str


class PathInto(BaseModel):
    name: str
    type: PathInfoTypeEnum
    size: NonNegativeInt
