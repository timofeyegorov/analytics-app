import os
import re
import pydantic

from s3fs import S3FileSystem
from typing import List
from dotenv import load_dotenv

from .data import EnvS3, PathInto
from .exceptions import S3EnvironmentException


class Client:
    _env: EnvS3
    _s3fs: S3FileSystem

    def __init__(self):
        self._read_env()
        self._s3fs = S3FileSystem(
            endpoint_url=str(self.env.endpoint),
            username=self.env.username,
            password=self.env.password,
        )

    def _read_env(self):
        load_dotenv()
        env = dict(
            filter(lambda item: str(item[0]).startswith("S3_"), os.environ.items())
        )
        try:
            self._env = EnvS3(
                **dict([(item[0][3:].lower(), item[1]) for item in env.items()])
            )
        except pydantic.ValidationError as error:
            raise S3EnvironmentException(
                dict([(item.get("loc")[0], item.get("msg")) for item in error.errors()])
            )

    def _resolve_path(self, path: str = None) -> str:
        if path is None:
            path = ""
        path = re.sub(r"^[\.\/]+", "", path)
        path = f"/{self.env.bucket}/{path}"
        return "/".join(list(filter(None, path.split("/"))))

    def _get_path_info(self) -> PathInto:
        return PathInto()

    @property
    def env(self) -> EnvS3:
        return self._env

    @property
    def s3fs(self) -> S3FileSystem:
        return self._s3fs

    def ls(self, destination: str = None) -> List[PathInto]:
        destination = self._resolve_path(destination)
        return [PathInto(**item) for item in self.s3fs.ls(destination, detail=True)]

    def put(self, target: str, destination: str = None):
        destination = self._resolve_path(destination)
        self.s3fs.put(target, destination, recursive=True)

    def rm(self, destination: str = None):
        destination = self._resolve_path(destination)
        try:
            self.s3fs.rm(destination, recursive=True)
        except FileNotFoundError:
            pass
