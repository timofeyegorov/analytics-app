import io
import os
import re
import hmac

import pandas
import pydantic
import requests

from time import time
from s3fs import S3FileSystem, S3File
from typing import List, Optional
from dotenv import load_dotenv
from hashlib import sha1

from .data import EnvS3, PathInto, Auth, PathInfoTypeEnum
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
        load_dotenv('.env.s3')
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

    def _get_link_key(self, path: str) -> str:
        return sha1(f"{self.env.password}{path}".encode("utf-8")).hexdigest()

    def _get_sig(self, link_key: str, expires: int, path: str) -> str:
        return hmac.new(
            link_key.encode("utf-8"), f"GET\n{expires}\n{path}".encode("utf-8"), sha1
        ).hexdigest()

    @property
    def env(self) -> EnvS3:
        return self._env

    @property
    def auth(self) -> Optional[Auth]:
        response = requests.get(
            "https://auth.selcdn.ru/",
            headers={
                "X-Auth-User": self.env.username,
                "X-Auth-Key": self.env.password,
            },
        )
        if response.status_code == 204:
            self._auth = Auth(
                token=response.headers.get("X-Auth-Token"),
                expire=response.headers.get("X-Expire-Auth-Token"),
                url=response.headers.get("X-Storage-Url"),
            )
        return self._auth

    @property
    def s3fs(self) -> S3FileSystem:
        return self._s3fs

    def ls(self, destination: str = None) -> List[PathInto]:
        """
        Метод для получения списка объектов в папке хранилища.

        :param destination: Путь до папки хранилища.
        :return: Список из объектов PathInfo.
        """
        destination = self._resolve_path(destination)
        return [PathInto(**item) for item in self.s3fs.ls(destination, detail=True)]

    def put(self, target: str, destination: str = None):
        """
        Метод для рекурсивной загрузки папки/файла в хранилище.

        :param target: Путь до папки/файла, который нужно загрузить в хранилище.
        :param destination: Путь до папки хранилища, в который нужно загрузить исходную папку/файл. Если параметр не указан, то исходный объект будет загружен в корень хранилища.
        """
        destination = self._resolve_path(destination)
        self.s3fs.put(target, destination, recursive=True)

    def rm(self, destination: str = None):
        """
        Метод для рекурсивного удаления папки/файла из хранилища.

        :param destination: Путь до папки/файла хранилища, который нужно удалить.
        """
        destination = self._resolve_path(destination)
        try:
            self.s3fs.rm(destination, recursive=True)
        except FileNotFoundError:
            pass

    def file(self, path: str) -> io.BytesIO:
        """
        Метод для чтения файла в оперативную память непосредственно с хранилища.

        :param path: Путь до файла хранилища, который нужно загрузить.
        :return: io.BytesIO
        """
        path = self._resolve_path(path)
        file = S3File(self.s3fs, path)
        return io.BytesIO(file.read())

    def link(self, path: str) -> str:
        """
        Метод для получения публичной ссылки файл хранилища. Время жизни ссылки определяется переменной окружения S3_LINK_TTL.

        :param path: Путь до файла хранилища, для которого нужно создать ссылку.
        :return: Публичная ссылка для доступа к файлу.
        """
        auth = self.auth
        path = self._resolve_path(path)
        link_key = self._get_link_key(f"/{path}")
        expires = int(time()) + self.env.link_ttl
        sig = self._get_sig(link_key, expires, f"/{path}")
        requests.post(
            f"{auth.url}{self.env.bucket}",
            headers={
                "X-Auth-Token": auth.token,
                "X-Container-Meta-Temp-URL-Key": link_key,
            },
        )
        return f"{auth.url}{path}?temp_url_sig={sig}&temp_url_expires={expires}"

    def read_text(self, path_to_txt: str) -> str:
        """
        Метод чтения текстового файла.

        :param path_to_txt: Путь до текстового файла, для последующего чтения.
        :return: строка
        """
        path_to_txt = self._resolve_path(path_to_txt)
        try:
            return self.s3fs.read_text(path=path_to_txt)
        except FileNotFoundError as err:
            print(err)

    def walk(self, path: str) -> List[str]:
        """
        Метод рекурсивного получение имен файлов в указанном каталоге

        :param path: путь до каталога.
        :return: список имен файлов с указанием относительного пути
        """
        path = self._resolve_path(path)
        return [os.path.join(path, file).
                replace('\\', '/').
                replace('//', '/').
                replace(f'{self.env.bucket}/', '')
                for path, _, file_list in self.s3fs.walk(path=path)
                for file in file_list]

    def get_paths_2_level(self) -> List[str]:
        """
        Генератор пути второго уровня всех пользователей,
        в которых располагаются json файлы
        """
        for folder in self.ls():
            if folder.type == PathInfoTypeEnum.directory and folder.name != 'temp':
                for sub_f in self.ls(folder.name):
                    if sub_f.type == PathInfoTypeEnum.directory:
                        yield sub_f.name

    def open(self, path: str):
        """
        Метод возвращает объект файлового типа из файловой системы
        Результирующий экземпляр должен правильно функционировать в контексте <with>

        :param path: путь до файла.
        :return: список имен файлов с указанием относительного пути
        """
        path = self._resolve_path(path)
        return self.s3fs.open(path)

    def mv(self, path1: str, path2: str, recursive=True):
        """
        Метод перемещения файла(ов) из одной директории в другую
        """
        path1 = self._resolve_path(path1)
        path2 = self._resolve_path(path2)
        return self.s3fs.mv(path1, path2, recursive)
