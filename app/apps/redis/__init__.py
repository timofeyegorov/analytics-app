from redis import StrictRedis
from urllib.parse import urlparse

from flask import Flask


class RedisApp(StrictRedis):
    def __init__(self, flask_app: Flask = None, *args, **kwargs):
        url = urlparse(flask_app.config.get("REDIS_URL", ""))
        try:
            host, port = url.netloc.split(":", 2)
            kwargs.update({"port": port})
        except ValueError:
            host = url.netloc
        try:
            kwargs.update({"db": list(filter(None, url.path.split("/")))[0]})
        except IndexError:
            pass
        kwargs.update({"charset": "utf-8", "host": host})
        super().__init__(*args, **kwargs)

        flask_app.jinja_env.globals["redis"] = self

        flask_app.extensions.update({"redis": self})
