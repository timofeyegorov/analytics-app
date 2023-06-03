from urllib.parse import urlparse
from flask import Flask
from redis import StrictRedis

from app.core.application import Application


class RedisApp(Application, StrictRedis):
    def __init__(self, flask_app: Flask, *args, **kwargs):
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
