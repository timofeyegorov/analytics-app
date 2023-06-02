from flask import Flask
from redis import StrictRedis


class RedisApplication(StrictRedis):
    def __init__(self, app: Flask, *args, **kwargs):
        kwargs.update(
            {
                "charset": "utf-8",
                **app.config.get("REDIS"),
            }
        )
        super().__init__(*args, **kwargs)

        app.jinja_env.globals["redis"] = self

        app.extensions.update({"redis": self})
