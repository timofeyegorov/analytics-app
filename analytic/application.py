from flask import Flask
from typing import Any


class FlaskApplication(Flask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config.from_object("config")

    def ext(self, name: str) -> Any:
        return self.extensions.get(name)
