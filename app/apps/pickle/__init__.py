import pandas

from typing import Any
from pathlib import Path

from flask import Flask


class PickleApp:
    flask_app: Flask

    def __init__(self, flask_app: Flask):
        self.flask_app = flask_app
        flask_app.extensions.update({"pickle": self})

    def get_path(self, name: str) -> Path:
        return (
            self.flask_app.root_path
            / self.flask_app.config.get("DATA_FOLDER")
            / f"{name}.pkl"
        )

    def load(self, name: str) -> Any:
        return pandas.read_pickle(self.get_path(name))
