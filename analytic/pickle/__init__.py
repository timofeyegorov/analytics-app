import pickle

from typing import Any
from pathlib import Path

from flask import Flask


class PickleApplication:
    app: Flask

    def __init__(self, app: Flask, *args, **kwargs):
        self.app = app
        app.extensions.update({"pickle": self})

    def get_path(self, name: str) -> Path:
        return Path(self.app.config.get("RESULTS_FOLDER"), f"{name}.pkl")

    def load(self, name: str) -> Any:
        with open(self.get_path(name), "rb") as file_ref:
            data = pickle.load(file_ref)
        return data
