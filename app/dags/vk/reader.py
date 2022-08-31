import pickle

from pathlib import Path
from typing import Any


class VKReader:
    path: Path

    def __init__(self, path: Path):
        self.path = path

    def __call__(self, method: str) -> Any:
        try:
            with open(self.path / f"{method}.pkl", "rb") as file_ref:
                return pickle.load(file_ref)
        except FileNotFoundError:
            return
