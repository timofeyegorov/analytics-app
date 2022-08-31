import pickle

from pathlib import Path
from typing import List, Dict, Any
from transliterate import slugify

from .data import AccountData


class VKWriter:
    path: Path

    def __init__(self, path: Path):
        self.path = path

    def __call__(self, method: str, data: Any):
        with open(self.path / f"{method}.pkl", "wb") as file_ref:
            getattr(self, slugify(method, "ru"))(file_ref, data)

    def adsgetaccounts(self, file, accounts: List[Dict[str, Any]]):
        data = list(map(lambda account: AccountData(**account), accounts))
        pickle.dump(data, file)
