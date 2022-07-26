import pickle

from pandas import DataFrame
from pathlib import Path

from config import RESULTS_FOLDER


class PickleLoader:
    def _load(self, pkl_path: Path):
        with open(pkl_path, "rb") as pkl_path_ref:
            return pickle.load(pkl_path_ref)

    @property
    def pickle_files_path(self) -> Path:
        return Path(RESULTS_FOLDER)

    @property
    def leads(self) -> DataFrame:
        return self._load(self.pickle_files_path / "leads.pkl")
