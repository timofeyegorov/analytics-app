import pickle
import pandas

from pathlib import Path

from config import RESULTS_FOLDER


class PickleLoader:
    def _load(self, pkl_path: Path):
        with open(pkl_path, "rb") as pkl_path_ref:
            data = pickle.load(pkl_path_ref)
            data.created_at = pandas.to_datetime(data.created_at).dt.normalize()
            return data

    @property
    def pickle_files_path(self) -> Path:
        return Path(RESULTS_FOLDER)

    @property
    def leads(self) -> pandas.DataFrame:
        return self._load(self.pickle_files_path / "leads.pkl")
