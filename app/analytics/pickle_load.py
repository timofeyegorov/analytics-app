import pickle
import pandas

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
    def leads(self) -> pandas.DataFrame:
        data = self._load(self.pickle_files_path / "leads.pkl")
        return data

    @property
    def additional_leads(self) -> pandas.DataFrame:
        data = self._load(self.pickle_files_path / "additional_leads.pkl")
        data.created_at = pandas.to_datetime(data.created_at).dt.normalize()
        return data

    @property
    def crops(self) -> pandas.DataFrame:
        data = self._load(self.pickle_files_path / "crops.pkl")
        return data

    @property
    def crops_list(self) -> pandas.DataFrame:
        data = self._load(self.pickle_files_path / "crops_list.pkl")
        return data

    @property
    def trafficologists(self) -> pandas.DataFrame:
        data = self._load(self.pickle_files_path / "trafficologists.pkl")
        return data

    @property
    def ca_payment_analytic(self) -> pandas.DataFrame:
        data = self._load(self.pickle_files_path / "ca_payment_analytic.pkl")
        return data

    @property
    def target_audience(self) -> pandas.DataFrame:
        data = self._load(self.pickle_files_path / "target_audience.pkl")
        return data
