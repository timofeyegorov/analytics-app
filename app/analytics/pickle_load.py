import pickle
import pandas

from pathlib import Path

from config import RESULTS_FOLDER


class PickleLoader:
    def __call__(self, name: str, *args, **kwargs) -> pandas.DataFrame:
        return self.__getattribute__(name)

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
    def leads_np(self) -> pandas.DataFrame:
        data = self._load(self.pickle_files_path / "leads_np.pkl")
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

    @property
    def statistics(self) -> pandas.DataFrame:
        data = self._load(self.pickle_files_path / "statistics.pkl")
        return data

    @property
    def roistat_leads(self) -> pandas.DataFrame:
        data = self._load(self.pickle_files_path / "roistat_leads.pkl")
        return data

    # Новое. Расходы для funnel_channel
    @property
    def roistat_expenses(self) -> pandas.DataFrame:
        data = self._load(self.pickle_files_path / "roistat_expenses.pkl")
        return data

    @property
    def roistat_db(self) -> pandas.DataFrame:
        data = self._load(self.pickle_files_path / "roistat_db.pkl")
        return data

    @property
    def roistat_levels(self) -> pandas.DataFrame:
        data = self._load(self.pickle_files_path / "roistat_levels.pkl")
        return data

    @property
    def roistat_packages(self) -> pandas.DataFrame:
        data = self._load(self.pickle_files_path / "roistat_packages.pkl")
        return data

    @property
    def intensives_registration(self) -> pandas.DataFrame:
        data = self._load(self.pickle_files_path / "intensives_registration.pkl")
        return data

    @property
    def intensives_preorder(self) -> pandas.DataFrame:
        data = self._load(self.pickle_files_path / "intensives_preorder.pkl")
        return data
