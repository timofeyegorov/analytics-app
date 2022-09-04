import pickle

from pathlib import Path
from typing import List, Dict, Any
from transliterate import slugify

from app.dags.vk.data import AccountData, ClientData, CampaignData, AdData


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

    def adsgetclients(self, file, clients: List[Dict[str, Any]]):
        data = list(map(lambda client: ClientData(**client), clients))
        pickle.dump(data, file)

    def adsgetcampaigns(self, file, campaigns: List[Dict[str, Any]]):
        data = list(map(lambda campaign: CampaignData(**campaign), campaigns))
        pickle.dump(data, file)

    def adsgetads(self, file, ads: List[Dict[str, Any]]):
        print(ads)
        data = list(map(lambda ad: AdData(**ad), ads))
        pickle.dump(data, file)
