import pandas
import pickle

from pathlib import Path
from typing import List, Dict, Any
from transliterate import slugify

from app.dags.vk.data import (
    AccountData,
    ClientData,
    CampaignData,
    TargetGroupData,
    AdData,
    AdLayoutData,
    DemographicData,
    StatisticData,
)


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

    def adsgettargetgroups(self, file, target_groups: List[Dict[str, Any]]):
        data = list(
            map(lambda target_group: TargetGroupData(**target_group), target_groups)
        )
        pickle.dump(data, file)

    def adsgetads(self, file, ads: List[Dict[str, Any]]):
        data = list(map(lambda ad: AdData(**ad), ads))
        pickle.dump(data, file)

    def adsgetadslayout(self, file, ads_layout: List[Dict[str, Any]]):
        data = list(map(lambda ad_layout: AdLayoutData(**ad_layout), ads_layout))
        pickle.dump(data, file)

    def adsgetdemographics(self, file, demographics: List[Dict[str, Any]]):
        data = list(
            map(lambda demographic: DemographicData(**demographic), demographics)
        )
        pickle.dump(data, file)

    def adsgetstatistics(self, file, statistics: List[Dict[str, Any]]):
        data = list(map(lambda statistic: StatisticData(**statistic), statistics))
        pickle.dump(data, file)

    def collectstatisticsdataframe(self, file, statistics: List[Dict[str, Any]]):
        data = statistics
        pickle.dump(data, file)


# data = pandas.DataFrame(list(map(lambda item: item.dict(), data)))
# data["spent"] = data["spent"].fillna(0).astype(float)
# data["impressions"] = data["impressions"].fillna(0).astype(int)
# data["clicks"] = data["clicks"].fillna(0).astype(int)
# data["reach"] = data["reach"].fillna(0).astype(int)
# data["uniq_views_count"] = data["uniq_views_count"].fillna(0).astype(int)
# data["link_external_clicks"] = (
#     data["link_external_clicks"].fillna(0).astype(int)
# )
# data["ctr"] = data["ctr"].fillna(0).astype(float)
# data["effective_cost_per_click"] = (
#     data["effective_cost_per_click"].fillna(0).astype(float)
# )
# data["effective_cost_per_mille"] = (
#     data["effective_cost_per_mille"].fillna(0).astype(float)
# )
# data["effective_cpf"] = data["effective_cpf"].fillna(0).astype(float)
# data["effective_cost_per_message"] = (
#     data["effective_cost_per_message"].fillna(0).astype(float)
# )
# data["message_sends"] = data["message_sends"].fillna(0).astype(int)
# data["video_plays_unique_started"] = (
#     data["video_plays_unique_started"].fillna(0).astype(int)
# )
# data["video_plays_unique_3_seconds"] = (
#     data["video_plays_unique_3_seconds"].fillna(0).astype(int)
# )
# data["video_plays_unique_25_percents"] = (
#     data["video_plays_unique_25_percents"].fillna(0).astype(int)
# )
# data["video_plays_unique_50_percents"] = (
#     data["video_plays_unique_50_percents"].fillna(0).astype(int)
# )
# data["video_plays_unique_75_percents"] = (
#     data["video_plays_unique_75_percents"].fillna(0).astype(int)
# )
# data["video_plays_unique_100_percents"] = (
#     data["video_plays_unique_100_percents"].fillna(0).astype(int)
# )
