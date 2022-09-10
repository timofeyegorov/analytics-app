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
    CityData,
    DemographicData,
    StatisticData,
    WallPostData,
    AdTargetingData,
    PositionsData,
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

    def adsgetadstargeting(self, file, targeting: List[Dict[str, Any]]):
        data = list(map(lambda target: AdTargetingData(**target), targeting))
        pickle.dump(data, file)

    def adsgetdemographics(self, file, demographics: List[Dict[str, Any]]):
        data = list(
            map(lambda demographic: DemographicData(**demographic), demographics)
        )
        pickle.dump(data, file)

    def adsgetstatistics(self, file, statistics: List[Dict[str, Any]]):
        data = list(map(lambda statistic: StatisticData(**statistic), statistics))
        pickle.dump(data, file)

    # def collectcities(self, file, cities: List[Dict[int, str]]):
    #     data = list(
    #         map(
    #             lambda city: CityData(**city),
    #             list(filter(lambda item: item.get("id") > 0, cities)),
    #         )
    #     )
    #     pickle.dump(data, file)

    def collectstatisticsdataframe(self, file, statistics: List[Dict[str, Any]]):
        data = pandas.DataFrame(statistics)
        # city_columns = list(
        #     filter(lambda item: item.startswith("city__"), data.columns)
        # )
        # data[city_columns] = data[city_columns].fillna(0).astype(float)
        data["spent"] = data["spent"].fillna(0).astype(float)
        data["impressions"] = data["impressions"].fillna(0).astype(int)
        data["clicks"] = data["clicks"].fillna(0).astype(int)
        data["ctr"] = data["ctr"].fillna(0).astype(float)
        data["effective_cost_per_click"] = (
            data["effective_cost_per_click"].fillna(0).astype(float)
        )
        data["effective_cost_per_mille"] = (
            data["effective_cost_per_mille"].fillna(0).astype(float)
        )
        data["effective_cpf"] = data["effective_cpf"].fillna(0).astype(float)
        data["effective_cost_per_message"] = (
            data["effective_cost_per_message"].fillna(0).astype(float)
        )
        data["message_sends"] = data["message_sends"].fillna(0).astype(int)
        data["sex__m__impressions_rate"] = (
            data["sex__m__impressions_rate"].fillna(0).astype(float)
        )
        data["sex__f__impressions_rate"] = (
            data["sex__f__impressions_rate"].fillna(0).astype(float)
        )
        data["sex__m__clicks_rate"] = (
            data["sex__m__clicks_rate"].fillna(0).astype(float)
        )
        data["sex__f__clicks_rate"] = (
            data["sex__f__clicks_rate"].fillna(0).astype(float)
        )
        data["age__12_18__impressions_rate"] = (
            data["age__12_18__impressions_rate"].fillna(0).astype(float)
        )
        data["age__18_21__impressions_rate"] = (
            data["age__18_21__impressions_rate"].fillna(0).astype(float)
        )
        data["age__21_24__impressions_rate"] = (
            data["age__21_24__impressions_rate"].fillna(0).astype(float)
        )
        data["age__24_27__impressions_rate"] = (
            data["age__24_27__impressions_rate"].fillna(0).astype(float)
        )
        data["age__27_30__impressions_rate"] = (
            data["age__27_30__impressions_rate"].fillna(0).astype(float)
        )
        data["age__30_35__impressions_rate"] = (
            data["age__30_35__impressions_rate"].fillna(0).astype(float)
        )
        data["age__35_45__impressions_rate"] = (
            data["age__35_45__impressions_rate"].fillna(0).astype(float)
        )
        data["age__45_100__impressions_rate"] = (
            data["age__45_100__impressions_rate"].fillna(0).astype(float)
        )
        data["age__12_18__clicks_rate"] = (
            data["age__12_18__clicks_rate"].fillna(0).astype(float)
        )
        data["age__18_21__clicks_rate"] = (
            data["age__18_21__clicks_rate"].fillna(0).astype(float)
        )
        data["age__21_24__clicks_rate"] = (
            data["age__21_24__clicks_rate"].fillna(0).astype(float)
        )
        data["age__24_27__clicks_rate"] = (
            data["age__24_27__clicks_rate"].fillna(0).astype(float)
        )
        data["age__27_30__clicks_rate"] = (
            data["age__27_30__clicks_rate"].fillna(0).astype(float)
        )
        data["age__30_35__clicks_rate"] = (
            data["age__30_35__clicks_rate"].fillna(0).astype(float)
        )
        data["age__35_45__clicks_rate"] = (
            data["age__35_45__clicks_rate"].fillna(0).astype(float)
        )
        data["age__45_100__clicks_rate"] = (
            data["age__45_100__clicks_rate"].fillna(0).astype(float)
        )
        pickle.dump(data, file)

    def adsgetsuggestionspositions(self, file, positions: List[Dict[str, Any]]):
        data = list(map(lambda position: PositionsData(**position), positions))
        pickle.dump(data, file)

    def wallget(self, file, posts: List[Dict[str, Any]]):
        data = list(map(lambda post: WallPostData(**post), posts))
        pickle.dump(data, file)
