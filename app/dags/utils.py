import re
import pandas

from enum import Enum
from typing import List, Dict, Optional
from urllib.parse import urlparse, parse_qsl, ParseResult


class RoistatExceptionEnum(Enum):
    several_accounts_for_one_campaign = (
        "Found several accounts %s for one campaign '%s'"
    )


class RoistatDetectLevels:
    _lead: pandas.Series = None
    _stats: pandas.DataFrame = None
    _url: ParseResult = None
    _qs: Dict[str, str] = None
    _rs: List[str] = None
    _content: Dict[str, str] = None
    _content_available: List[str] = None

    _account: str = ""
    _campaign: str = ""
    _group: str = ""
    _ad: str = ""

    def __init__(self, lead: pandas.Series, stats: pandas.DataFrame):
        self._lead = lead
        self._stats = stats
        self._url = urlparse(self._lead.traffic_channel)
        self._qs = dict(parse_qsl(self._url.query.replace(r"&amp;", "&")))
        rs = self._qs.get("rs") or self._qs.get("roistat")
        self._rs = rs.split("_") if rs else []
        self._content = self._parse_utm_content(self._qs.get("utm_content", ""))
        self._content_available = self._get_content_available()

        self._detect_account()
        self._detect_campaign()
        self._detect_group()
        self._detect_ad()

        self._correct_ad()
        if self._ad is not None:
            self._correct_account()
            self._correct_campaign()
            self._correct_group()
        if self._group is not None:
            self._correct_account()
            self._correct_campaign()
        if self._campaign is not None:
            self._correct_account()

    def __str__(self) -> str:
        return f"""{self.__class__.__name__}:
       a: {self.account}
       c: {self.campaign}
       g: {self.group}
       a: {self.ad}"""

    @property
    def account(self) -> Optional[str]:
        return self._account

    @property
    def campaign(self) -> Optional[str]:
        return self._campaign

    @property
    def group(self) -> Optional[str]:
        return self._group

    @property
    def ad(self) -> Optional[str]:
        return self._ad

    @property
    def stats(self) -> pandas.DataFrame:
        return self._stats

    def _correct_account(self):
        accounts = self._stats.account.unique()
        if self._account == "" and len(accounts) == 1:
            self._account = accounts[0]

    def _correct_campaign(self):
        campaigns = self._stats.campaign.unique()
        if self._campaign == "" and len(campaigns) == 1:
            self._campaign = campaigns[0]

    def _correct_group(self):
        groups = self._stats.group.unique()
        if self._group == "" and len(groups) == 1:
            self._group = groups[0]

    def _correct_ad(self):
        ads = self._stats.ad.unique()
        if self._ad == "" and len(ads) == 1:
            self._ad = ads[0]

    def _parse_utm_content(self, value: str) -> Dict[str, str]:
        params = value.split("|")
        if re.findall(r":", params[0]):
            return dict(map(lambda item: tuple(item.split(":")), params))
        else:
            keys = params[0::2]
            values = params[1::2]
            values = values + [""] * (len(keys) - len(values))
            return dict(zip(keys, values))

    def _get_content_available(self) -> List[str]:
        return list(
            filter(
                None,
                [
                    self._content.get("cid"),
                    self._content.get("gid"),
                    self._content.get("aid"),
                    self._content.get("pid"),
                    self._content.get("did"),
                ],
            )
        )

    def _exception(self, exception: RoistatExceptionEnum, *args):
        message = exception.value
        if args:
            message = message % args
        raise Exception(message)

    def _detect_account(self):
        account = ""
        accounts = list(set(filter(None, self._stats.account.unique())))

        if len(self._rs) > 0:
            account = self._rs[0].lower().strip()
            if account not in accounts:
                account = ""

        if not account:
            utm_source = self._qs.get("utm_source")
            if utm_source:
                account = f":utm:{utm_source}".lower().strip()
                if account not in accounts:
                    account = ""

        self._account = account
        if self._account:
            self._stats = self._stats[self._stats.account == self._account]

    def _detect_campaign(self):
        campaign = ""
        campaigns = list(set(filter(None, self._stats.campaign.unique())))

        if len(self._rs) > 1:
            campaigns_detect = list(set(self._rs[1:]) & set(campaigns))
            if campaigns_detect:
                campaign = campaigns_detect[0]

        if not campaign:
            utm_campaign = self._qs.get("utm_campaign", "")
            if utm_campaign:
                campaign = utm_campaign.strip()
                if campaign not in campaigns:
                    campaign = ""

        if not campaign:
            if self._content_available:
                campaigns_detect = list(set(self._content_available) & set(campaigns))
                if campaigns_detect:
                    campaign = campaigns_detect[0]

        self._campaign = campaign
        if self._campaign:
            self._stats = self._stats[self._stats.campaign == self._campaign]

    def _detect_group(self):
        group = ""
        groups = list(set(filter(None, self._stats.group.unique())))

        if len(self._rs) > 1:
            groups_detect = list(set(self._rs[1:]) & set(groups))
            if groups_detect:
                group = groups_detect[0]

        if not group:
            if self._content_available:
                groups_detect = list(set(self._content_available) & set(groups))
                if groups_detect:
                    group = groups_detect[0]

        self._group = group
        if self._group:
            self._stats = self._stats[self._stats.group == self._group]

    def _detect_ad(self):
        ad = ""
        ads = list(set(filter(None, self._stats.ad.unique())))

        if len(self._rs) > 1:
            ads_detect = list(set(self._rs[1:]) & set(ads))
            if ads_detect:
                ad = ads_detect[0]

        if not ad:
            if self._content_available:
                ads_detect = list(set(self._content_available) & set(ads))
                if ads_detect:
                    ad = ads_detect[0]

        self._ad = ad
        if self._ad:
            self._stats = self._stats[self._stats.ad == self._ad]
