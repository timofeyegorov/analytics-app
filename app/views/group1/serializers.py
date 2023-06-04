from typing import Optional
from pydantic import ConstrainedDate

from app.forms.serializers import BaseSerializer


class SegmentsSerializer(BaseSerializer):
    date_start: Optional[ConstrainedDate]
    date_end: Optional[ConstrainedDate]


class TurnoverSerializer(BaseSerializer):
    date_request_start: Optional[ConstrainedDate]
    date_request_end: Optional[ConstrainedDate]
    date_payment_start: Optional[ConstrainedDate]
    date_payment_end: Optional[ConstrainedDate]


class ClustersSerializer(BaseSerializer):
    date_start: Optional[ConstrainedDate]
    date_end: Optional[ConstrainedDate]


class LandingsSerializer(BaseSerializer):
    date_start: Optional[ConstrainedDate]
    date_end: Optional[ConstrainedDate]


class ChannelsSerializer(BaseSerializer):
    date_from: Optional[ConstrainedDate]
    date_to: Optional[ConstrainedDate]
    account: Optional[str]


class TrafficSourcesSerializer(BaseSerializer):
    date_start: Optional[ConstrainedDate]
    date_end: Optional[ConstrainedDate]


class SegmentsStatsSerializer(BaseSerializer):
    date_start: Optional[ConstrainedDate]
    date_end: Optional[ConstrainedDate]


class LeadsTAStatsSerializer(BaseSerializer):
    date_start: Optional[ConstrainedDate]
    date_end: Optional[ConstrainedDate]
