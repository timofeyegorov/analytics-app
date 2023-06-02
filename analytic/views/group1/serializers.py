from typing import Optional
from pydantic import ConstrainedDate

from analytic.forms.serializers import BaseSerializer


class SegmentsSerializer(BaseSerializer):
    date_start: Optional[ConstrainedDate]
    date_end: Optional[ConstrainedDate]


class TurnoverSerializer(BaseSerializer):
    date_request_start: Optional[ConstrainedDate]
    date_request_end: Optional[ConstrainedDate]
    date_payment_start: Optional[ConstrainedDate]
    date_payment_end: Optional[ConstrainedDate]
