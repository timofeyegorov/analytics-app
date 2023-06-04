from typing import List, Optional
from pydantic import ConstrainedDate

from app.forms.serializers import BaseSerializer


class AudienceBaseSerializer(BaseSerializer):
    date_start: Optional[ConstrainedDate]
    date_end: Optional[ConstrainedDate]
    utm: Optional[List[str]]
    utm_value: Optional[List[str]]


class AbsoluteSerializer(AudienceBaseSerializer):
    pass


class RelativeSerializer(AudienceBaseSerializer):
    pass
