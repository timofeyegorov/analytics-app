from enum import Enum
from typing import Tuple, List


class UTMEnum(Enum):
    utm_source = "UTM source"
    utm_medium = "UTM medium"
    utm_campaign = "UTM campaign"
    utm_term = "UTM term"
    utm_content = "UTM content"

    @classmethod
    def choices(cls) -> List[Tuple[str, str]]:
        return list(map(lambda item: (item.name, item.value), cls))
