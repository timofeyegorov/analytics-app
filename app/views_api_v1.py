import pytz
import datetime

from typing import Dict, Any
from pathlib import Path
from flask.views import MethodView, ResponseReturnValue
from flask.wrappers import Response

from app.analytics.pickle_load import PickleLoader

from config import DATA_FOLDER


WEEK_FOLDER = Path(DATA_FOLDER) / "week"


class APIView(MethodView):
    data: Dict[str, Any] = {}

    def render(self):
        return Response(self.data, content_type="application/json")

    def get(self, *args, **kwargs):
        return self.render()


# class TestView(APIView):
#     def get(self, *args, **kwargs):
#         tz = pytz.timezone("Europe/Moscow")
#         data = PickleLoader().roistat_analytics
#         data["date"] = data["date"].apply(lambda item: item.date())
#         date = datetime.datetime.now(tz=tz).date() - datetime.timedelta(days=1)
#         data = data[data["date"] >= date]
#         data.rename(
#             columns={
#                 **dict(
#                     map(
#                         lambda item: (f"marker_level_{item}_title", f"level{item}"),
#                         range(8),
#                     )
#                 ),
#                 "visitsCost": "expenses",
#             },
#             inplace=True,
#         )
#
#         self.data = data[
#             [
#                 "date",
#                 "expenses",
#                 "level1",
#                 "level2",
#                 "level3",
#                 "level4",
#                 "level5",
#                 "level6",
#                 "level7",
#             ]
#         ].to_json(orient="records", date_format="iso", double_precision=0)
#
#         return super().get(*args, **kwargs)
