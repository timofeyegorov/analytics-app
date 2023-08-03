import os
from tempfile import TemporaryDirectory

import pytz
import datetime

from typing import Dict, Any
from pathlib import Path

from flask import request
from flask.views import MethodView, ResponseReturnValue
from flask.wrappers import Response

from app.analytics.pickle_load import PickleLoader
from app.plugins.s3 import Client

from config import DATA_FOLDER


WEEK_FOLDER = Path(DATA_FOLDER) / "week"


class APIView(MethodView):
    data: Dict[str, Any] = {}

    def render(self):
        return Response(self.data, content_type="application/json")

    def get(self, *args, **kwargs):
        return self.render()

    def post(self, *args, **kwargs):
        return self.render()


class ApiZoomS3UploadView(APIView):
    def post(self):
        manager = request.values.to_dict().pop("manager")
        # for dir, file_list in request.files.lists():
        #     for file in file_list:
        #         print(f'{manager}/{dir}/{file.filename}')

        with TemporaryDirectory() as tmpdir:
            s3_client = Client()
            for path, list_files in request.files.lists():
                for file in list_files:
                    file_path = os.path.join(manager, path, file.filename).replace('\\', '/')
                    if file_path not in s3_client.walk(manager):
                        tmp_file_dir = os.path.join(tmpdir, manager, path)
                        os.makedirs(tmp_file_dir, exist_ok=True)
                        file.save(os.path.join(tmp_file_dir, file.filename))
                        print(file_path)
            try:
                s3_client.put(os.path.join(tmpdir, manager))
            except FileNotFoundError as err:
                self.data = {'status_upload': 'failed'}
                print(f'{manager}: {self.data} | {err}')

        self.data = {'status_upload': 'ok'}
        return super().post()


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
