import json
import os
import pickle
from tempfile import TemporaryDirectory
from typing import List, Tuple

import pytz
from datetime import datetime
import pandas as pd
import numpy as np

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
    managers_zooms_path: Path = Path(DATA_FOLDER) / "week" / "managers_zooms.pkl"

    @staticmethod
    def check_date_format(date_string, date_format):
        try:
            datetime.strptime(date_string, date_format)
            return True
        except ValueError:
            return False

    def load_dataframe(self, path: Path) -> pd.DataFrame:
        with open(path, "rb") as file_ref:
            dataframe: pd.DataFrame = pickle.load(file_ref)
        return dataframe

    def get_managers_zooms(self, user) -> pd.Series:
        managers_zooms = self.load_dataframe(self.managers_zooms_path)
        managers_zooms['datetime'] = pd.to_datetime(managers_zooms.date) + pd.to_timedelta(
            managers_zooms.time.astype(str))
        managers_zooms = managers_zooms[(managers_zooms.manager == user)].loc[:, 'datetime']
        return managers_zooms

    @staticmethod
    def get_zoom_timeframes(managers_zooms: pd.Series):
        zoom_timeframes: List[Tuple] = []
        zoom_time = managers_zooms.to_numpy()
        for i in range(len(zoom_time)):  # np.datetime64 2023-07-28T10:00:00.000000000
            cur_zt = zoom_time[i] - np.timedelta64(5, 'm')
            next_zt = zoom_time[i + 1] if i < len(zoom_time) - 1 else zoom_time[i]
            next_zt = next_zt - np.timedelta64(5, 'm')

            # если между текущим временем и следующем меньше дня
            time_duration = np.timedelta64(23, 'h') + np.timedelta64(59, 'm') + np.timedelta64(59, 's')
            if (
                    np.datetime64(next_zt, 'D') - np.datetime64(cur_zt, 'D')
            ) / np.timedelta64(1, 'D') == 0:
                if cur_zt != next_zt:
                    zoom_timeframes.append(
                        (zoom_time[i], cur_zt, next_zt)
                    )
                else:
                    next_zt = np.datetime64(cur_zt, 'D') + time_duration
                    zoom_timeframes.append(
                        (zoom_time[i], cur_zt, next_zt)
                    )
            else:
                # делаю след время до 23:59:59 и добавляю в zoom_timeframes
                next_zt = np.datetime64(cur_zt, 'D') + time_duration
                zoom_timeframes.append(
                    (zoom_time[i], cur_zt, next_zt)
                )
        return zoom_timeframes

    @staticmethod
    def check_date_include(zt: np.datetime64,
                           zt_start: np.datetime64,
                           zt_end: np.datetime64):
        return zt_start < zt < zt_end

    def post(self):
        date_format = "%Y-%m-%d %H.%M.%S"
        save_date_format = "%Y%m%d-%H%M"
        self.data = json.dumps({'status_upload': 'failed'})

        manager = request.values.to_dict().pop("manager")
        s3_files = request.values.to_dict().pop("s3_files").split(',')
        managers_zooms = self.get_managers_zooms(user=manager)

        print(f'***** User <{manager}> uploading next files: *****')
        # for dir, file_list in request.files.lists():
        # for file in file_list:
        #     print(f'{manager}/{dir}/{file.filename}')

        # если имеется информация по zoom по данному пользователю
        # формирую временные отрезки для последующего поиска нужной zoom конференции
        if managers_zooms.count().sum() == 0:
            self.data = json.dumps({'status_upload': 'failed', 'message': 'ManagerNotFoundError'})
            return super().post()
        else:
            zoom_timeframes = self.get_zoom_timeframes(managers_zooms)

        with TemporaryDirectory() as tmpdir:
            s3_client = Client()
            for path, list_files in request.files.lists():
                data_in_path = path[:19]
                if self.check_date_format(data_in_path, date_format):
                    datetime64_obj = np.datetime64(data_in_path.replace('.', ':'))
                    for zt_base, zt_start, zt_end in zoom_timeframes:
                        if self.check_date_include(datetime64_obj, zt_start, zt_end):
                            for file in list_files:
                                zt_base = np.datetime_as_string(np.datetime64(zt_base), unit='s')
                                zt_base = datetime.strptime(zt_base, "%Y-%m-%dT%H:%M:%S")

                                file_path = os.path.join(
                                    manager,
                                    zt_base.strftime(save_date_format),
                                    path,
                                    file.filename).replace('\\', '/'
                                                           )

                                if file_path not in s3_files:
                                    tmp_file_dir = os.path.join(
                                        tmpdir,
                                        manager,
                                        zt_base.strftime(save_date_format),
                                        path
                                    )

                                    os.makedirs(tmp_file_dir, exist_ok=True)
                                    file.save(os.path.join(tmp_file_dir, file.filename))
                                    print(file_path)
            try:
                s3_client.put(os.path.join(tmpdir, manager))
                self.data = json.dumps({'status_upload': 'ok'})
            except FileNotFoundError as err:
                self.data = json.dumps({'status_upload': 'failed', 'message': 'FileNotFoundError'})

        print(f'User: <{manager}>: {self.data}')
        return super().post()


class ApiZoomS3GetUserFilesView(APIView):
    def post(self):
        manager = request.values.to_dict().pop("manager")
        s3_client = Client()
        user_files = s3_client.walk(manager)
        self.data = json.dumps({manager: user_files}, indent=4)
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
