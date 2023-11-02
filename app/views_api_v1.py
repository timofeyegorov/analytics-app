import os
import hmac
import json
import pickle
import pandas
import base64
import hashlib
import tempfile
import datetime
import pandas as pd
import numpy as np

from typing import List, Tuple
from transliterate import slugify

from flask import session
from werkzeug.datastructures import FileStorage

from dotenv import load_dotenv
from pathlib import Path

from flask import request
from flask.views import MethodView
from flask.wrappers import Response

from .database.auth import (
    get_user_by_id,
    update_last_update_zoom,
    get_last_update_zoom,
)
from app import db
from app.plugins.s3 import Client
from app.database import models

from config import DATA_FOLDER

WEEK_FOLDER = Path(DATA_FOLDER) / "week"


class APIView(MethodView):
    data: bytes = json.dumps({})

    def render(self):
        return Response(self.data, content_type="application/json")

    def get(self, *args, **kwargs):
        return self.render()

    def post(self, *args, **kwargs):
        return self.render()


class ApiZoomS3UploadView(APIView):
    @staticmethod
    def sign(key, msg):
        return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

    def get_signature_key(self, key, date_stamp, region_name, service_name):
        k_date = self.sign(("AWS4" + key).encode("utf-8"), date_stamp)
        k_region = self.sign(k_date, region_name)
        k_service = self.sign(k_region, service_name)
        k_signing = self.sign(k_service, "aws4_request")
        return k_signing

    @staticmethod
    def get_forms3(
        xAmzCredential: str,
        xAmzAlgorithm: str,
        xAmzDate: str,
        policy: str,
        XAmzSignature: str,
    ) -> dict:
        return {
            "xAmzCredential": xAmzCredential,
            "xAmzAlgorithm": xAmzAlgorithm,
            "xAmzDate": xAmzDate,
            "policy": policy,
            "XAmzSignature": XAmzSignature,
        }

    def post(self):
        manager_id = session.get("uid")
        if not manager_id:
            self.data = json.dumps({"status": "failed"}).encode("utf-8")
            return super().post()
        else:
            load_dotenv(".env.s3")
            date_now = datetime.datetime.now().date()
            date_stamp = date_now.strftime("%Y%m%d")
            xAmzDate = date_now.strftime("%Y%m%dT000000Z")
            region_name = "ru-1"
            service_name = "s3"
            expiration = (
                datetime.datetime.now() + datetime.timedelta(hours=12)
            ).isoformat()[:-6] + "000Z"
            xAmzCredential = f"184282_anu_zoom_01/{date_stamp}/{region_name}/{service_name}/aws4_request"
            xAmzAlgorithm = "AWS4-HMAC-SHA256"
            secret_key = os.environ.get("S3_PASSWORD")

            policy_dict = {
                "expiration": expiration,
                "conditions": [
                    {"bucket": "anu-zoom-01"},
                    ["starts-with", "$key", ""],
                    {"x-amz-credential": xAmzCredential},
                    {"x-amz-algorithm": xAmzAlgorithm},
                    {"x-amz-date": xAmzDate},
                ],
            }
            policy = base64.b64encode(
                json.dumps(policy_dict).encode("utf-8")
            ).decode("utf-8")

            signing_key = self.get_signature_key(
                secret_key, date_stamp, region_name, service_name
            )
            signature = self.sign(signing_key, policy).hex()

            self.data = json.dumps(
                {
                    "forms3": self.get_forms3(
                        xAmzCredential=xAmzCredential,
                        xAmzAlgorithm=xAmzAlgorithm,
                        xAmzDate=xAmzDate,
                        policy=policy,
                        XAmzSignature=signature,
                    )
                }
            ).encode("utf-8")

            return super().post()


class ApiZoomS3GetUserFilesView(APIView):
    def post(self):
        manager_id = session.get("uid")
        manager_name = get_user_by_id(manager_id).username
        s3_client = Client()
        user_files = s3_client.walk(manager_name)
        self.data = json.dumps(
            {"cloudfiles": user_files}, indent=2, ensure_ascii=False
        ).encode("utf-8")
        return super().post()


class ApiUpdateZoomUploadDate(APIView):
    def post(self):
        manager_id = session.get("uid")
        update_last_update_zoom(manager_id)
        return super().post()


class ApiGetZoomLink(APIView):
    def post(self):
        manager_id = session.get("uid")
        link = None
        if manager_id:
            dataLink = request.json["dataLink"].replace("_", "/")
            s3 = Client()
            cloudfiles = s3.walk(dataLink)
            for file in cloudfiles:
                if "mp4" in file:
                    link = s3.link(file)
                    break
            self.data = json.dumps({"data-link": link}).encode("utf-8")
        else:
            self.data = json.dumps({"status": "failed"}).encode("utf-8")
        return super().post()


class ApiGetLastUpdateZoom(APIView):
    managers_zooms: Path = WEEK_FOLDER / "managers_zooms.pkl"

    @staticmethod
    def load_dataframe(path: Path) -> pandas.DataFrame:
        with open(path, "rb") as file_ref:
            dataframe: pandas.DataFrame = pickle.load(file_ref)
        return dataframe

    def get(self, username):
        if session.get("uid"):
            df = self.load_dataframe(self.managers_zooms)
            username = df[df.manager_id == username].manager.unique()[0]
            datetime_zoom = get_last_update_zoom(username)
            if datetime_zoom:
                last_uploaded_zoom = datetime_zoom["last_update_zoom"]
                if datetime_zoom["last_update_zoom"]:
                    self.data = json.dumps(
                        {
                            "datetime_zoom": last_uploaded_zoom.strftime(
                                "%Y.%m.%d %H:%M"
                            )
                        }
                    ).encode("utf-8")
            else:
                self.data = json.dumps({"datetime_zoom": ""}).encode("utf-8")
            return super().post()


class ApiUserZoomTimeframes(APIView):
    managers_zooms_path: Path = (
        Path(DATA_FOLDER) / "week" / "managers_zooms.pkl"
    )

    @staticmethod
    def load_dataframe(path: Path) -> pd.DataFrame:
        with open(path, "rb") as file_ref:
            dataframe: pd.DataFrame = pickle.load(file_ref)
        return dataframe

    def get_managers_zooms(self, user) -> pd.Series:
        managers_zooms = self.load_dataframe(self.managers_zooms_path)
        managers_zooms["datetime"] = pd.to_datetime(
            managers_zooms.date
        ) + pd.to_timedelta(managers_zooms.time.astype(str))
        managers_zooms = managers_zooms[managers_zooms.manager == user].loc[
            :, "datetime"
        ]
        return managers_zooms

    @staticmethod
    def get_zoom_timeframes(managers_zooms: pd.Series):
        zoom_timeframes: List[Tuple] = []
        zoom_time = managers_zooms.to_numpy()
        for i in range(
            len(zoom_time)
        ):  # np.datetime64 2023-07-28T10:00:00.000000000
            cur_zt = zoom_time[i] - np.timedelta64(5, "m")
            next_zt = (
                zoom_time[i + 1] if i < len(zoom_time) - 1 else zoom_time[i]
            )
            next_zt = next_zt - np.timedelta64(5, "m")

            # если между текущим временем и следующем меньше дня
            time_duration = (
                np.timedelta64(23, "h")
                + np.timedelta64(59, "m")
                + np.timedelta64(59, "s")
            )
            if (
                np.datetime64(next_zt, "D") - np.datetime64(cur_zt, "D")
            ) / np.timedelta64(1, "D") == 0:
                if cur_zt != next_zt:
                    zoom_timeframes.append(
                        (
                            np.datetime_as_string(zoom_time[i]),
                            np.datetime_as_string(cur_zt),
                            np.datetime_as_string(next_zt),
                        )
                    )
                else:
                    next_zt = np.datetime64(cur_zt, "D") + time_duration
                    zoom_timeframes.append(
                        (
                            np.datetime_as_string(zoom_time[i]),
                            np.datetime_as_string(cur_zt),
                            np.datetime_as_string(next_zt),
                        )
                    )
            else:
                # делаю след время до 23:59:59 и добавляю в zoom_timeframes
                next_zt = np.datetime64(cur_zt, "D") + time_duration
                zoom_timeframes.append(
                    (
                        np.datetime_as_string(zoom_time[i]),
                        np.datetime_as_string(cur_zt),
                        np.datetime_as_string(next_zt),
                    )
                )
        return zoom_timeframes

    def post(self, *args, **kwargs):
        manager_id = session.get("uid")
        manager = get_user_by_id(manager_id).username.strip()
        managers_zooms = self.get_managers_zooms(user=manager)

        # если имеется информация по zoom по данному пользователю
        # формирую временные отрезки для последующего поиска нужной zoom конференции
        if managers_zooms.count().sum() == 0:
            self.data = json.dumps(
                {"status_upload": "failed", "message": "ManagerNotFoundError"},
                indent=2,
                ensure_ascii=False,
            ).encode("utf-8")
            return super().post()
        else:
            zoom_timeframes = self.get_zoom_timeframes(managers_zooms)
            self.data = json.dumps(
                {"zoom_timeframes": zoom_timeframes},
                ensure_ascii=False,
                indent=2,
            ).encode("utf-8")
        return super().post()


class ApiUserName(APIView):
    def post(self, *args, **kwargs):
        manager_id = session.get("uid")
        manager = get_user_by_id(manager_id).username
        self.data = json.dumps({"username": manager}).encode("utf-8")
        return super(ApiUserName, self).post()


class ApiCopyTempZoomFiles(APIView):
    def post(self):
        manager_id = session.get("uid")
        if manager_id:
            err_list = []
            s3 = Client()
            temp_files = json.loads(request.data.decode("utf-8"))
            for item in temp_files["tempfiles"]:
                try:
                    s3.mv(f"temp/{item}", item)
                except FileNotFoundError:
                    err_list.append(item)
                    continue

            self.data = json.dumps(
                {
                    "copy status": "ok" if len(err_list) == 0 else "failed",
                    "errors": err_list,
                }
            ).encode("utf-8")
        else:
            self.data = json.dumps({"status": "copy tempfiles failed"}).encode(
                "utf-8"
            )
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


class ApiUploadLeads(APIView):
    def process_file(self, file: FileStorage):
        tmp = tempfile.NamedTemporaryFile()
        file.save(tmp.name)
        dataframe = pandas.read_csv(tmp.name)
        dataframe.rename(
            columns=dict(
                (item, slugify(str(item), "ru").replace("-", "_"))
                for item in dataframe.columns
            ),
            inplace=True,
        )

        dataframe.rename(
            columns={
                "traffic_channel": "roistat_url",
                "country": "quiz_answers1",
                "skolko_vam_let": "quiz_answers2",
                "v_kakoj_sfere_sejchas_rabotaete": "quiz_answers3",
                "vash_srednij_dohod_v_mesjats": "quiz_answers4",
                "rassmatrivaete_li_v_perspektive_platnoe_obuchenie_professii_ra"
                "zrabotchik_iskusstvennogo_intellekta": "quiz_answers5",
                "skolko_vremeni_gotovy_vydelit_na_obuchenie_v_nedelju": "quiz_a"
                "nswers6",
                "iz_kakoj_vy_strany": "quiz_answers1_dop",
                "email": "email",
                "phone": "phone",
            },
            inplace=True,
        )
        dataframe.fillna("", inplace=True)
        if (
            "quiz_answers1" in dataframe.columns
            and "quiz_answers1_dop" in dataframe.columns
        ):
            dataframe["quiz_answers1"] = dataframe.apply(
                lambda item: item["quiz_answers1"] or item["quiz_answers1_dop"],
                axis=1,
            )
        dataframe["identifier"] = dataframe.apply(
            lambda item: f'{item["created"]}_{item["phone"]}_{item["email"]}',
            axis=1,
        )
        db_rows = []
        if len(dataframe):
            for row in models.TildaLead.query.filter(
                models.TildaLead.identifier_exp.in_(
                    dataframe["identifier"].tolist()
                )
            ).all():
                db_rows.append(row.identifier)

        dataframe = dataframe[~dataframe["identifier"].isin(db_rows)]
        if "roistat_fields_roistat" not in dataframe.columns:
            dataframe["roistat_fields_roistat"] = ""
        dataframe = dataframe[
            [
                "created",
                "quiz_answers1",
                "quiz_answers2",
                "quiz_answers3",
                "quiz_answers4",
                "quiz_answers5",
                "quiz_answers6",
                "sp_book_id",
                "roistat_fields_roistat",
                "name",
                "phone",
                "email",
                "roistat_url",
                "formid",
                "formname",
                "referer",
                "checkbox",
            ]
        ]

        instances = [
            models.TildaLead(**item) for item in dataframe.to_dict("records")
        ]
        db.session.bulk_save_objects(instances)
        db.session.commit()

        return {"source": len(dataframe), "added": len(instances)}

    def post(self, *args, **kwargs):
        file = request.files.get("file")
        if file is not None:
            self.data = json.dumps(self.process_file(file))
        return super().post(*args, **kwargs)
