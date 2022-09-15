import json
import pandas
import requests
import tempfile

from enum import Enum
from pathlib import Path
from typing import Tuple, List, Dict, Any
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qsl

from xlsxwriter import Workbook

from flask import send_file
from flask import request, render_template
from flask.views import MethodView

from app.plugins.ads import vk
from app.analytics.pickle_load import PickleLoader
from app.dags.vk import reader as vk_reader, data as vk_data


pickle_loader = PickleLoader()


class ContextTemplate:
    data: Dict[str, Any] = {}

    def __call__(self, name: str, value: Any):
        self.data.update({name: value})


class TemplateView(MethodView):
    context: ContextTemplate = ContextTemplate()
    template_name: str
    title: str = ""

    def get_template_name(self) -> str:
        return self.template_name

    def render(self):
        self.context("title", self.title)
        return render_template(self.get_template_name(), **self.context.data)

    def get(self):
        return self.render()


class APIView(MethodView):
    data: Dict[str, Any] = {}

    def render(self):
        return self.data

    def get(self):
        return self.render()


class VKStatisticsView(TemplateView):
    template_name = "vk/statistics.html"
    title = "Статистика объявлений в ВК"

    @property
    def posts(self) -> Dict[int, vk_data.WallPostData]:
        return dict(map(lambda item: (item.ad_id, item), vk_reader("wall.get")))

    @property
    def accounts(self) -> Dict[int, vk_data.AccountData]:
        return dict(
            map(lambda item: (item.account_id, item), vk_reader("ads.getAccounts"))
        )

    @property
    def clients(self) -> Dict[int, vk_data.ClientData]:
        return dict(map(lambda item: (item.id, item), vk_reader("ads.getClients")))

    @property
    def campaigns(self) -> Dict[int, vk_data.CampaignData]:
        return dict(map(lambda item: (item.id, item), vk_reader("ads.getCampaigns")))

    @property
    def ads(self) -> Dict[int, vk_data.AdData]:
        return dict(map(lambda item: (item.id, item), vk_reader("ads.getAds")))

    @property
    def ads_layout(self) -> Dict[int, vk_data.AdLayoutData]:
        return dict(map(lambda item: (item.id, item), vk_reader("ads.getAdsLayout")))

    def set_context(self, data: Dict[str, Any]):
        for name, value in data.items():
            self.context(name, value)

    def get_args(self) -> Dict[str, Any]:
        return {
            "group_by": request.args.get("group_by", "ad_id"),
            "account": request.args.get("account_id") or None,
            "client": request.args.get("client_id") or None,
            "campaign": request.args.get("campaign_id") or None,
        }

    def get_stats(self) -> pandas.DataFrame:
        # TODO: убрать ограничение на 100 записей
        stats = vk_reader("collectStatisticsDataFrame")[:100]
        stats["spent"] = stats["spent"].apply(lambda value: "%.2f" % value)
        stats["ctr"] = stats["ctr"].apply(lambda value: "%.3f" % value)
        stats["effective_cost_per_click"] = stats["effective_cost_per_click"].apply(
            lambda value: "%.3f" % value
        )
        stats["effective_cost_per_mille"] = stats["effective_cost_per_mille"].apply(
            lambda value: "%.3f" % value
        )
        stats["effective_cpf"] = stats["effective_cpf"].apply(
            lambda value: "%.3f" % value
        )
        stats["effective_cost_per_message"] = stats["effective_cost_per_message"].apply(
            lambda value: "%.2f" % value
        )
        stats["sex__m__impressions_rate"] = stats["sex__m__impressions_rate"].apply(
            lambda value: "%.3f" % value
        )
        stats["sex__f__impressions_rate"] = stats["sex__f__impressions_rate"].apply(
            lambda value: "%.3f" % value
        )
        stats["sex__m__clicks_rate"] = stats["sex__m__clicks_rate"].apply(
            lambda value: "%.3f" % value
        )
        stats["sex__f__clicks_rate"] = stats["sex__f__clicks_rate"].apply(
            lambda value: "%.3f" % value
        )
        stats["age__12_18__impressions_rate"] = stats[
            "age__12_18__impressions_rate"
        ].apply(lambda value: "%.3f" % value)
        stats["age__18_21__impressions_rate"] = stats[
            "age__18_21__impressions_rate"
        ].apply(lambda value: "%.3f" % value)
        stats["age__21_24__impressions_rate"] = stats[
            "age__21_24__impressions_rate"
        ].apply(lambda value: "%.3f" % value)
        stats["age__24_27__impressions_rate"] = stats[
            "age__24_27__impressions_rate"
        ].apply(lambda value: "%.3f" % value)
        stats["age__27_30__impressions_rate"] = stats[
            "age__27_30__impressions_rate"
        ].apply(lambda value: "%.3f" % value)
        stats["age__30_35__impressions_rate"] = stats[
            "age__30_35__impressions_rate"
        ].apply(lambda value: "%.3f" % value)
        stats["age__35_45__impressions_rate"] = stats[
            "age__35_45__impressions_rate"
        ].apply(lambda value: "%.3f" % value)
        stats["age__45_100__impressions_rate"] = stats[
            "age__45_100__impressions_rate"
        ].apply(lambda value: "%.3f" % value)
        stats["age__12_18__clicks_rate"] = stats["age__12_18__clicks_rate"].apply(
            lambda value: "%.3f" % value
        )
        stats["age__18_21__clicks_rate"] = stats["age__18_21__clicks_rate"].apply(
            lambda value: "%.3f" % value
        )
        stats["age__21_24__clicks_rate"] = stats["age__21_24__clicks_rate"].apply(
            lambda value: "%.3f" % value
        )
        stats["age__24_27__clicks_rate"] = stats["age__24_27__clicks_rate"].apply(
            lambda value: "%.3f" % value
        )
        stats["age__27_30__clicks_rate"] = stats["age__27_30__clicks_rate"].apply(
            lambda value: "%.3f" % value
        )
        stats["age__30_35__clicks_rate"] = stats["age__30_35__clicks_rate"].apply(
            lambda value: "%.3f" % value
        )
        stats["age__35_45__clicks_rate"] = stats["age__35_45__clicks_rate"].apply(
            lambda value: "%.3f" % value
        )
        stats["age__45_100__clicks_rate"] = stats["age__45_100__clicks_rate"].apply(
            lambda value: "%.3f" % value
        )
        return stats

    def get(self):
        self.context("posts", self.posts)
        self.context("accounts", self.accounts)
        self.context("clients", self.clients)
        self.context("campaigns", self.campaigns)
        self.context("ads", self.ads)
        self.context("ads_layout", self.ads_layout)
        self.set_context({"fields": self.get_args()})
        self.context("stats", self.get_stats())
        return super().get()


class VKCreateAdView(TemplateView):
    template_name = "vk/create-ad.html"
    title = "Создание объявления в ВК"

    def set_context(self, data: Dict[str, Any]):
        for name, value in data.items():
            self.context(name, value)

    def form_context_add(self, **kwargs):
        self.set_context(
            {
                "fields": {
                    "account_id": kwargs.get("account_id", ""),
                    "campaign_id": kwargs.get("campaign_id", ""),
                    "cost_type": kwargs.get("cost_type", ""),
                    "ad_format": kwargs.get("ad_format", ""),
                    "link_url": kwargs.get("link_url", ""),
                    "title": kwargs.get("title", ""),
                    "description": kwargs.get("description", ""),
                    "photo": kwargs.get("photo", ""),
                    "goal_type": kwargs.get("goal_type", ""),
                }
            }
        )

    def get_photo_url(self, file, ad_format: int) -> str:
        upload_url = vk("ads.getUploadURL", ad_format=ad_format)
        target = tempfile.NamedTemporaryFile(suffix=Path(file.filename).suffix)
        target.writelines(file.stream.readlines())
        with open(target.name, "rb") as target_ref:
            response = requests.post(upload_url, files={"file": target_ref})
            output = response.json()
            if output.get("errcode"):
                raise Exception(output)
            return output.get("photo", "")

    def get(self):
        self.form_context_add()
        return super().get()

    def post(self):
        account_id = request.form.get("account_id")
        campaign_id = request.form.get("campaign_id")
        cost_type = request.form.get("cost_type")
        ad_format = request.form.get("ad_format")
        link_url = request.form.get("link_url", "")
        title = request.form.get("title", "")
        description = request.form.get("description", "")
        goal_type = request.form.get("goal_type", "")
        form_context = {
            "account_id": account_id,
            "campaign_id": campaign_id,
            "cost_type": cost_type,
            "ad_format": ad_format,
            "link_url": link_url,
            "title": title,
            "description": description,
            "goal_type": goal_type,
        }
        try:
            photo = request.form.get("photo") or self.get_photo_url(
                request.files.get("photo_file"), ad_format
            )
            form_context.update({"photo": photo})
        except Exception as error:
            self.form_context_add(**form_context)
            self.context("error", f"Основное изображение: {error}")
            return self.render()
        self.form_context_add(**form_context)
        params = {
            "account_id": int(account_id) if account_id else "",
            "data": json.dumps(
                [
                    {
                        "campaign_id": int(campaign_id) if campaign_id else "",
                        "cost_type": int(cost_type) if cost_type else "",
                        "ad_format": int(ad_format) if ad_format else "",
                        "link_url": link_url,
                        "title": title,
                        "description": description,
                        "photo": photo,
                        "goal_type": int(goal_type) if goal_type else "",
                    }
                ]
            ),
        }
        print(params)
        try:
            response = vk("ads.createAds", **params)
            print(response)
        except Exception as error:
            self.context("error", error)
        return self.render()


class VKXlsxAdsView(MethodView):
    def create_ads_worksheet(self, workbook: Workbook):
        worksheet = workbook.add_worksheet("Объявления")
        columns = [
            "id",
            "account_id",
            "client_id",
            "campaign_id",
            "title",
            "text",
            "image",
            "target_url",
            "ad_format",
            "cost_type",
            "cpc",
            "cpm",
            "ocpm",
            "goal_type",
            "impressions_limit",
            "impressions_limited",
            "ad_platform",
            "ad_platform_no_wall",
            "ad_platform_no_ad_network",
            "publisher_platforms",
            "all_limit",
            "day_limit",
            "autobidding",
            "autobidding_max_cost",
            "category1_id",
            "category2_id",
            "status",
            "name",
            "approved",
            "targeting__sex",
            "targeting__age_from",
            "targeting__age_to",
            "targeting__birthday",
            "targeting__country",
            "targeting__cities",
            "targeting__cities_not",
            "targeting__statuses",
            "targeting__groups",
            "targeting__groups_not",
            "targeting__apps",
            "targeting__apps_not",
            "targeting__districts",
            "targeting__stations",
            "targeting__streets",
            "targeting__schools",
            "targeting__positions",
            "targeting__religions",
            "targeting__interest_categories",
            "targeting__interests",
            "targeting__user_devices",
            "targeting__user_os",
            "targeting__user_browsers",
            "targeting__retargeting_groups",
            "targeting__retargeting_groups_not",
            "targeting__count",
        ]
        worksheet.write_row(0, 0, columns)
        posts = dict(map(lambda post: (post.ad_id, post.dict()), vk_reader("wall.get")))
        targeting = dict(
            map(lambda item: (item.id, item), vk_reader("ads.getAdsTargeting"))
        )
        for row, ad in enumerate(vk_reader("ads.getAds")):
            target = targeting.get(ad.id)
            worksheet.write_row(
                row + 1,
                0,
                [
                    ad.id,
                    ad.account_id,
                    ad.client_id,
                    ad.campaign_id,
                    posts.get(ad.id).get("title", ""),
                    posts.get(ad.id).get("text", ""),
                    posts.get(ad.id).get("image", ""),
                    posts.get(ad.id).get("target_url", ""),
                    ad.ad_format.value if ad.ad_format else "",
                    ad.cost_type.value if ad.cost_type else "",
                    ad.cpc,
                    ad.cpm,
                    ad.ocpm,
                    ad.goal_type.value if ad.goal_type else "",
                    ad.impressions_limit,
                    ad.impressions_limited,
                    ad.ad_platform.value if ad.ad_platform else "",
                    ad.ad_platform_no_wall,
                    ad.ad_platform_no_ad_network,
                    ad.publisher_platforms.value if ad.publisher_platforms else "",
                    ad.all_limit,
                    ad.day_limit,
                    ad.autobidding.value if ad.autobidding else "",
                    ad.autobidding_max_cost,
                    ad.category1_id,
                    ad.category2_id,
                    ad.status.value if ad.status else "",
                    ad.name,
                    ad.approved.value if ad.approved else "",
                    target.sex.value if target.sex else "",
                    target.age_from,
                    target.age_to,
                    target.birthday,
                    target.country,
                    ",".join(list(map(lambda item: str(item), target.cities))),
                    ",".join(list(map(lambda item: str(item), target.cities_not))),
                    ",".join(list(map(lambda item: str(item.value), target.statuses))),
                    ",".join(list(map(lambda item: str(item), target.groups))),
                    ",".join(list(map(lambda item: str(item), target.groups_not))),
                    ",".join(list(map(lambda item: str(item), target.apps))),
                    ",".join(list(map(lambda item: str(item), target.apps_not))),
                    ",".join(list(map(lambda item: str(item), target.districts))),
                    ",".join(list(map(lambda item: str(item), target.stations))),
                    ",".join(list(map(lambda item: str(item), target.streets))),
                    ",".join(list(map(lambda item: str(item), target.schools))),
                    ",".join(list(map(lambda item: str(item), target.positions))),
                    ",".join(list(map(lambda item: str(item), target.religions))),
                    ",".join(
                        list(map(lambda item: str(item), target.interest_categories))
                    ),
                    ",".join(list(map(lambda item: str(item), target.interests))),
                    ",".join(list(map(lambda item: str(item), target.user_devices))),
                    ",".join(list(map(lambda item: str(item), target.user_os))),
                    ",".join(list(map(lambda item: str(item), target.user_browsers))),
                    ",".join(
                        list(map(lambda item: str(item), target.retargeting_groups))
                    ),
                    ",".join(
                        list(map(lambda item: str(item), target.retargeting_groups_not))
                    ),
                    target.count,
                ],
            )

    def create_statistics_worksheet(self, workbook: Workbook):
        worksheet = workbook.add_worksheet("Статистика")
        data = vk_reader("collectStatisticsDataFrame")
        data["date"] = data["date"].astype(str)
        worksheet.write_row(0, 0, data.columns)
        for row, stat in enumerate(data.iterrows()):
            worksheet.write_row(row + 1, 0, stat[1].values)

    def create_enum(self, workbook: Workbook, enum_data: Enum, title: str):
        worksheet = workbook.add_worksheet(title)
        worksheet.write_row(0, 0, ["id", "name"])
        for row, data in enumerate(enum_data):
            worksheet.write_row(row + 1, 0, [data.value, data.title])

    def create_enums(self, workbook: Workbook):
        items = [
            (vk_data.AdFormatEnum, "ad_format"),
            (vk_data.AdCostTypeEnum, "cost_type"),
            (vk_data.AdGoalTypeEnum, "goal_type"),
            (vk_data.AdPlatformEnum, "ad_platform"),
            (vk_data.AdPublisherPlatformsEnum, "publisher_platforms"),
            (vk_data.AdAutobiddingEnum, "autobidding"),
            (vk_data.AdStatusEnum, "status"),
            (vk_data.AdApprovedEnum, "approved"),
            (vk_data.SexEnum, "targeting__sex"),
            (vk_data.BirthdayEnum, "targeting__birthday"),
            (vk_data.FamilyStatusEnum, "targeting__statuses"),
        ]
        for item in items:
            self.create_enum(workbook, *item)

    def create_countries(self, workbook: Workbook):
        worksheet = workbook.add_worksheet("targeting__countries")
        countries = vk_reader("ads.getSuggestions.countries")
        worksheet.write_row(0, 0, ["id", "name"])
        for row, country in enumerate(countries):
            worksheet.write_row(row + 1, 0, country.dict().values())

    def get(self):
        target = tempfile.NamedTemporaryFile(suffix=".xlsx")
        workbook = Workbook(target.name)
        self.create_ads_worksheet(workbook)
        self.create_statistics_worksheet(workbook)
        self.create_enums(workbook)
        self.create_countries(workbook)
        workbook.close()
        return send_file(
            workbook.filename,
            download_name=f'vk-{datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%f")}.xlsx',
            as_attachment=True,
        )


class VKXlsxLeadsView(MethodView):
    def create_leads(self, workbook: Workbook):
        leads = pickle_loader.leads
        leads = leads.drop(
            [
                "status_amo",
                "payment_amount",
                "date_request",
                "date_payment",
                "date_status_change",
                "opener",
                "closer",
                "current_lead_amo",
                "main_lead_amo",
                "is_double",
                "is_processed",
                "amo_marker",
                "updated_at",
                "channel_expense2",
                "id",
                "email",
                "phone",
            ],
            axis=1,
        )
        leads = leads[
            (
                leads.utm_source.str.contains("vk")
                | leads.utm_source.str.contains("VK")
                | leads.utm_source.str.contains("Vk")
            )
            & ~leads.utm_source.str.contains("kladovka")
        ]
        leads["created_at"] = leads["created_at"].astype(str)
        leads["turnover_on_lead"] = leads["turnover_on_lead"].astype(float)
        leads = leads.reset_index(drop=True)
        worksheet = workbook.add_worksheet("Лиды")

        def link_column(index: int) -> str:
            return f"internal:'Описание'!A{index + 1}"

        for index, column in enumerate(leads.columns):
            worksheet.write_url(0, index, link_column(index), string=column)

        for row, lead in leads.iterrows():
            worksheet.write_row(row + 1, 0, lead.values)
        worksheet.autofilter("A1:R1")

    def create_description(self, workbook: Workbook):
        datatype_indexes = vk_data.DatatypeEnum.table_indexes()

        def link_datatype(value: vk_data.DatatypeEnum) -> Tuple[str, str]:
            return (
                f"internal:'Типы данных'!A{datatype_indexes.get(value.name)}",
                value.value,
            )

        columns = [
            [
                "traffic_channel",
                link_datatype(vk_data.DatatypeEnum.URL),
                "Целевая ссылка",
            ],
            [
                "quiz_answers1",
                link_datatype(vk_data.DatatypeEnum.String),
                "Ответ на вопрос 1 квиза",
            ],
            [
                "quiz_answers2",
                link_datatype(vk_data.DatatypeEnum.String),
                "Ответ на вопрос 2 квиза",
            ],
            [
                "quiz_answers3",
                link_datatype(vk_data.DatatypeEnum.String),
                "Ответ на вопрос 3 квиза",
            ],
            [
                "quiz_answers4",
                link_datatype(vk_data.DatatypeEnum.String),
                "Ответ на вопрос 4 квиза",
            ],
            [
                "quiz_answers5",
                link_datatype(vk_data.DatatypeEnum.String),
                "Ответ на вопрос 5 квиза",
            ],
            [
                "quiz_answers6",
                link_datatype(vk_data.DatatypeEnum.String),
                "Ответ на вопрос 6 квиза",
            ],
            [
                "turnover_on_lead",
                link_datatype(vk_data.DatatypeEnum.NonNegativeFloat),
                "Оборот за лид (в рублях)",
            ],
            [
                "trafficologist",
                link_datatype(vk_data.DatatypeEnum.String),
                "Название трафиколога",
            ],
            [
                "account",
                link_datatype(vk_data.DatatypeEnum.String),
                "Название аккаунта",
            ],
            [
                "target_class",
                link_datatype(vk_data.DatatypeEnum.PositiveInteger),
                f'Количество попаданий лидов по вопросам квиза в целевую аудиторию [{", ".join(pickle_loader.target_audience)}]',
            ],
            [
                "channel_expense",
                link_datatype(vk_data.DatatypeEnum.NonNegativeFloat),
                "Расход (в рублях)",
            ],
            [
                "created_at",
                link_datatype(vk_data.DatatypeEnum.Datetime),
                "Дата создания",
            ],
            [
                "utm_source",
                link_datatype(vk_data.DatatypeEnum.String),
                "UTM source",
            ],
            [
                "utm_medium",
                link_datatype(vk_data.DatatypeEnum.String),
                "UTM medium",
            ],
            [
                "utm_campaign",
                link_datatype(vk_data.DatatypeEnum.String),
                "UTM campaign",
            ],
            [
                "utm_content",
                link_datatype(vk_data.DatatypeEnum.String),
                "UTM content",
            ],
            [
                "utm_term",
                link_datatype(vk_data.DatatypeEnum.String),
                "UTM term",
            ],
        ]
        worksheet = workbook.add_worksheet("Описание")
        worksheet.write_row(0, 0, ["Название колонки", "Тип данных", "Описание"])
        for row, item in enumerate(columns):
            worksheet.write_row(
                row + 1,
                0,
                list(
                    map(
                        lambda cell: str(cell) if isinstance(cell, tuple) else cell,
                        item,
                    )
                ),
            )
            worksheet.write_url(row + 1, 1, item[1][0], string=item[1][1])
        worksheet.autofilter("A1:C1")

    def create_datatype(self, workbook: Workbook):
        worksheet = workbook.add_worksheet("Типы данных")
        for row, item in enumerate(vk_data.DatatypeEnum.table_data()):
            worksheet.write_row(row, 0, item)
        worksheet.autofilter("A1:B1")

    def get(self):
        target = tempfile.NamedTemporaryFile(suffix=".xlsx")
        workbook = Workbook(target.name)
        self.create_leads(workbook)
        self.create_description(workbook)
        self.create_datatype(workbook)
        workbook.close()
        return send_file(
            workbook.filename,
            download_name=f'vk-{datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%f")}.xlsx',
            as_attachment=True,
        )


class VKXlsxView(TemplateView):
    template_name = "vk/xlsx.html"
    title = "Скачать таблицу XLSX"

    @property
    def ads(self) -> pandas.DataFrame:
        posts = dict(map(lambda post: (post.ad_id, post.dict()), vk_reader("wall.get")))
        rows = []
        ads_columns = []
        posts_columns = []
        for item in vk_reader("ads.getAds"):
            post = posts.get(item.id, {})
            ads_columns = item.dict().keys()
            posts_columns = post.keys()
            rows.append(list(item.dict().values()) + list(post.values()))
        return pandas.DataFrame(rows, columns=(list(ads_columns) + list(posts_columns)))

    @property
    def leads(self) -> pandas.DataFrame:
        leads = pickle_loader.leads
        leads = leads.drop(
            [
                "id",
                "email",
                "phone",
                "quiz_answers1",
                "quiz_answers2",
                "quiz_answers3",
                "quiz_answers4",
                "quiz_answers5",
                "quiz_answers6",
                "status_amo",
                "payment_amount",
                "turnover_on_lead",
                "date_request",
                "date_payment",
                "date_status_change",
                "opener",
                "closer",
                "current_lead_amo",
                "main_lead_amo",
                "is_double",
                "is_processed",
                "trafficologist",
                "account",
                "target_class",
                "channel_expense",
                "amo_marker",
                "created_at",
                "updated_at",
                "channel_expense2",
            ],
            axis=1,
        )
        leads = leads[
            leads.utm_source.str.contains("vk") | leads.utm_source.str.contains("VK")
        ]
        # leads["date_request"] = leads["date_request"].astype(str)
        # leads["date_payment"] = leads["date_payment"].astype(str)
        # leads["date_status_change"] = leads["date_status_change"].astype(str)
        # leads["created_at"] = leads["created_at"].astype(str)
        # leads["updated_at"] = leads["updated_at"].astype(str)
        leads = leads.reset_index(drop=True)
        return leads

    def get(self):
        leads = self.leads
        ads = self.ads
        # print(len(list(leads["traffic_channel"].unique())))
        # print(len(list(ads["target_url"].unique())))
        print(ads[ads.target_url.isin(leads.traffic_channel.unique())])
        self.context("leads", leads)
        return super().get()


class ApiVKCreateAdDependesFieldsView(APIView):
    def get(self):
        data = dict(parse_qsl(request.query_string.decode("utf-8")))
        account_id = int(data.get("account_id", 0))
        accounts = [("", "", False)] + list(
            map(
                lambda item: (
                    item.account_id,
                    item.account_name,
                    item.account_id == account_id,
                ),
                vk_reader("ads.getAccounts"),
            )
        )
        campaigns = []
        if list(filter(lambda item: item[2], accounts)):
            campaign_id = int(data.get("campaign_id", 0))
            campaigns = [("", "", False)] + list(
                map(
                    lambda item: (
                        item.id,
                        item.name,
                        item.id == campaign_id,
                    ),
                    list(
                        filter(
                            lambda campaign: campaign.account_id == account_id,
                            vk_reader("ads.getCampaigns"),
                        )
                    ),
                )
            )
        cost_type = int(data.get("cost_type", -1))
        cost_types = [("", "", False)] + list(
            map(
                lambda item: (item.value, item.title, item.value == cost_type),
                vk_data.CampaignCostTypeEnum,
            )
        )
        ad_format = int(data.get("ad_format", -1))
        ad_formats = [("", "", False)] + list(
            map(
                lambda item: (item.value, item.title, item.value == ad_format),
                vk_data.CampaignAdFormatEnum,
            )
        )
        goal_type = int(data.get("goal_type", -1))
        goal_types = [("", "", False)] + list(
            map(
                lambda item: (item.value, item.title, item.value == goal_type),
                vk_data.AdGoalTypeEnum,
            )
        )
        self.data = {
            "accounts": accounts,
            "campaigns": campaigns,
            "cost_type": cost_types,
            "ad_format": ad_formats,
            "goal_type": goal_types,
        }
        return super().get()


class ChannelsView(TemplateView):
    template_name = "channels/index.html"
    title = "Каналы"

    def _filter_date_from(
        self, date: datetime, leads: pandas.DataFrame
    ) -> pandas.DataFrame:
        if date:
            leads = leads[leads.created_at >= datetime.strptime(date, "%Y-%m-%d")]
        return leads

    def _filter_date_to(
        self, date: datetime, leads: pandas.DataFrame
    ) -> pandas.DataFrame:
        if date:
            leads = leads[
                leads.created_at
                < (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1))
            ]
        return leads

    def get_choices(self, leads: pandas.DataFrame) -> Dict[str, List[str]]:
        date_from = request.args.get("date_from") or None
        date_to = request.args.get("date_to") or None

        leads = self._filter_date_from(date_from, leads)
        leads = self._filter_date_to(date_to, leads)

        return {
            "accounts": [""] + list(leads["account"].unique()),
        }

    def get_filters(self, choices: Dict[str, List[str]]) -> Dict[str, str]:
        date_from = request.args.get("date_from") or None
        date_to = request.args.get("date_to") or None
        account = request.args.get("account") or None

        if account not in choices.get("accounts"):
            account = None

        return {
            "date_from": date_from,
            "date_to": date_to,
            "account": account,
        }

    def get_filtered_data(
        self, leads: pandas.DataFrame, filters: Dict[str, str]
    ) -> pandas.DataFrame:
        leads = self._filter_date_from(filters.get("date_from"), leads)
        leads = self._filter_date_to(filters.get("date_to"), leads)

        if filters.get("account"):
            leads = leads[leads["account"] == filters.get("account")].groupby(
                "utm_campaign"
            )
        else:
            leads = leads.groupby("account")

        return leads

    def get(self):
        leads = pickle_loader.leads.sort_values(["created_at"])

        choices = self.get_choices(leads)
        filters = self.get_filters(choices)
        data = self.get_filtered_data(leads, filters)

        output = list(
            map(
                lambda group: {
                    "name": group[0],
                    "dates": list(
                        map(
                            lambda item: int(item / 1000000),
                            group[1]["created_at"].unique().tolist(),
                        )
                    ),
                },
                data,
            )
        )

        if filters.get("date_from"):
            date_from = int(
                datetime.strptime(filters.get("date_from"), "%Y-%m-%d")
                .replace(tzinfo=timezone.utc)
                .timestamp()
                * 1000
            )
        else:
            date_from = min(list(map(lambda item: min(item.get("dates")), output)))

        if filters.get("date_to"):
            date_to = int(
                datetime.strptime(filters.get("date_to"), "%Y-%m-%d")
                .replace(tzinfo=timezone.utc)
                .timestamp()
                * 1000
            )
        else:
            date_to = max(list(map(lambda item: max(item.get("dates")), output)))

        self.context("choices", choices)
        self.context("filters", filters)
        self.context("date_range", [date_from, date_to])
        self.context("data", output)

        return super().get()


class ApiVKLeadsView(APIView):
    data: Dict[str, Any] = {}

    @property
    def leads(self) -> pandas.DataFrame:
        leads = pickle_loader.leads
        leads = leads.drop(
            [
                "amo_marker",
                "channel_expense2",
                "closer",
                "current_lead_amo",
                "date_payment",
                "date_request",
                "date_status_change",
                "email",
                "id",
                "is_double",
                "is_processed",
                "main_lead_amo",
                "opener",
                "payment_amount",
                "phone",
                "status_amo",
                "updated_at",
            ],
            axis=1,
        )
        leads = leads[
            leads.utm_source.str.contains("vk")
            | leads.utm_source.str.contains("VK")
            | leads.utm_source.str.contains("Vk")
        ]
        leads["created_at"] = leads["created_at"].astype(str)
        leads = leads.reset_index(drop=True)
        return leads

    def get(self):
        leads = self.leads
        columns = list(leads.columns)
        leads_output = []
        for index, item in leads.iterrows():
            leads_output.append(dict(zip(columns, list(item.values))))
        self.data.update({"leads": leads_output})
        return super().get()


class ApiVKAdsView(APIView):
    data: Dict[str, Any]

    def __init__(self):
        self.data = {}
        super().__init__()

    @property
    def add_format(self) -> Dict[str, str]:
        return dict(
            map(lambda item: (str(item.value), item.title), vk_data.AdFormatEnum)
        )

    @property
    def ad_platform(self) -> Dict[str, str]:
        return dict(
            map(lambda item: (str(item.value), item.title), vk_data.AdPlatformEnum)
        )

    @property
    def approved(self) -> Dict[str, str]:
        return dict(
            map(lambda item: (str(item.value), item.title), vk_data.AdApprovedEnum)
        )

    @property
    def autobidding(self) -> Dict[str, str]:
        return dict(
            map(lambda item: (str(item.value), item.title), vk_data.AdAutobiddingEnum)
        )

    @property
    def categories(self) -> Dict[str, str]:
        return dict(
            map(
                lambda item: (str(item.id), item.name),
                vk_reader("ads.getSuggestions.interest_categories_v2"),
            )
        )

    @property
    def cost_type(self) -> Dict[str, str]:
        return dict(
            map(lambda item: (str(item.value), item.title), vk_data.AdCostTypeEnum)
        )

    @property
    def goal_type(self) -> Dict[str, str]:
        return dict(
            map(lambda item: (str(item.value), item.title), vk_data.AdGoalTypeEnum)
        )

    @property
    def publisher_platforms(self) -> Dict[str, str]:
        return dict(
            map(
                lambda item: (str(item.value), item.title),
                vk_data.AdPublisherPlatformsEnum,
            )
        )

    @property
    def status(self) -> Dict[str, str]:
        return dict(
            map(lambda item: (str(item.value), item.title), vk_data.AdStatusEnum)
        )

    @property
    def ads(self) -> pandas.DataFrame:
        posts = dict(map(lambda post: (post.ad_id, post.dict()), vk_reader("wall.get")))
        rows = []
        columns = []
        for item in vk_reader("ads.getAds"):
            post = posts.get(item.id, {})
            post.pop("ad_id", None)
            post.pop("attachments", None)
            post.pop("date", None)
            post.pop("from_id", None)
            post.pop("id", None)
            post.pop("owner_id", None)

            item = item.dict()
            item.pop("account_id", None)
            item.pop("ad_platform_no_ad_network", None)
            item.pop("ad_platform_no_wall", None)
            item.pop("campaign_id", None)
            item.pop("name", None)
            item.pop("client_id", None)
            item.pop("disclaimer_medical", None)
            item.pop("disclaimer_specialist", None)
            item.pop("disclaimer_supplements", None)
            item.pop("impressions_limit", None)
            item.pop("impressions_limited", None)
            item.pop("events_retargeting_groups", None)
            item.pop("video", None)
            item.pop("weekly_schedule_hours", None)
            item.pop("weekly_schedule_use_holidays", None)

            item.update(
                {
                    "ad_format": str(item.get("ad_format").value)
                    if item.get("ad_format")
                    else None,
                    "ad_platform": str(item.get("ad_platform").value)
                    if item.get("ad_platform")
                    else None,
                    "all_limit": int(item.get("all_limit", 0)),
                    "approved": str(item.get("approved").value)
                    if item.get("approved")
                    else None,
                    "autobidding": str(
                        (item.get("autobidding") or vk_data.AdAutobiddingEnum(0)).value
                    ),
                    "autobidding_max_cost": int(item.get("autobidding_max_cost") / 100)
                    if item.get("autobidding_max_cost")
                    else 0,
                    "category1_id": str(item.get("category1_id"))
                    if item.get("category1_id")
                    else None,
                    "category2_id": str(item.get("category2_id"))
                    if item.get("category2_id")
                    else None,
                    "cost_type": str(item.get("cost_type").value)
                    if item.get("cost_type")
                    else None,
                    "cpc": float(item.get("cpc") / 100) if item.get("cpc") else None,
                    "cpm": float(item.get("cpm") / 100) if item.get("cpm") else None,
                    "ocpm": float(item.get("ocpm") / 100) if item.get("ocpm") else None,
                    "day_limit": int(item.get("day_limit", 0)),
                    "goal_type": str(item.get("goal_type").value)
                    if item.get("goal_type")
                    else None,
                    "id": int(item.get("id")),
                    "publisher_platforms": str(item.get("publisher_platforms").value)
                    if item.get("publisher_platforms")
                    else None,
                    "status": str(item.get("status").value)
                    if item.get("status")
                    else None,
                    **post,
                }
            )
            columns = item.keys()
            rows.append(list(item.values()))
        return pandas.DataFrame(rows, columns=(list(columns)))

    def get(self):
        ads = self.ads
        columns = list(ads.columns)
        leads_output = []
        for index, item in ads.iterrows():
            leads_output.append(dict(zip(columns, list(item.values))))
        self.data.update(
            {
                "ads": leads_output,
                "ad_format": self.add_format,
                "ad_platform": self.ad_platform,
                "approved": self.approved,
                "autobidding": self.autobidding,
                "categories": self.categories,
                "cost_type": self.cost_type,
                "goal_type": self.goal_type,
                "publisher_platforms": self.publisher_platforms,
                "status": self.status,
                "titles": {
                    "ad_format": "Формат объявления",
                    "ad_platform": "Рекламные площадки, на которых будет показываться объявление",
                    "all_limit": "Общий лимит объявления (в рублях, 0 — лимит не задан)",
                    "approved": "Статус модерации объявления",
                    "autobidding": "Автоматическое управление ценой",
                    "autobidding_max_cost": "Максимальное ограничение автоматической ставки (в рублях)",
                    "category1_id": "ID тематики или подраздела тематики объявления",
                    "category2_id": "ID тематики или подраздела тематики объявления (дополнительная тематика)",
                    "cost_type": "Тип оплаты",
                    "cpc": "Цена за переход (в рублях)",
                    "cpm": "Цена за 1000 показов (в рублях)",
                    "day_limit": "Дневной лимит объявления (в рублях, 0 — лимит не задан)",
                    "goal_type": "Тип цели",
                    "id": "Идентификатор объявления",
                    "ocpm": "Цена за действие для oCPM (в рублях)",
                    "publisher_platforms": "На каких площадках показывается объявление",
                    "status": "Статус объявления",
                    "image": "Изображение объявления",
                    "target_url": "Целевая ссылка",
                    "text": "Описание объявления",
                    "title": "Заголовок объявления",
                },
            }
        )
        return super().get()
