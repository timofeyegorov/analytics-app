import json
import pytz
import requests
import datetime

from typing import Dict, Any, Optional
from flask.views import MethodView
from flask.globals import request
from flask.templating import render_template

from config import BASE_DIR


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

    def get(self, *args, **kwargs):
        return self.render()


class AmoCRMEvents:
    @classmethod
    def parse(cls, event, value: Any = None, *args, **kwargs) -> str:
        if value is None:
            return ""
        return getattr(cls, f"parse_{event[0]}")(value, *args, **kwargs)

    @classmethod
    def parse_1(cls, value: Any, *args, **kwargs) -> str:
        # Новая сделка
        print(kwargs.get("notes"))
        return (
            str(
                kwargs.get("notes")
                .get(value.get("note").get("id"))
                .get("params")
                .get("text")
            )
            if kwargs.get("notes").get(value.get("note").get("id"))
            else None
        )

    @classmethod
    def parse_14(cls, value: Any, *args, **kwargs) -> str:
        # Изменение этапа продажи
        status = value.get("lead_status")
        return (
            kwargs.get("pipelines")
            .get(status.get("pipeline_id"))
            .get("statuses")
            .get(status.get("id"))
            .get("name")
        )

    @classmethod
    def parse_24(cls, value: Any, *args, **kwargs) -> str:
        # Изменение поля c переданным ID
        return str(value.get("custom_field_value").get("text"))

    @classmethod
    def parse_25(cls, value: Any, *args, **kwargs) -> str:
        # Ответственный изменен
        return kwargs.get("users").get(value.get("responsible_user").get("id"))

    @classmethod
    def parse_27(cls, value: Any, *args, **kwargs) -> str:
        # Новое примечание
        return str(
            kwargs.get("notes")
            .get(value.get("note").get("id"))
            .get("params")
            .get("text")
        )

    @classmethod
    def parse_28(cls, value: Any, *args, **kwargs) -> Dict[str, Any]:
        # Добавлен новый файл
        info = kwargs.get("notes").get(value.get("note").get("id"))
        file = kwargs.get("files").get(info.get("params").get("file_uuid"))
        return {
            "Ссылка": file.get("_links").get("download").get("href"),
            "Дата добавления файла": str(
                datetime.datetime.fromtimestamp(
                    info.get("created_at"), tz=pytz.timezone("Europe/Moscow")
                )
            )
            if info.get("created_at")
            else None,
            "Дата изменения файла": str(
                datetime.datetime.fromtimestamp(
                    info.get("updated_at"), tz=pytz.timezone("Europe/Moscow")
                )
            )
            if info.get("updated_at")
            else None,
            "Размер файла": file.get("size"),
            "MimeType": file.get("metadata").get("mime_type"),
        }

    @classmethod
    def parse_63(cls, value: Any, *args, **kwargs) -> str:
        # Теги добавлены
        return str(value.get("tag").get("name"))

    @classmethod
    def parse_69(cls, value: Any, *args, **kwargs) -> int:
        # Изменение поля “Бюджет”
        return value.get("sale_field_value").get("sale")

    @classmethod
    def parse_70(cls, value: Any, *args, **kwargs) -> str:
        # Изменение поля "Название"
        return value.get("name_field_value").get("name")

    @classmethod
    def parse_72(cls, value: Any, *args, **kwargs) -> str:
        # Прикрепление
        entity = value.get("link").get("entity")
        tz = pytz.timezone("Europe/Moscow")
        if entity.get("type") == "contact":
            amocrm = AmoCRMAPI()
            amocrm(
                f'contacts/{entity.get("id")}',
                params={"with": "catalog_elements,leads,customers"},
            )
            return {
                "ID контакта": amocrm.response.get("id"),
                "Название контакта": amocrm.response.get("name"),
                "Имя контакта": amocrm.response.get("first_name"),
                "Фамилия контакта": amocrm.response.get("last_name"),
                "Дата создания контакта,": str(
                    datetime.datetime.fromtimestamp(
                        amocrm.response.get("created_at"), tz=tz
                    )
                )
                if amocrm.response.get("created_at")
                else None,
                "Дата изменения контакта": str(
                    datetime.datetime.fromtimestamp(
                        amocrm.response.get("updated_at"), tz=tz
                    )
                )
                if amocrm.response.get("updated_at")
                else None,
                "Дата ближайшей задачи к выполнению": str(
                    datetime.datetime.fromtimestamp(
                        amocrm.response.get("closest_task_at"), tz=tz
                    )
                )
                if amocrm.response.get("closest_task_at")
                else None,
                "Дополнительная информация": dict(
                    map(
                        lambda group: (
                            group.get("field_name"),
                            str(
                                getattr(
                                    AmoCRMAPI,
                                    f'parse_{group.get("field_type")}',
                                )(group.get("values")[0].get("value"))
                            )
                            if group.get("values")
                            else None,
                        ),
                        amocrm.response.get("custom_fields_values"),
                    )
                ),
            }
        raise ValueError(f'Undefined condition for type="{entity.get("type")}"')

    @classmethod
    def parse_89(cls, value: Any, *args, **kwargs) -> str:
        # Входящее сообщение
        return ", ".join(
            list(
                map(lambda item: f"[{item[0]}]={item[1]}", value.get("message").items())
            )
        )

    @classmethod
    def parse_90(cls, value: Any, *args, **kwargs) -> str:
        # Исходящее сообщение
        return ", ".join(
            list(
                map(lambda item: f"[{item[0]}]={item[1]}", value.get("message").items())
            )
        )


class AmoCRMAPI:
    _host: str = "neuraluniversity.amocrm.ru"
    _version: str = "v4"
    _credentials_file: str = "amocrm-credentials.json"
    _auth_file: str = "amocrm-auth.json"
    _error: Dict[str, Any] = None
    _response: Dict[str, Any] = None

    @property
    def error(self) -> Optional[Dict[str, Any]]:
        return self._error

    @property
    def response(self) -> Optional[Dict[str, Any]]:
        return self._response

    @classmethod
    def parse_textarea(cls, value) -> str:
        return str(value)

    @classmethod
    def parse_text(cls, value) -> str:
        return str(value)

    @classmethod
    def parse_multitext(cls, value) -> str:
        return str(value)

    @classmethod
    def parse_tracking_data(cls, value) -> str:
        return str(value)

    @classmethod
    def parse_select(cls, value) -> str:
        return str(value)

    @classmethod
    def parse_multiselect(cls, value) -> str:
        return str(value)

    @classmethod
    def parse_numeric(cls, value) -> str:
        return str(value)

    @classmethod
    def parse_checkbox(cls, value) -> str:
        return "Да" if value is True else "Нет"

    @classmethod
    def parse_date_time(cls, value) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(value, tz=pytz.timezone("Europe/Moscow"))

    @classmethod
    def parse_date(cls, value) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(value, tz=pytz.timezone("Europe/Moscow"))

    def __call__(
        self,
        method: str,
        params: Dict[str, Any] = None,
        is_file: bool = False,
        *args,
        **kwargs,
    ) -> Dict[str, Any]:
        if params is None:
            params = {}
        auth = self._get_auth()
        if is_file:
            url = self._get_url_drive(method)
        else:
            url = self._get_url(method)
        response = requests.get(
            url,
            params=params,
            headers={"Authorization": f'Bearer {auth.get("access_token")}'},
        )
        try:
            data = response.json()
        except requests.exceptions.JSONDecodeError:
            data = response.content.decode("utf-8")
        if not response.ok:
            if data.get("status") == 401:
                self._refresh_token(auth.get("refresh_token"))
                self._error = None
                self._response = None
                self(method, *args, **kwargs)
            else:
                self._error = data
        else:
            self._response = data

    def _get_url_prefix(self) -> str:
        return f"https://{self._host}/"

    def _get_url(self, method: str) -> str:
        if method is None:
            method = ""
        return f"{self._get_url_prefix()}api/{self._version}/{method}"

    def _get_url_drive(self, method: str) -> str:
        return f"https://drive-b.amocrm.ru/{method}"

    def _get_credentials(self) -> Dict[str, Any]:
        try:
            with open(BASE_DIR / self._credentials_file, "r") as file_ref:
                return json.load(file_ref)
        except FileNotFoundError:
            return {}

    def _request_access_token(self, args: Dict[str, str]) -> Optional[Dict[str, Any]]:
        response = requests.post(
            f"{self._get_url_prefix()}oauth2/access_token", json=args
        )
        if response.ok:
            data = response.json()
            data.update(
                {
                    "expires_in": str(
                        datetime.datetime.utcnow()
                        + datetime.timedelta(seconds=data.get("expires_in"))
                    )
                }
            )
            with open(BASE_DIR / self._auth_file, "w") as file_ref:
                json.dump(data, file_ref, indent=2, ensure_ascii=False)
            return data
        else:
            self._error = response.json()

    def _refresh_token(self, refresh_token: str) -> Optional[Dict[str, Any]]:
        return self._request_access_token(
            {
                **self._get_credentials(),
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
            }
        )

    def _authorize(self) -> Optional[Dict[str, Any]]:
        return self._request_access_token(
            {
                **self._get_credentials(),
                "grant_type": "authorization_code",
            }
        )

    def _get_auth(self) -> Optional[Dict[str, Any]]:
        try:
            with open(BASE_DIR / self._auth_file, "r") as file_ref:
                auth = json.load(file_ref)
        except FileNotFoundError:
            auth = self._authorize()
        if auth is not None:
            expires_in = datetime.datetime.strptime(
                auth.get("expires_in"), "%Y-%m-%d %H:%M:%S.%f"
            ) - datetime.timedelta(minutes=1)
            if expires_in <= datetime.datetime.utcnow():
                auth = self._refresh_token(auth.get("refresh_token"))
            return auth


class AmoCRMAPIBaseView(TemplateView):
    amocrm: AmoCRMAPI

    def __init__(self, *args, **kwargs):
        self.amocrm = AmoCRMAPI()
        super().__init__(*args, **kwargs)


class LeadView(AmoCRMAPIBaseView):
    template_name = "amocrm/index.html"
    title = "AmoCRM Lead"

    def _get_lead(self, lead: int) -> Dict[str, Any]:
        amocrm = AmoCRMAPI()
        amocrm(
            f"leads/{lead}",
            params={
                "with": "catalog_elements,is_price_modified_by_robot,loss_reason,contacts,source_id"
            },
        )
        return amocrm.response

    def _get_pipelines(self) -> Dict[str, Any]:
        amocrm = AmoCRMAPI()
        amocrm("leads/pipelines")
        pipelines = {}
        for pipeline in amocrm.response.get("_embedded").get("pipelines"):
            pipeline.pop("_links")
            embedded = pipeline.pop("_embedded")
            statuses = {}
            for status in embedded.get("statuses"):
                status.pop("_links")
                statuses.update({status.get("id"): status})
            pipeline.update({"statuses": statuses})
            pipelines.update({pipeline.get("id"): pipeline})
        return pipelines

    def _get_users(self, page: int = 1, users: Dict[str, Any] = None) -> Dict[str, Any]:
        amocrm = AmoCRMAPI()
        amocrm(
            "users",
            params={
                "page": page,
                "limit": 250,
                "with": "role,group",
            },
        )
        if users is None:
            users = {}
        for user in amocrm.response.get("_embedded").get("users"):
            user.pop("_links")
            rights = user.pop("rights")
            embedded = user.pop("_embedded")
            group_id = rights.get("group_id")
            role_id = rights.get("role_id")
            groups = dict(
                map(
                    lambda item: (item.get("id"), item.get("name")),
                    embedded.get("groups"),
                )
            )
            roles = dict(
                map(
                    lambda item: (item.get("id"), item.get("name")),
                    embedded.get("roles"),
                )
            )
            user.update(
                {
                    "group": groups.get(group_id),
                    "role": roles.get(role_id),
                }
            )
            users.update({user.get("id"): user})
        page_count = amocrm.response.get("_page_count")
        if page < page_count:
            users = self._get_users(page + 1, users)
        return users

    def _get_user(self, user: int) -> Dict[str, Any]:
        amocrm = AmoCRMAPI()
        amocrm(f"users/{user}", params={"with": "role,group,uuid,amojo_id,user_rank"})
        return amocrm.response

    def _get_pipeline(self, pipeline: int) -> Dict[str, Any]:
        amocrm = AmoCRMAPI()
        amocrm(f"leads/pipelines/{pipeline}")
        return amocrm.response

    def _get_contact(self, contact: int) -> Dict[str, Any]:
        amocrm = AmoCRMAPI()
        amocrm(
            f"contacts/{contact}", params={"with": "catalog_elements,leads,customers"}
        )
        return amocrm.response

    def _get_lead_events(self, lead: int) -> Dict[str, Any]:
        amocrm = AmoCRMAPI()
        amocrm(
            "events",
            params={
                "limit": 100,
                "filter[entity]": "lead",
                "filter[entity_id]": lead,
            },
        )
        return amocrm.response

    def _get_events_types(self) -> Dict[str, Any]:
        amocrm = AmoCRMAPI()
        amocrm(
            "events/types",
            params={
                "language_code": "ru",
            },
        )
        return amocrm.response

    def _get_lead_notes(self, lead: int) -> Dict[str, Any]:
        amocrm = AmoCRMAPI()
        amocrm(
            f"leads/{lead}/notes",
            params={
                "limit": 250,
            },
        )
        return amocrm.response

    def _get_lead_files(self, lead: int) -> Dict[str, Any]:
        amocrm = AmoCRMAPI()
        amocrm(
            f"leads/{lead}/files",
        )
        return amocrm.response

    def _get_file(self, file_uuid: str) -> Dict[str, Any]:
        amocrm = AmoCRMAPI()
        amocrm(
            f"v1.0/files/{file_uuid}",
            is_file=True,
        )
        return amocrm.response

    def _collect_data(self, lead: Dict[str, Any]) -> Dict[str, Any]:
        pipelines_dict = self._get_pipelines()
        users_dict = self._get_users()
        responsible_user = self._get_user(lead.get("responsible_user_id"))
        pipeline = self._get_pipeline(lead.get("pipeline_id"))
        events = self._get_lead_events(lead.get("id"))
        events_types = dict(
            map(
                lambda item: (item["key"], (item["type"], item["lang"])),
                self._get_events_types().get("_embedded").get("events_types"),
            )
        )
        notes = self._get_lead_notes(lead.get("id"))
        files_dict = dict(
            map(
                lambda item: (
                    item.get("file_uuid"),
                    self._get_file(item.get("file_uuid")),
                ),
                self._get_lead_files(lead.get("id")).get("_embedded").get("files"),
            )
        )

        tz = pytz.timezone("Europe/Moscow")
        responsible_user_embedded = responsible_user.get("_embedded")
        groups = dict(
            map(
                lambda item: (item.get("id"), item.get("name")),
                responsible_user_embedded.get("groups"),
            )
        )
        roles = dict(
            map(
                lambda item: (item.get("id"), item.get("name")),
                responsible_user_embedded.get("roles"),
            )
        )
        group_id = responsible_user.get("rights").get("group_id")
        role_id = responsible_user.get("rights").get("role_id")

        lead_embedded = lead.get("_embedded")
        contacts = list(
            map(
                lambda item: self._get_contact(item.get("id")),
                lead_embedded.get("contacts"),
            )
        )

        notes_embedded = notes.get("_embedded").get("notes")
        notes_dict = dict(map(lambda item: (item.get("id"), item), notes_embedded))
        events_embedded = events.get("_embedded").get("events")
        events_list = []
        for item in events_embedded:
            event_info = events_types.get(item.get("type"))
            before = item.get("value_before")
            after = item.get("value_after")
            events_list.append(
                {
                    "ID события": item.get("id"),
                    "Тип события": item.get("type"),
                    "Название типа события": event_info[1],
                    "Дата создания события": str(
                        datetime.datetime.fromtimestamp(item.get("created_at"), tz=tz)
                    )
                    if item.get("created_at")
                    else None,
                    "Значение до": AmoCRMEvents.parse(
                        event_info,
                        before[0] if before else None,
                        notes=notes_dict,
                        files=files_dict,
                        pipelines=pipelines_dict,
                        users=users_dict,
                    ),
                    "Значение после": AmoCRMEvents.parse(
                        event_info,
                        after[0] if after else None,
                        notes=notes_dict,
                        files=files_dict,
                        pipelines=pipelines_dict,
                        users=users_dict,
                    ),
                }
            )
        # print(json.dumps(notes_dict, indent=2, ensure_ascii=False))

        return {
            "ID сделки": lead.get("id"),
            "Название сделки": lead.get("name"),
            "Бюджет сделки": lead.get("price"),
            "Дата создания сделки": str(
                datetime.datetime.fromtimestamp(lead.get("created_at"), tz=tz)
            )
            if lead.get("created_at")
            else None,
            "Дата изменения сделки": str(
                datetime.datetime.fromtimestamp(lead.get("updated_at"), tz=tz)
            )
            if lead.get("updated_at")
            else None,
            "Дата закрытия сделки": str(
                datetime.datetime.fromtimestamp(lead.get("closed_at"), tz=tz)
            )
            if lead.get("closed_at")
            else None,
            "Дата ближайшей задачи к выполнению": str(
                datetime.datetime.fromtimestamp(lead.get("closest_task_at"), tz=tz)
            )
            if lead.get("closest_task_at")
            else None,
            "Ответственный за сделку": {
                "ID пользователя": responsible_user.get("id"),
                "Полное имя пользователя": responsible_user.get("name"),
                "E-mail пользователя": responsible_user.get("email"),
                "Язык пользователя": responsible_user.get("lang"),
                "ID пользователя в сервисе чатов": responsible_user.get("amojo_id"),
                "Группа": {
                    "ID группы": group_id,
                    "Название группы": groups.get(group_id),
                },
                "Роль": {
                    "ID роли": role_id,
                    "Название роли": roles.get(role_id),
                },
            },
            "Воронки сделки": {
                "ID воронки": pipeline.get("id"),
                "Название воронки": pipeline.get("name"),
            },
            "Статус сделки": {
                "ID статуса": lead.get("status_id"),
                "Название статуса": pipelines_dict.get(lead.get("pipeline_id")).get(
                    lead.get("status_id")
                ),
            },
            "Теги": list(
                map(lambda item: str(item.get("name")), lead_embedded.get("tags"))
            ),
            "Контакты": list(
                map(
                    lambda item: {
                        "ID контакта": item.get("id"),
                        "Название контакта": item.get("name"),
                        "Имя контакта": item.get("first_name"),
                        "Фамилия контакта": item.get("last_name"),
                        "Дата создания контакта,": str(
                            datetime.datetime.fromtimestamp(
                                item.get("created_at"), tz=tz
                            )
                        )
                        if item.get("created_at")
                        else None,
                        "Дата изменения контакта": str(
                            datetime.datetime.fromtimestamp(
                                item.get("updated_at"), tz=tz
                            )
                        )
                        if item.get("updated_at")
                        else None,
                        "Дата ближайшей задачи к выполнению": str(
                            datetime.datetime.fromtimestamp(
                                item.get("closest_task_at"), tz=tz
                            )
                        )
                        if item.get("closest_task_at")
                        else None,
                        "Дополнительная информация": dict(
                            map(
                                lambda group: (
                                    group.get("field_name"),
                                    str(
                                        getattr(
                                            AmoCRMAPI,
                                            f'parse_{group.get("field_type")}',
                                        )(group.get("values")[0].get("value"))
                                    )
                                    if group.get("values")
                                    else None,
                                ),
                                item.get("custom_fields_values"),
                            )
                        ),
                    },
                    contacts,
                )
            ),
            "Дополнительная информация": dict(
                map(
                    lambda item: (
                        item.get("field_name"),
                        list(
                            map(
                                lambda value: str(
                                    getattr(
                                        AmoCRMAPI, f'parse_{item.get("field_type")}'
                                    )(value.get("value"))
                                ),
                                item.get("values"),
                            )
                        ),
                    ),
                    lead.get("custom_fields_values"),
                )
            ),
            "События": events_list,
        }

    def get(self, lead: int, *args, **kwargs):
        self.context(
            "response",
            json.dumps(
                self._collect_data(self._get_lead(lead)),
                indent=4,
                ensure_ascii=False,
            ),
        )
        self.context("method", "")
        return super().get(*args, **kwargs)


class ApiView(AmoCRMAPIBaseView):
    template_name = "amocrm/index.html"
    title = "AmoCRM API"

    def get(self, method_name: str, *args, **kwargs):
        method = getattr(self, f"get_{method_name}", None)
        if method is not None:
            method()
        self.context(
            "response",
            json.dumps(
                self.amocrm.error
                if self.amocrm.error is not None
                else self.amocrm.response,
                indent=4,
                ensure_ascii=False,
            ),
        )
        self.context("method", method_name)
        return super().get(*args, **kwargs)

    def get_account(self):
        self.amocrm(
            "account",
            params={
                "with": "amojo_id,amojo_rights,users_groups,task_types,version,entity_names,datetime_settings,drive_url,is_api_filter_enabled",
            },
        )

    def get_leads(self):
        self.amocrm(
            "leads",
            params={
                "with": "catalog_elements,is_price_modified_by_robot,loss_reason,contacts,source_id",
            },
        )

    def get_leads_by_id(self):
        self.amocrm(
            f'leads/{request.args.get("lead")}',
            params={
                "with": "catalog_elements,is_price_modified_by_robot,loss_reason,contacts,source_id",
            },
        )

    def get_leads_pipelines(self):
        self.amocrm(
            "leads/pipelines",
        )

    def get_leads_pipelines_by_id(self):
        self.amocrm(
            f'leads/pipelines/{request.args.get("pipeline")}',
        )

    def get_leads_pipelines_by_id_statuses(self):
        self.amocrm(
            f'leads/pipelines/{request.args.get("pipeline")}/statuses',
            params={
                "with": "descriptions",
            },
        )

    def get_leads_pipelines_by_id_statuses_by_id(self):
        self.amocrm(
            f'leads/pipelines/{request.args.get("pipeline")}/statuses/{request.args.get("status")}',
            params={
                "with": "descriptions",
            },
        )

    def get_contacts(self):
        self.amocrm(
            "contacts",
            params={
                "with": "catalog_elements,leads,customers",
            },
        )

    def get_contacts_by_id(self):
        self.amocrm(
            f'contacts/{request.args.get("contact")}',
            params={
                "with": "catalog_elements,leads,customers",
            },
        )

    def get_companies(self):
        self.amocrm(
            "companies",
            params={
                "with": "catalog_elements,leads,customers,contacts",
            },
        )

    def get_companies_by_id(self):
        self.amocrm(
            f'companies/{request.args.get("company")}',
            params={
                "with": "catalog_elements,leads,customers,contacts",
            },
        )

    def get_catalogs(self):
        self.amocrm(
            "catalogs",
        )

    def get_catalogs_by_id(self):
        self.amocrm(
            f'catalogs/{request.args.get("catalog")}',
        )

    def get_catalogs_by_id_elements(self):
        self.amocrm(
            f'catalogs/{request.args.get("catalog")}/elements',
            params={
                "with": "invoice_link",
            },
        )

    def get_catalogs_by_id_elements_by_id(self):
        self.amocrm(
            f'catalogs/{request.args.get("catalog")}/elements/{request.args.get("element")}',
            params={
                "with": "invoice_link",
            },
        )

    def get_tasks(self):
        self.amocrm(
            "tasks",
        )

    def get_tasks_by_id(self):
        self.amocrm(
            f'tasks/{request.args.get("task")}',
        )

    def get_entity_tags(self):
        self.amocrm(
            f'{request.args.get("entity")}/tags',
        )

    def get_events(self):
        self.amocrm(
            "events",
            params={
                "with": "contact_name,lead_name,company_name,catalog_element_name,customer_name,catalog_name",
            },
        )

    def get_events_by_id(self):
        self.amocrm(
            f'events/{request.args.get("event")}',
        )

    def get_events_types(self):
        self.amocrm(
            "events/types",
            params={
                "language_code": "ru",
            },
        )

    def get_entity_notes(self):
        self.amocrm(
            f'{request.args.get("entity")}/notes',
        )

    def get_entity_by_id_notes(self):
        self.amocrm(
            f'{request.args.get("entity")}/{request.args.get("id")}/notes',
        )

    def get_entity_by_id_notes_by_id(self):
        self.amocrm(
            f'{request.args.get("entity")}/{request.args.get("id")}/notes/{request.args.get("note")}',
        )

    def get_users(self):
        self.amocrm(
            "users",
            params={
                "with": "role,group,uuid,amojo_id,user_rank",
            },
        )

    def get_users_by_id(self):
        self.amocrm(
            f'users/{request.args.get("user")}',
            params={
                "with": "role,group,uuid,amojo_id,user_rank",
            },
        )

    def get_roles(self):
        self.amocrm(
            "roles",
            params={
                "with": "users",
            },
        )

    def get_roles_by_id(self):
        self.amocrm(
            f'roles/{request.args.get("role")}',
            params={
                "with": "users",
            },
        )

    def get_webhooks(self):
        self.amocrm(
            "webhooks",
        )

    def get_widgets(self):
        self.amocrm(
            "widgets",
        )

    def get_widgets_by_code(self):
        self.amocrm(
            f'widgets/{request.args.get("code")}',
        )
