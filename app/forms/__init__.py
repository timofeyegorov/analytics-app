from typing import Dict, Type
from markupsafe import Markup
from collections import OrderedDict

from flask import render_template, url_for, request
from flask_wtf import FlaskForm
from flask_wtf.form import SUBMIT_METHODS

from . import fields, validators, data, context_processors, serializers


class BaseForm(FlaskForm):
    template = "forms/form.html"
    action: str = ""
    method: str = data.FormMethodEnum.get
    serializer_class: Type[serializers.BaseSerializer]
    serializer: serializers.BaseSerializer
    attrs: Dict[str, str] = {}

    def __init__(self, *args, **kwargs):
        if self.method.value in SUBMIT_METHODS:
            super(BaseForm, self).__init__(*args, **kwargs)
        else:
            super(BaseForm, self).__init__(request.args, *args, **kwargs)

    def get_template(self) -> str:
        return self.template

    def get_serializer(self) -> serializers.BaseSerializer:
        return self.serializer_class(**self.data)

    def validate_on_submit(self, *args, **kwargs) -> bool:
        if self.method.value in SUBMIT_METHODS:
            is_valid = super().validate_on_submit(*args, **kwargs)
        else:
            is_valid = self.validate()
        if is_valid:
            self.serializer = self.get_serializer()
        return is_valid

    def render(self) -> str:
        attrs = {
            "id": f"form-{self.action}",
            "action": url_for(self.action),
            "method": self.method.value,
            "autocomplete": "OFF",
            "novalidate": "novalidate",
            **self.attrs,
        }
        init_fields = {}
        csrf_field = self._fields.get("csrf_token")
        if csrf_field is not None:
            init_fields.setdefault("csrf_token", csrf_field)
        fields = OrderedDict(**init_fields)
        actions = OrderedDict()
        for field_name, field in self._fields.items():
            if field_name == "csrf_token":
                continue
            if field.is_action:
                actions.setdefault(field_name, field)
            else:
                fields.setdefault(field_name, field)
        return Markup(
            render_template(
                self.get_template(),
                attrs=attrs,
                fields=fields,
                actions=actions,
                form=self,
            )
        )

    def set_error(self, message: str):
        self.form_errors.append(message)


class FilterForm(BaseForm):
    attrs = {"class": "form-filter"}

    class Meta:
        csrf = False
