import datetime

from typing import Dict, Optional
from flask import render_template
from wtforms import (
    Field as FieldWT,
    StringField as StringFieldWT,
    PasswordField as PasswordFieldWT,
    SubmitField as SubmitFieldWT,
    DateField as DateFieldWT,
    SelectField as SelectFieldWT,
)
from markupsafe import Markup

from . import widgets


class BaseField(FieldWT):
    template: str = "forms/field.html"
    placeholder: str
    is_action: bool = False
    attrs: Dict[str, str] = {}

    def __init__(
        self,
        is_action: bool = False,
        placeholder: Optional[str] = None,
        attrs: Optional[Dict[str, str]] = None,
        *args,
        **kwargs,
    ):
        self.placeholder = placeholder
        self.attrs = attrs if attrs is not None else {}
        self.is_action = is_action
        label = kwargs.get("label")
        super().__init__(*args, **kwargs)
        if label is None:
            self.label = ""

    def get_template(self) -> str:
        return self.template

    def get_attrs(self) -> Dict[str, str]:
        return self.attrs

    def render(self):
        return Markup(render_template(self.get_template(), field=self))


class StringBaseField(BaseField):
    pass


class DateBaseField(BaseField):
    def process_formdata(self, valuelist):
        if isinstance(valuelist, list):
            valuelist = list(filter(None, valuelist))

        if not valuelist:
            return

        date_str = " ".join(valuelist)
        for date_format in self.strptime_format:
            try:
                self.data = datetime.datetime.strptime(date_str, date_format).date()
                return
            except ValueError:
                self.data = None

        raise ValueError(f'Некорректное значение даты: {" ".join(valuelist)}')


class SelectBaseField(BaseField):
    def update_choices(self, choices=None):
        if callable(choices):
            choices = choices()
        if choices is not None:
            self.choices = choices if isinstance(choices, dict) else list(choices)
        else:
            self.choices = None


class SubmitBaseField(BaseField):
    pass


class StringField(StringBaseField, StringFieldWT):
    widget = widgets.TextInput()


class PasswordField(StringBaseField, PasswordFieldWT):
    widget = widgets.PasswordInput()


class DateField(DateBaseField, DateFieldWT):
    widget = widgets.DateInput()


class SelectField(SelectBaseField, SelectFieldWT):
    widget = widgets.Select()

    def __init__(self, *args, **kwargs):
        kwargs.update({"validate_choice": False})
        super().__init__(*args, **kwargs)


class SubmitField(SubmitBaseField, SubmitFieldWT):
    widget = widgets.SubmitInput()


class ResetField(SubmitField):
    widget = widgets.ResetInput()

    def __init__(self, *args, **kwargs):
        kwargs.update({"attrs": {"class": "btn btn-secondary"}})
        super().__init__(*args, **kwargs)
