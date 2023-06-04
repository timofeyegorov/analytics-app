import datetime
import itertools

from typing import List, Dict, Optional
from flask import render_template
from wtforms import (
    Field as FieldWT,
    StringField as StringFieldWT,
    PasswordField as PasswordFieldWT,
    SubmitField as SubmitFieldWT,
    DateField as DateFieldWT,
    SelectField as SelectFieldWT,
    HiddenField as HiddenFieldWT,
)
from wtforms.utils import unset_value
from markupsafe import Markup

from . import widgets


class BaseField(FieldWT):
    template: str = "forms/field.html"
    placeholder: str
    multivalue: bool = False
    is_action: bool = False
    attrs: Dict[str, str] = {}

    def __init__(
        self,
        is_action: bool = False,
        multivalue: bool = False,
        placeholder: Optional[str] = None,
        attrs: Optional[Dict[str, str]] = None,
        *args,
        **kwargs,
    ):
        self.placeholder = placeholder
        self.multivalue = multivalue
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

    def render_one(self, index: int):
        return Markup(render_template(self.get_template(), field=self, index=index))

    def preprocess(self, valuelist: List[str]) -> List[str]:
        if not isinstance(valuelist, list):
            valuelist = [valuelist]
        return valuelist

    def process_formdata(self, valuelist: List[str]):
        valuelist = self.preprocess(valuelist)
        if not self.multivalue:
            valuelist = valuelist[0] if valuelist else None
        self.data = valuelist

    def _to_str(self, value) -> str:
        return str(value) if value is not None else ""

    def _value(self, index: str = None):
        if self.multivalue:
            if index is None:
                return list(map(lambda value: self._to_str(value), self.data))
            else:
                try:
                    return self._to_str(self.data[int(index)])
                except IndexError:
                    return ""
        else:
            return self._to_str(self.data)


class StringBaseField(BaseField):
    pass


class HiddenBaseField(StringBaseField):
    template: str = "forms/hidden.html"


class DateBaseField(BaseField):
    def process_formdata(self, valuelist: List[str]):
        valuelist = self.preprocess(valuelist)

        error_values = []
        for index, value in enumerate(valuelist):
            for pattern in self.strptime_format:
                try:
                    valuelist[index] = datetime.datetime.strptime(value, pattern).date()
                except ValueError:
                    error_values.append(value)
                    valuelist[index] = None

        if not self.multivalue:
            self.data = valuelist[0] if valuelist else None
        else:
            self.data = valuelist

        if not self.multivalue:
            valuelist = valuelist[:1] if valuelist else []

        if list(filter(lambda item: item is None, valuelist)):
            raise ValueError(f'Некорректное значение даты: {", ".join(error_values)}')


class SelectBaseField(BaseField):
    def update_choices(self, choices=None):
        if callable(choices):
            choices = choices()
        if choices is not None:
            self.choices = choices if isinstance(choices, dict) else list(choices)
        else:
            self.choices = None

    def iter_choices(self, index: str = None):
        if not self.choices:
            choices = []
        elif isinstance(self.choices, dict):
            choices = list(itertools.chain.from_iterable(self.choices.values()))
        else:
            choices = self.choices

        return self._choices_generator(choices, index)

    def iter_groups(self, index: str = None):
        if isinstance(self.choices, dict):
            for label, choices in self.choices.items():
                yield label, self._choices_generator(choices, index)

    def _choices_generator(self, choices, index: str = None):
        if not choices:
            _choices = []

        elif isinstance(choices[0], (list, tuple)):
            _choices = choices

        else:
            _choices = zip(choices, choices)

        try:
            compare_value = self.data if index is None else self.data[int(index)]
        except IndexError:
            compare_value = ""
        for value, label in _choices:
            yield value, label, self.coerce(value) == compare_value


class SubmitBaseField(BaseField):
    pass


class StringField(StringBaseField, StringFieldWT):
    widget = widgets.TextInput()


class PasswordField(StringBaseField, PasswordFieldWT):
    widget = widgets.PasswordInput()


class HiddenField(HiddenBaseField, HiddenFieldWT):
    widget = widgets.HiddenInput()


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
