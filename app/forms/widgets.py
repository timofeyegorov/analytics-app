from typing import List, Any

from flask import render_template
from wtforms.widgets import (
    Input as InputWT,
    TextInput as TextInputWT,
    PasswordInput as PasswordInputWT,
    SubmitInput as SubmitInputWT,
    DateInput as DateInputWT,
    Select as SelectWT,
    HiddenInput as HiddenInputWT,
)
from markupsafe import Markup


class BaseInput(InputWT):
    template: str = "forms/widgets/input.html"

    def __call__(self, field, **kwargs):
        index = str(kwargs.pop("index", "")) or None
        if not field.is_action:
            kwargs.setdefault("name", field.id)
        kwargs.setdefault("id", field.id if index is None else f"{field.id}_{index}")
        kwargs.setdefault("type", self.input_type)
        if "value" not in kwargs:
            kwargs["value"] = field._value(index)
        flags = getattr(field, "flags", {})
        for k in dir(flags):
            if k in self.validation_attrs and k not in kwargs:
                kwargs[k] = getattr(flags, k)
        if field.is_action:
            kwargs.setdefault("class", "btn btn-primary")
        else:
            kwargs.setdefault("class", "form-control")
        if field.placeholder is not None:
            kwargs.setdefault("placeholder", field.placeholder)
        kwargs.update(**field.get_attrs())
        return Markup(render_template(self.get_template(), attrs=kwargs))

    def get_template(self) -> str:
        return self.template


class BaseSelect(SelectWT):
    template: str = "forms/widgets/select.html"
    validation_attrs = []

    def __call__(self, field, **kwargs):
        index = str(kwargs.pop("index", "")) or None
        if not field.is_action:
            kwargs.setdefault("name", field.id)
        kwargs.setdefault("id", field.id if index is None else f"{field.id}_{index}")
        if self.multiple:
            kwargs["multiple"] = "multiple"
        flags = getattr(field, "flags", {})
        for k in dir(flags):
            if k in self.validation_attrs and k not in kwargs:
                kwargs[k] = getattr(flags, k)
        kwargs.setdefault("class", "form-select")
        if field.has_groups():
            options = []
            for group, choices in field.iter_groups(index):
                if group:
                    options.append(
                        {
                            "name": group,
                            "options": self._get_options_from_generator(choices),
                        }
                    )
                else:
                    options += self._get_options_from_generator(choices)
        else:
            options = self._get_options_from_generator(field.iter_choices(index))
        kwargs.update(**field.get_attrs())
        return Markup(
            render_template(self.get_template(), attrs=kwargs, options=options)
        )

    def _get_options_from_generator(self, options) -> List[List[Any]]:
        return list(map(lambda item: ["option"] + list(item), options))

    def get_template(self) -> str:
        return self.template


class BaseSubmit(BaseInput):
    def __call__(self, field, **kwargs):
        kwargs.setdefault("value", field.placeholder)
        field.placeholder = None
        return super().__call__(field, **kwargs)


class TextInput(BaseInput, TextInputWT):
    validation_attrs = ["maxlength", "minlength", "pattern"]


class PasswordInput(BaseInput, PasswordInputWT):
    validation_attrs = ["maxlength", "minlength", "pattern"]


class HiddenInput(BaseInput, HiddenInputWT):
    validation_attrs = []


class DateInput(BaseInput, DateInputWT):
    validation_attrs = ["max", "min", "step"]


class Select(BaseSelect, SelectWT):
    validation_attrs = []


class SubmitInput(BaseSubmit, SubmitInputWT):
    validation_attrs = []


class ResetInput(SubmitInput):
    input_type = "reset"
