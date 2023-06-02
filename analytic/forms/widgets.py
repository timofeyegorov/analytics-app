from flask import render_template
from wtforms.widgets import (
    Input,
    TextInput as TextInputWT,
    PasswordInput as PasswordInputWT,
    SubmitInput as SubmitInputWT,
    DateInput as DateInputWT,
)
from markupsafe import Markup


class BaseInput(Input):
    template: str = "forms/widgets/input.html"

    def __call__(self, field, **kwargs):
        if not field.is_action:
            kwargs.setdefault("name", field.id)
        kwargs.setdefault("id", field.id)
        kwargs.setdefault("type", self.input_type)
        if "value" not in kwargs:
            kwargs["value"] = field._value()
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


class BaseSubmit(BaseInput):
    def __call__(self, field, **kwargs):
        kwargs.setdefault("value", field.placeholder)
        field.placeholder = None
        return super().__call__(field, **kwargs)


class TextInput(BaseInput, TextInputWT):
    validation_attrs = ["maxlength", "minlength", "pattern"]


class PasswordInput(BaseInput, PasswordInputWT):
    validation_attrs = ["maxlength", "minlength", "pattern"]


class DateInput(BaseInput, DateInputWT):
    validation_attrs = ["max", "min", "step"]


class SubmitInput(BaseSubmit, SubmitInputWT):
    validation_attrs = []


class ResetInput(SubmitInput):
    input_type = "reset"
