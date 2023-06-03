from flask import session
from hashlib import md5

from app import forms
from app.apps.database import models

from . import serializers


class LoginForm(forms.BaseForm):
    action = "login"
    method = forms.data.FormMethodEnum.post
    serializer_class = serializers.LoginSerializer

    login = forms.fields.StringField(
        label="Логин", validators=[forms.validators.DataRequired()]
    )
    password = forms.fields.PasswordField(
        label="Пароль", validators=[forms.validators.DataRequired()]
    )
    auth = forms.fields.SubmitField(placeholder="Войти", is_action=True)

    def validate_on_submit(self, *args, **kwargs) -> bool:
        is_valid = super().validate_on_submit(*args, **kwargs)
        if is_valid:
            user = models.User.query.filter_by(
                login=self.serializer.login,
                password=md5(self.serializer.password.encode("utf-8")).hexdigest(),
            ).first()
            if user is None:
                is_valid = False
                self.set_error("Неверный логин или пароль")
                self.password.data = ""
            else:
                session.setdefault("user", user.id)
        return is_valid


class LogoutForm(forms.BaseForm):
    action = "index"
    method = forms.data.FormMethodEnum.post
    serializer_class = serializers.LogoutSerializer

    logout = forms.fields.SubmitField(
        placeholder="Выйти", is_action=True, attrs={"class": "btn btn-danger"}
    )
