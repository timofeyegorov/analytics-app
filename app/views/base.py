from typing import Dict, Any, Type

from flask import render_template, request, session, redirect, url_for, current_app
from flask.views import MethodView
from flask.typing import ResponseReturnValue
from flask_wtf.form import SUBMIT_METHODS

from werkzeug.routing.exceptions import BuildError

from app import forms
from app.apps.database import models

from .context import ContextTemplate


class AuthViewMixin(MethodView):
    def _authenticate(self) -> bool:
        user_id = session.get("user")
        request.user = models.User.query.filter_by(id=user_id).first()
        return request.user is not None

    def dispatch_request(self, *args: Any, **kwargs: Any) -> ResponseReturnValue:
        login_url = url_for("login")
        if not self._authenticate():
            if login_url != request.path:
                return redirect(login_url)
        else:
            if login_url == request.path:
                return redirect(url_for("index"))
        return super().dispatch_request(*args, **kwargs)


class BaseView(AuthViewMixin):
    def get(self, *args, **kwargs):
        return self.render()

    def post(self, *args, **kwargs):
        return self.render()


class TemplateView(BaseView):
    context: ContextTemplate = ContextTemplate()
    template_name: str
    title: str = ""

    def get_template_name(self) -> str:
        return self.template_name

    def get_title(self) -> str:
        return self.title

    def get_title_page(self) -> str:
        return self.get_title()

    def get_title_html(self) -> str:
        config = current_app.config
        site_name = config.get("SITE_NAME", "")
        delimiter = config.get("SITE_TITLE_DELIMITER", " - ") if site_name else ""
        title = self.get_title()
        return f"{title}{delimiter}{site_name}" if title else site_name

    def get_context(self) -> Dict[str, Any]:
        return self.context

    def render(self):
        self.context("user", request.user)
        self.context("title_page", self.get_title_page())
        self.context("title_html", self.get_title_html())
        return render_template(self.get_template_name(), **self.get_context())


class FormView(TemplateView):
    form_class: Type[forms.BaseForm]
    form: forms.BaseForm
    success_url: str

    def dispatch_request(self, *args: Any, **kwargs: Any) -> ResponseReturnValue:
        self.form = self.get_form()
        if self.form.validate_on_submit():
            self.form_valid()
            if self.form.method.value in SUBMIT_METHODS:
                return redirect(self.get_success_url())
        else:
            self.form_invalid()
        return super().dispatch_request(*args, **kwargs)

    def get_form(self) -> forms.BaseForm:
        return self.form_class()

    def get_success_url(self) -> str:
        success_url = getattr(self, "success_url", None)
        if success_url is None:
            success_url = self.form.action
        try:
            url = url_for(success_url)
        except BuildError:
            url = success_url
        return url

    def form_valid(self):
        pass

    def form_invalid(self):
        pass

    def get_context(self) -> Dict[str, Any]:
        data = super().get_context()
        data.update({"form": self.form})
        return data
