from flask import session

from analytic.views.base import TemplateView, FormView
from analytic.views.forms import LoginForm, LogoutForm


class LoginView(FormView):
    template_name = "login.html"
    title = "Авторизация"
    form_class = LoginForm
    success_url = "index"


class IndexView(FormView):
    template_name = "index.html"
    title = "Меню"
    form_class = LogoutForm
    success_url = "login"

    def form_valid(self):
        session.clear()


class NotFoundView(TemplateView):
    template_name = "404.html"
    title = "Страница не найдена"

    def get(self, path: str, *args, **kwargs):
        self.context("path", path)
        return super().get(*args, **kwargs), 404
