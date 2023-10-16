from typing import Dict, Any

from flask import render_template
from flask.views import MethodView


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


class LeadsView(TemplateView):
    template_name = "update/leads.html"
    title = "Обновление лидов Тильды"
