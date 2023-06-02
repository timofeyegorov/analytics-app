from flask import current_app
from typing import Dict
from markupsafe import Markup


@current_app.context_processor
def utility_processor():
    def render_attrs(attrs: Dict[str, str]) -> str:
        attrs_str = " ".join(
            list(map(lambda item: f'{item[0]}="{item[1]}"', attrs.items()))
        )
        if attrs_str:
            attrs_str = f" {attrs_str}"
        return Markup(attrs_str)

    return dict(render_attrs=render_attrs)
