from typing import Tuple, List, Dict, Any, Union
from importlib import import_module
from dataclasses import dataclass

from flask import current_app


@dataclass
class RouteData:
    name: str
    url: str
    view: Any
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]


@dataclass
class RouteGroupData:
    name: str
    url: str
    items: List[Union["RouteGroupData", "RouteData"]]


class Routing:
    _patterns: RouteGroupData
    _cursor: RouteGroupData

    def module(self, module_path: str, url: str = "", ignore_name: bool = False):
        module_name = "" if ignore_name else module_path.split(".")[:-1].pop()
        patterns = RouteGroupData(
            name=module_name,
            url=url,
            items=[],
        )
        if not hasattr(self, "_patterns"):
            self._patterns = patterns
        else:
            self._cursor.items.append(patterns)
        if hasattr(self, "_cursor"):
            _cursor = self._cursor
        else:
            _cursor = patterns
        self._cursor = patterns
        import_module(module_path)
        self._cursor = _cursor

    def url(self, url: str, name: str, view: Any, *args, **kwargs):
        self._cursor.items.append(
            RouteData(
                name=name,
                url=url,
                view=view,
                args=args,
                kwargs=kwargs,
            )
        )

    def build(self, group: RouteGroupData = None, url: str = "", prefix: str = ""):
        if group is None:
            group = self._patterns
        group_name = ":".join(list(filter(None, [prefix, group.name])))
        group_url = "".join(list(filter(None, [url, group.url])))
        for item in group.items:
            if isinstance(item, RouteGroupData):
                self.build(item, group_url, group_name)
            else:
                item_name = ":".join(list(filter(None, [group_name, item.name])))
                item_url = "".join(list(filter(None, [group_url, item.url])))
                current_app.add_url_rule(
                    item_url,
                    view_func=item.view.as_view(item_name),
                    *item.args,
                    **item.kwargs
                )
