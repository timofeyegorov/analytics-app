from flask_minify import Minify

from flask import Flask


class MinifyApp(Minify):
    def __init__(self, flask_app: Flask, *args, **kwargs):
        super().__init__(flask_app, *args, **kwargs)

        flask_app.extensions.update({"minify": self})
