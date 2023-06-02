from flask import Flask
from flask_minify import Minify


class MinifyApplication(Minify):
    def __init__(self, app: Flask, *args, **kwargs):
        super().__init__(app, *args, **kwargs)
        app.extensions.update({"minify": self})
