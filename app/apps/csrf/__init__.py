from flask_wtf.csrf import CSRFProtect

from flask import Flask


class CSRFApp(CSRFProtect):
    def __init__(self, flask_app: Flask, *args, **kwargs):
        super().__init__(flask_app, *args, **kwargs)

        flask_app.extensions.update({"csrf": self})
