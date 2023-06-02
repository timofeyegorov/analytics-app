from flask import Flask
from flask_wtf.csrf import CSRFProtect


class CSRFApplication(CSRFProtect):
    def __init__(self, app: Flask, *args, **kwargs):
        super().__init__(app, *args, **kwargs)
        app.extensions.update({"csrf": self})
