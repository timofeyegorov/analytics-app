from flask_sqlalchemy import SQLAlchemy

from flask import Flask


class DatabaseApp(SQLAlchemy):
    def __init__(self, flask_app: Flask, *args, **kwargs):
        super().__init__(flask_app, *args, **kwargs)

        flask_app.extensions.update({"database": self})
