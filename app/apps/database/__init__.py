from flask_sqlalchemy import SQLAlchemy

from app.core.application import Application


class DatabaseApp(Application, SQLAlchemy):
    pass
