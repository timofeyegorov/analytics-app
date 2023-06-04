from flask import current_app


db = current_app.app("database")


class BaseMixin:
    id = db.Column(db.Integer, primary_key=True)


class User(BaseMixin, db.Model):
    __tablename__ = "users"

    login = db.Column(db.String(255))
    password = db.Column(db.String(255))
