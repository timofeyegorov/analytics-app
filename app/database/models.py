from typing import Dict, Any
from sqlalchemy.sql import functions
from sqlalchemy.ext.hybrid import hybrid_property

from app import db


class Users(db.Model):
    __tablename__ = "users"
    id = db.Column(db.Integer, primary_key=True)
    login = db.Column(db.String(255), index=True, unique=True)
    password = db.Column(db.String(255))


class RoistatPackages(db.Model):
    __tablename__ = "roistat_packages"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(32), index=True, unique=True)
    title = db.Column(db.String(32), index=True)

    _cache: Dict[str, Any] = {}

    @classmethod
    def get_or_create(cls, name: str, title: str):
        unique_str = f"{name}_{title}"
        package = cls._cache.get(unique_str)
        if package:
            return package

        package = cls.query.filter_by(name=name, title=title).one_or_none()
        if package is None:
            package = cls(name=name, title=title)
            db.session.add(package)
            db.session.commit()

        cls._cache[unique_str] = package

        return package


class RoistatLevels(db.Model):
    __tablename__ = "roistat_levels"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(512), index=True)
    title = db.Column(db.String(512), index=True)
    level = db.Column(db.Integer, index=True)

    _cache: Dict[str, Any] = {}

    @classmethod
    def get_or_create(cls, name: str, title: str, level_no: int):
        unique_str = f"{name}_{title}_{level_no}"
        level = cls._cache.get(unique_str)
        if level:
            return level

        level = cls.query.filter_by(
            name=name, title=title, level=level_no
        ).one_or_none()
        if level is None:
            level = cls(name=name, title=title, level=level_no)
            db.session.add(level)
            db.session.commit()

        cls._cache[unique_str] = level

        return level


class Roistat(db.Model):
    __tablename__ = "roistat"
    id = db.Column(db.Integer, primary_key=True)
    visits_cost = db.Column(db.Float, default=0, index=True)
    date = db.Column(db.Date, index=True)
    package_id = db.Column(db.ForeignKey("roistat_packages.id"), index=True)
    level_1_id = db.Column(db.ForeignKey("roistat_levels.id"), index=True)
    level_2_id = db.Column(db.ForeignKey("roistat_levels.id"), index=True)
    level_3_id = db.Column(db.ForeignKey("roistat_levels.id"), index=True)
    level_4_id = db.Column(db.ForeignKey("roistat_levels.id"), index=True)
    level_5_id = db.Column(db.ForeignKey("roistat_levels.id"), index=True)
    level_6_id = db.Column(db.ForeignKey("roistat_levels.id"), index=True)
    level_7_id = db.Column(db.ForeignKey("roistat_levels.id"), index=True)
    account_id = db.Column(db.ForeignKey("roistat_levels.id"), index=True)
    campaign_id = db.Column(db.ForeignKey("roistat_levels.id"), index=True)
    group_id = db.Column(db.ForeignKey("roistat_levels.id"), index=True)
    ad_id = db.Column(db.ForeignKey("roistat_levels.id"), index=True)


class TildaLead(db.Model):
    __tablename__ = "tilda_leads"
    id = db.Column(db.Integer, primary_key=True)
    created = db.Column(db.DateTime, index=True)
    quiz_answers1 = db.Column(db.String(256), index=True)
    quiz_answers2 = db.Column(db.String(256), index=True)
    quiz_answers3 = db.Column(db.String(256), index=True)
    quiz_answers4 = db.Column(db.String(256), index=True)
    quiz_answers5 = db.Column(db.String(256), index=True)
    quiz_answers6 = db.Column(db.String(256), index=True)
    sp_book_id = db.Column(db.String(16), index=True)
    roistat_fields_roistat = db.Column(db.String(16), index=True)
    name = db.Column(db.String(2048), index=True)
    phone = db.Column(db.String(32), index=True)
    email = db.Column(db.String(128), index=True)
    roistat_url = db.Column(db.String(4096), index=True)
    formid = db.Column(db.String(32), index=True)
    formname = db.Column(db.String(32), index=True)
    referer = db.Column(db.String(4096), index=True)
    checkbox = db.Column(db.String(16), index=True)

    @hybrid_property
    def identifier_exp(self):
        return functions.concat(self.created, "_", self.phone, "_", self.email)

    @property
    def identifier(self) -> str:
        return f"{self.created}_{self.phone}_{self.email}"
