from typing import Dict, Any

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
    visits_cost = db.Column(db.Float, default=0)
    date = db.Column(db.Date)
    package_id = db.Column(db.ForeignKey("roistat_packages.id"))
    level_1_id = db.Column(db.ForeignKey("roistat_levels.id"))
    level_2_id = db.Column(db.ForeignKey("roistat_levels.id"))
    level_3_id = db.Column(db.ForeignKey("roistat_levels.id"))
    level_4_id = db.Column(db.ForeignKey("roistat_levels.id"))
    level_5_id = db.Column(db.ForeignKey("roistat_levels.id"))
    level_6_id = db.Column(db.ForeignKey("roistat_levels.id"))
    level_7_id = db.Column(db.ForeignKey("roistat_levels.id"))
