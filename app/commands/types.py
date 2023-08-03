import datetime


def date(value: str) -> datetime.date:
    return datetime.datetime.fromisoformat(value).date()
