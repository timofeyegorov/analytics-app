from typing import Tuple
from datetime import date, timedelta


def detect_week(value: date) -> Tuple[date, date]:
    start_week = 3
    date_from = value - timedelta(
        days=value.weekday() + (7 if value.weekday() < start_week else 0) - start_week
    )
    date_to = date_from + timedelta(days=6)
    return date_from, date_to
