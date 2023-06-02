from celery import shared_task
from datetime import datetime, timedelta

# from analytic.plugins.tg_report import TGReportChannelsSummary


@shared_task
def report_channels_summary(debug: bool = False):
    datetime_now = datetime.now()

    if debug:
        datetime_now = datetime_now - timedelta(weeks=2)
        days_delta = 7
    else:
        if datetime_now.weekday() == 0:
            days_delta = 4
        elif datetime_now.weekday() == 3:
            days_delta = 3
        else:
            days_delta = 0

    time_kwargs = {"microsecond": 0, "second": 0, "minute": 0, "hour": 11}

    date_from = datetime_now.replace(**time_kwargs) - timedelta(days=days_delta)
    date_to = datetime_now.replace(**time_kwargs)

    report = TGReportChannelsSummary(daterange=(date_from, date_to))
    report.send()
