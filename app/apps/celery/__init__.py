from celery import Celery
from celery.schedules import crontab

from flask import Flask


class CeleryApp(Celery):
    def __init__(self, flask_app: Flask, *args, **kwargs):
        kwargs.update({"main": flask_app.name})
        super().__init__(*args, **kwargs)

        flask_app.extensions.update({"celery": self})

        self.conf.update(
            {
                "beat_schedule": {
                    "report-channels-summary": {
                        "task": "app.celery.tasks.report_channels_summary",
                        "schedule": crontab(minute="0", hour="11", day_of_week="1,4"),
                    }
                },
            }
        )
