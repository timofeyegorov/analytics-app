from flask import Flask
from celery import Celery
from celery.schedules import crontab

from app.core.application import Application


class CeleryApp(Application, Celery):
    def __init__(self, flask_app: Flask, *args, **kwargs):
        kwargs.update({"main": flask_app.name})
        super().__init__(*args, **kwargs)

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
