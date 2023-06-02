from flask import Flask
from celery import Celery
from celery.schedules import crontab


class CeleryApplication(Celery):
    def __init__(self, app: Flask, *args, **kwargs):
        kwargs.update(
            {
                "main": app.name,
                **app.config.get("CELERY"),
            }
        )
        super().__init__(*args, **kwargs)

        self.conf.update(
            {
                **app.config,
                "beat_schedule": {
                    "report-channels-summary": {
                        "task": "analytic.celery.tasks.report_channels_summary",
                        "schedule": crontab(minute="0", hour="11", day_of_week="1,4"),
                    }
                },
            }
        )

        app.extensions.update({"celery": self})
