import os
import json
import redis
import click
import pandas as pd
import pickle as pkl

from flask import Flask, request, render_template
from flask.cli import with_appcontext
from flask_sqlalchemy import SQLAlchemy
from .database.auth import check_token
from .database import get_leads_data, get_target_audience
from config import config
from config import RESULTS_FOLDER
from celery import Celery
from celery.schedules import crontab
from datetime import datetime, timedelta
from app.plugins.tg_report import TGReportChannelsSummary
from app import decorators, commands


# def fig_leads_dynamics():
#   result = get_leads_data()
#   result['created_at'] = pd.to_datetime(result['created_at'])
#   leads_day_df = result.resample('D', on='created_at')['id'].count().to_frame()
#   fig = px.histogram(result, x = 'created_at', nbins = leads_day_df.shape[0])
#   fig.update_layout(title = 'Количество лидов по дням', title_x = 0.5,
#                     xaxis_title='Дата',
#                     yaxis_title='Лиды',)
#   graphJSON = to_json(fig)
#   # graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
#   return graphJSON

app = Flask(__name__, static_url_path="/assets", static_folder="assets")
app.config.from_object("config")

for bp in commands.bps:
    app.register_blueprint(bp)


redis_config = config["redis"]
redis_db = redis.StrictRedis(
    host=redis_config["host"],
    port=redis_config["port"],
    charset="utf-8",
    db=redis_config["db"],
)

app.jinja_env.globals["redis_db"] = redis_db

db = SQLAlchemy(app)


celery_client = Celery(
    app.name,
    broker=app.config["CELERY_BROKER_URL"],
    backend=app.config["CELERY_BACKEND_URL"],
)
celery_client.conf.update(
    {
        **app.config,
        "beat_schedule": {
            "report-channels-summary": {
                "task": "app.report_channels_summary",
                "schedule": crontab(minute="0", hour="11", day_of_week="1,4"),
            }
        },
    }
)


@celery_client.task
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


@app.route("/", endpoint="root")
@app.route("/index", endpoint="index")
@decorators.auth
def index():
    return render_template(
        "index.html"
    )  # , fig_leads_dynamics=fig_leads_dynamics)


@app.route("/leads", endpoint="leads")
@decorators.auth
def data():
    data = get_leads_data()
    data.traffic_channel = data.traffic_channel.str.split("?").str[0]
    return render_template(
        "leads.html", tables={"Leads": data.drop(columns=["id"])}
    )


@app.route("/trafficologists", endpoint="trafficologists")
@decorators.auth
def trafficologist_page(trafficologist_error=None, account_error=None):
    with open(os.path.join(RESULTS_FOLDER, "trafficologists.pkl"), "rb") as f:
        trafficologists = pkl.load(f)
    # accounts = get_accounts()
    # trafficologists = get_trafficologists()
    # trafficologists2 = get_trafficologists()
    # print(trafficologists)
    return render_template(
        "trafficologists.html",
        tables=[trafficologists.to_html()],
    )  # ,
    # trafficologists=zip(trafficologists.id, trafficologists.name),
    # trafficologists2=zip(trafficologists2.id, trafficologists2.name),
    # trafficologist_error=trafficologist_error,
    # account_error=account_error)


@app.route("/target_audience", endpoint="target_audience")
@decorators.auth
def target_audience():
    with open(os.path.join(RESULTS_FOLDER, "target_audience.pkl"), "rb") as f:
        target_audience = pkl.load(f)
    # return render_template('target_audience.html', target_audience=get_target_audience())
    return render_template(
        "target_audience.html", target_audience=target_audience
    )


@app.route("/crops", endpoint="crops")
@decorators.auth
def crops():
    with open(os.path.join(RESULTS_FOLDER, "crops_list.pkl"), "rb") as f:
        crops = pkl.load(f)
    # return render_template('target_audience.html', target_audience=get_target_audience())
    return render_template("crops.html", crops=crops)


@app.route("/expenses", endpoint="expenses")
@decorators.auth
def expenses():
    with open(
        os.path.join(RESULTS_FOLDER, "expenses.json"), "r", encoding="cp1251"
    ) as f:
        expenses = json.load(f)
    exp = []
    for i in range(len(expenses)):
        for k, v in expenses[i]["items"].items():
            exp.append(
                [
                    k,
                    v,
                    expenses[i]["dateFrom"],
                ]
            )
    exp = pd.DataFrame(exp, columns=["Ройстат", "Расход", "Дата"])
    exp["Дата"] = pd.to_datetime(exp["Дата"]).dt.normalize()
    output_dict = {
        "Кол-во записей - ": exp.shape,
        "Расход с 01.11 по 30.11 - ": round(
            exp[(exp["Дата"] >= "2021-11-01") & (exp["Дата"] <= "2021-11-30")][
                "Расход"
            ].sum()
        ),
        "Расход с 01.12 по 31.12 - ": round(
            exp[(exp["Дата"] >= "2021-12-01") & (exp["Дата"] <= "2021-12-31")][
                "Расход"
            ].sum()
        ),
        "Расход по facebook32 с 01.11 по 30.11 - ": round(
            exp[
                (exp["Ройстат"] == "Facebook 32")
                & (exp["Дата"] >= "2021-11-01")
                & (exp["Дата"] <= "2021-11-30")
            ]["Расход"].sum()
        ),
        "Расход по Михаил с 01.11 по 30.11 - ": round(
            exp[
                (
                    (exp["Ройстат"] == "Facebook Michail Zh (ИП2)")
                    | (exp["Ройстат"] == "Facebook 31")
                )
                & (exp["Дата"] >= "2021-11-01")
                & (exp["Дата"] <= "2021-11-30")
            ]["Расход"].sum()
        ),
        "Расход по facebook32 с 01.12 по 31.12 - ": round(
            exp[
                (exp["Ройстат"] == "Facebook 32")
                & (exp["Дата"] >= "2021-12-01")
                & (exp["Дата"] <= "2021-12-31")
            ]["Расход"].sum()
        ),
        "Расход по Михаил с 01.12 по 31.12 - ": round(
            exp[
                (
                    (exp["Ройстат"] == "Facebook Michail Zh (ИП2)")
                    | (exp["Ройстат"] == "Facebook 31")
                )
                & (exp["Дата"] >= "2021-12-01")
                & (exp["Дата"] <= "2021-12-31")
            ]["Расход"].sum()
        ),
        "Расход с 1 по 20 января": [
            round(
                exp[
                    (exp["Дата"] >= "2022-01-01")
                    & (exp["Дата"] <= "2022-01-20")
                ]["Расход"].sum()
            ),
            round(
                exp[
                    (exp["Дата"] >= "2022-01-01")
                    & (exp["Дата"] <= "2022-01-20")
                ]["Расход"].sum()
            )
            * 1.2,
        ],
        "Расход с 1 по 21 января": [
            round(
                exp[
                    (exp["Дата"] >= "2022-01-01")
                    & (exp["Дата"] <= "2022-01-21")
                ]["Расход"].sum()
            ),
            round(
                exp[
                    (exp["Дата"] >= "2022-01-01")
                    & (exp["Дата"] <= "2022-01-21")
                ]["Расход"].sum()
            )
            * 1.2,
        ],
        "Расход за январь": round(
            exp[(exp["Дата"] >= "2022-01-01") & (exp["Дата"] <= "2022-01-31")][
                "Расход"
            ].sum()
        ),
    }
    # return render_template('target_audience.html', target_audience=get_target_audience())
    return render_template("expenses.html", exp=output_dict)


@app.route("/statuses", endpoint="statuses")
@decorators.auth
def statuses_page():
    with open(os.path.join(RESULTS_FOLDER, "statuses.pkl"), "rb") as f:
        statuses = pkl.load(f)
    # accounts = get_accounts()
    # trafficologists = get_trafficologists()
    # trafficologists2 = get_trafficologists()
    # print(trafficologists)
    return render_template(
        "statuses.html",
        tables=[statuses.to_html()],
    )  # ,
    # trafficologists=zip(trafficologists.id, trafficologists.name),
    # trafficologists2=zip(trafficologists2.id, trafficologists2.name),
    # trafficologist_error=trafficologist_error,
    # account_error=account_error)


from .auth import *
from .analytics import *
from .trafficologists import *
