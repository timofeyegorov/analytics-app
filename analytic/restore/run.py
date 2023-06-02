#!/usr/bin/env python

import json

from urllib import parse
from pathlib import Path
from datetime import datetime


DATERANGE = (
    datetime.fromisoformat("2022-07-08 00:22:23"),
    datetime.fromisoformat("2022-07-10 11:42:42"),
)

DATES = {}


def read_sources() -> dict:
    def _read_file(filepath: Path) -> dict:
        with open(filepath) as filepath_ref:
            _data = json.load(filepath_ref)
            return _data

    source_data = []
    for filepath in ["1.json", "2.json"]:
        for lead in _read_file(Path(__file__).parent / filepath).get("leads"):
            date = datetime.fromisoformat(lead.get("sys").get("tilda_created"))
            if DATERANGE[0] < date < DATERANGE[1]:
                last_date = DATES.get(lead.get("Phone"))
                is_double = "no"
                if last_date:
                    delta = date - last_date
                    is_double = (
                        "yes, more than 2 weeks"
                        if delta.days >= 14
                        else "yes, less than 2 weeks"
                    )
                sql = 'insert into leads (traffic_channel, quiz_answers1, quiz_answers2, quiz_answers3, quiz_answers4, quiz_answers5, quiz_answers6, status_amo, payment_amount, date_status_change, is_double, is_processed, email, phone, amo_marker, created_at, updated_at) values ("{traffic_channel}", "{quiz_answers1}", "{quiz_answers2}", "{quiz_answers3}", "{quiz_answers4}", "{quiz_answers5}", "{quiz_answers6}", "{status_amo}", "{payment_amount}", "{date_status_change}", "{is_double}", "{is_processed}", "{email}", "{phone}", "{amo_marker}", "{created_at}", "{updated_at}");'.format(
                    traffic_channel=lead.get("roistat_url"),
                    quiz_answers1=lead.get("country"),
                    quiz_answers2=lead.get("Сколько_вам_лет"),
                    quiz_answers3=lead.get("В_какой_сфере_сейчас_работаете"),
                    quiz_answers4=lead.get("Ваш_средний_доход_в_месяц"),
                    quiz_answers5=lead.get(
                        "Рассматриваете_ли_в_перспективе_платное_обучение_профессии_Разработчик_Искусственного_Интеллекта"
                    ),
                    quiz_answers6=lead.get(
                        "Сколько_времени_готовы_выделить_на_обучение_в_неделю"
                    ),
                    status_amo="Новая заявка (Теплые продажи)",
                    payment_amount="0.00",
                    date_status_change=lead.get("sys").get("tilda_created"),
                    is_double=is_double,
                    is_processed="no",
                    email=lead.get("Email"),
                    phone=lead.get("Phone"),
                    amo_marker=lead.get("sys").get("tilda_tranid"),
                    created_at=lead.get("sys").get("tilda_created"),
                    updated_at=lead.get("sys").get("tilda_created"),
                )
                source_data.append(sql)

    return source_data


if __name__ == "__main__":
    source_data = read_sources()
    with open(Path(__file__).parent / "insert.sql", "w") as sql_ref:
        sql_ref.write("\n".join(source_data))
