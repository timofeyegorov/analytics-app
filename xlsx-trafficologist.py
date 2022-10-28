#!/usr/bin/env python

import pytz
import argparse

from typing import List, Optional
from datetime import datetime
from xlsxwriter import Workbook

from app.analytics import pickle_loader


def run(date: List[Optional[datetime]], name: str):
    leads = pickle_loader.leads
    tz = pytz.timezone("Europe/Moscow")
    dates = list(leads.sort_values(["date"]).date.unique())
    if date[0] is None:
        date[0] = min(dates)
    if date[1] is None:
        date[1] = max(dates)
    try:
        date_from = tz.localize(date[0])
    except ValueError:
        date_from = date[0]
    try:
        date_to = tz.localize(date[1])
    except ValueError:
        date_to = date[1]
    leads = leads[
        (leads.date >= date_from)
        & (leads.date <= date_to)
        & (leads.trafficologist.str.lower() == name.lower())
    ].reset_index(drop=True)
    leads.date = leads.date.astype(str)
    workbook = Workbook(
        f'./leads_{date_from.strftime("%Y-%m-%d")}_{date_to.strftime("%Y-%m-%d")}_{name}.xlsx'
    )
    worksheet = workbook.add_worksheet("Leads")
    worksheet.write_row(0, 0, leads.columns)
    for index, row in leads.iterrows():
        worksheet.write_row(index + 1, 0, row.values)
    workbook.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser("python service-xlsx-trafficologist.py")
    parser.add_argument(
        "-df",
        "--date_from",
        type=str,
        help="С даты (формат %%Y-%%m-%%d)",
        required=False,
    )
    parser.add_argument(
        "-dt",
        "--date_to",
        type=str,
        help="По дату (формат %%Y-%%m-%%d)",
        required=False,
    )
    parser.add_argument("-n", "--name", type=str, help="Трафиколог", required=True)
    args = parser.parse_args()
    run(
        date=[
            datetime.strptime(args.date_from, "%Y-%m-%d") if args.date_from else None,
            datetime.strptime(args.date_to, "%Y-%m-%d") if args.date_to else None,
        ],
        name=args.name,
    )
