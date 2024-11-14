import datetime
import hashlib
from io import BytesIO
from typing import Tuple

import requests
import pandas
import logging

from app.ats.api.config import hash_api, user_api, url_api

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

REQUEST_RETRY = 1


def parse_date(value: str) -> datetime.date:
    return datetime.date.fromisoformat(value)


def get_response(df: datetime.date, dt: datetime.date) -> Tuple[pandas.DataFrame, int]:
    # https://doc.sipuni.com/articles/626-636-642--poluchenie-statistiki/
    hash_dict = {
        'anonymous': '1',
        'dtmfUserAnswer': '0',
        'firstTime': '0',
        'from': datetime.date.strftime(df, '%Y-%m-%d'),
        'fromNumber': '',
        'names': '1',
        'numbersInvolved': '1',
        'numbersRinged': '1',
        'outgoingLine': '1',
        'showTreeId': '0',
        'state': '0',
        'to': datetime.date.strftime(dt, '%Y-%m-%d'),
        'toAnswer': '',
        'toNumber': '',
        'tree': '',
        'type': '2',
        'user': user_api,
        'hash': hash_api,
    }
    hash_string = '+'.join([v for v in hash_dict.values()])
    hash = hashlib.md5(hash_string.encode()).hexdigest()
    hash_dict.update({'hash': hash})
    response = requests.post(url_api, data=hash_dict)
    if not response.ok:
        return pandas.DataFrame(), response.status_code

    response_df = pandas.read_csv(
        BytesIO(response.content), delimiter=';', encoding='utf-8', header=0,
    )
    return response_df, response.status_code


def start_import(from_date, to_date):
    date_from = datetime.datetime.strptime(from_date, '%d-%m-%Y')
    date_to = datetime.datetime.strptime(to_date, '%d-%m-%Y')
    error_message = None
    status_code = None
    dataframe = pandas.DataFrame()
    for _ in range(REQUEST_RETRY):
        try:
            dataframe, status_code = get_response(date_from, date_to)
            break
        except Exception as e:
            error_message = str(e)
            continue

    if error_message:
        logger.error(f'Error: {error_message}')

    if not dataframe.empty:
        dataframe.reset_index(drop=True, inplace=True)
        dataframe.to_csv('app/ats/api/data.csv', sep=';', encoding='utf-8')
    return status_code
