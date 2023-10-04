import datetime
import hashlib
from io import BytesIO
import requests
import pandas

from app.ats.api.config import hash_api, user_api, url_api


def parse_date(value: str) -> datetime.date:
    return datetime.date.fromisoformat(value)


def get_response(df: datetime.date, dt: datetime.date) -> pandas.DataFrame:
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
        'tree': '000-478278',
        'type': '2',
        'user': user_api,
        'hash': hash_api,
    }
    hash_string = '+'.join([hash_dict.get(key, '') for key in hash_dict])
    hash = hashlib.md5(hash_string.encode()).hexdigest()
    hash_dict.update({'hash': hash})
    response = requests.post(url_api, data=hash_dict)
    response_df: pandas.DataFrame = pandas.read_csv(BytesIO(response.content), delimiter=';', encoding='utf-8',
                                                    header=0)
    return response_df


def start_import(from_date, to_date):
    df = datetime.datetime.strptime(from_date, '%d-%m-%Y')
    dt = datetime.datetime.strptime(to_date, '%d-%m-%Y')
    selected_date = df
    tmp = pandas.DataFrame()
    while selected_date <= dt:
        try:
            df = get_response(selected_date, selected_date)
            tmp = pandas.concat([tmp, df], ignore_index=True)
            selected_date += datetime.timedelta(days=1)
        except Exception as e:
            selected_date += datetime.timedelta(days=1)
    tmp.to_csv('app/ats/api/data.csv', sep=';', encoding='utf-8')
    return 200
