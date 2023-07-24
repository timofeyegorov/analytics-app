from datetime import datetime
import requests
from api.config import create_config
import asyncio


# функция обращается по api и получает csv файл
async def get_data(data):
    api_url = 'https://sipuni.com/api/statistic/export'
    try:
        response = requests.post(api_url, data=data)
        if response.status_code == 200:
            with open(f"api/data.csv", 'wb') as file:
                file.write(response.content)
        else:
            with open('api/api.log', 'a', encoding='utf-8') as file:
                file.write(f'{datetime.now()} {response.status_code}\n')
    except Exception as e:
        with open('api/api.log', 'a', encoding='utf-8') as file:
            file.write(f'{datetime.now()} {str(e)}\n')


def start_import(from_date, to_date):
    data = create_config(from_date, to_date)
    asyncio.run(get_data(data))
