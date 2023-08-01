import os
from datetime import datetime
import requests
from app.ats.api.config import create_config
import asyncio


# функция обращается по api и получает csv файл
async def get_data(data):
    api_url = 'https://sipuni.com/api/statistic/export'
    try:
        response = requests.post(api_url, data=data)
        if response.status_code == 200:
            if not os.path.exists('app/ats/api/data.csv'):
                # Создаем пустой файл, если его нет
                open('app/ats/api/data.csv', 'w').close()
            with open(f"app/ats/api/data.csv", 'wb') as file:
                file.write(response.content)
                return response.status_code
        else:
            with open('app/ats/api/api.log', 'a', encoding='utf-8') as file:
                file.write(f'{datetime.now()} {response.status_code}\n')
                return response.status_code
    except Exception as e:
        with open('app/ats/api/api.log', 'a', encoding='utf-8') as file:
            file.write(f'{datetime.now()} {str(e)}\n')


def start_import(from_date, to_date):
    data = create_config(from_date, to_date)
    result = asyncio.run(get_data(data))
    return result
