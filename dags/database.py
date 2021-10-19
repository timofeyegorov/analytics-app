from config import config
import pickle as pkl
import pandas as pd
import pymysql
import pickle as pkl
from uuid import uuid4 as uuid
import httplib2
import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials
from preprocessing import preprocess_target_audience, preprocess_dataframe

def connect():
    connection = pymysql.connections.Connection(**config['database'])
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    return connection, cursor

def get_leads_data():
    conn, cursor = connect()
    query = "SELECT * FROM leads"
    cursor.execute(query)
    data = cursor.fetchall()
    conn.close()
    return preprocess_dataframe(pd.DataFrame(data))


def get_trafficologist_data():
    conn, cursor = connect()
    query = "SELECT label, title, name FROM trafficologists, accounts WHERE accounts.trafficologist_id=trafficologists.id LIMIT 1000"
    cursor.execute(query)
    data = cursor.fetchall()
    conn.close()
    account = {d['label']: d['title'] for d in data}
    trafficologist = {d['label']: d['name'] for d in data}
    return account, trafficologist

def get_accounts():
    conn, cursor = connect()
    query = "SELECT accounts.id, label, title, name FROM trafficologists, accounts WHERE accounts.trafficologist_id=trafficologists.id"
    cursor.execute(query)
    data = cursor.fetchall()
    conn.close()
    data = pd.DataFrame(data)
    return data

def get_trafficologists():
    conn, cursor = connect()
    query = "SELECT id, name FROM trafficologists"
    cursor.execute(query)
    data = cursor.fetchall()
    conn.close()
    return pd.DataFrame(data)

def get_target_audience():
    CREDENTIALS_FILE = '/data/projects/analytic/python/analytics-app/dags/analytics-322510-46607fe39c6c.json'  # Имя файла с закрытым ключом, вы должны подставить свое
    spreadsheet_id = '1_kytD9tww-2ETFOp46Av75PndibaoFzgHKV46fxHEWY'
    # Читаем ключи из файла
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_FILE,
        ['https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'])

    httpAuth = credentials.authorize(httplib2.Http()) # Авторизуемся в системе
    service = apiclient.discovery.build('sheets', 'v4', http = httpAuth) # Выбираем работу с таблицами и 4 версию API

    values = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range='A1:A996',
        majorDimension='COLUMNS'
    ).execute()

    return preprocess_target_audience(values['values'][0])
