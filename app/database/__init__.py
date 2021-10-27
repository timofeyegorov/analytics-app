from config import config
import pandas as pd
import pymysql
import httplib2
import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials
from config import CREDENTIALS_FILE
from .preprocessing import preprocess_dataframe, preprocess_target_audience

def connect():
    connection = pymysql.connections.Connection(**config['database'])
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    return connection, cursor

def get_leads_data():
    conn, cursor = connect()
    query = "SELECT * FROM leads WHERE traffic_channel NOT IN ('https://neural-university.ru/terra_ai_education_new',\
                                                               'https://neural-university.ru/python_new',\
                                                               'https://neural-university.ru/python_analysis_2',\
                                                               'https://neural-university.ru/python_data_analysis')"
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

def add_trafficologist(name):
    conn, cursor = connect()
    query = "INSERT INTO trafficologists (name) VALUES (%s)"
    cursor.execute(query, (name,))
    conn.commit()
    conn.close()

def delete_trafficologist(id):
    conn, cursor = connect()
    query = "DELETE FROM trafficologists WHERE id=%s"
    cursor.execute(query, (id,))
    conn.commit()
    conn.close()

def add_account(title, label, trafficologist_id):
    conn, cursor = connect()
    query = "INSERT INTO accounts (title, label, trafficologist_id) VALUES (%s, %s, %s)"
    cursor.execute(query, (title, label, trafficologist_id))
    conn.commit()
    conn.close()

def delete_account(id):
    conn, cursor = connect()
    query = "DELETE FROM accounts WHERE id=%s"
    cursor.execute(query, (id,))
    conn.commit()
    conn.close()
    
def get_countries():
    conn, cursor = connect()
    query = "SELECT DISTINCT quiz_answers1 as country FROM leads"
    cursor.execute(query)
    data = cursor.fetchall()
    arr = [d['country'] for d in data]
    conn.close()
    return arr

def get_ages():
    conn, cursor = connect()
    query = "SELECT DISTINCT quiz_answers2 as age FROM leads"
    cursor.execute(query)
    data = cursor.fetchall()
    arr = [d['age'] for d in data]
    conn.close()
    return arr

def get_jobs():
    conn, cursor = connect()
    query = "SELECT DISTINCT quiz_answers3 as job FROM leads"
    cursor.execute(query)
    data = cursor.fetchall()
    arr = [d['job'] for d in data]
    conn.close()
    return arr

def get_earnings():
    conn, cursor = connect()
    query = "SELECT DISTINCT quiz_answers4 as earning FROM leads"
    cursor.execute(query)
    data = cursor.fetchall()
    arr = [d['earning'] for d in data]
    conn.close()
    return arr

def get_trainings():
    conn, cursor = connect()
    query = "SELECT DISTINCT quiz_answers5 as training FROM leads"
    cursor.execute(query)
    data = cursor.fetchall()
    arr = [d['training'] for d in data]
    conn.close()
    return arr

def get_times():
    conn, cursor = connect()
    query = "SELECT DISTINCT quiz_answers6 as time FROM leads"
    cursor.execute(query)
    data = cursor.fetchall()
    arr = [d['time'] for d in data]
    conn.close()
    return arr

# Подключаем библиотеки

def get_target_audience():
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

def get_status():
    try:
        return pd.read_csv('data/status.csv')
    except FileNotFoundError:
        return pd.read_csv('dags/data/status.csv')