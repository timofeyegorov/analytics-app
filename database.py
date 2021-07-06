from config import config
import pandas as pd
import pymysql

def connect():
    connection = pymysql.connections.Connection(**config['database'])
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    return connection, cursor

def get_all_data():
    conn, cursor = connect()
    query = "SELECT * FROM leads"
    cursor.execute(query)
    data = cursor.fetchall()
    conn.close()
    return pd.DataFrame(data)

def get_trafficologist_data():
    conn, cursor = connect()
    query = "SELECT label, title, name FROM trafficologists, accounts WHERE accounts.trafficologist_id=trafficologists.id"
    cursor.execute(query)
    data = cursor.fetchall()
    conn.close()
    account = {d['label']: d['title'] for d in data}
    trafficologist = {d['label']: d['name'] for d in data}
    return account, trafficologist

def get_accounts():
    conn, cursor = connect()
    query = "SELECT label, title, name FROM trafficologists, accounts WHERE accounts.trafficologist_id=trafficologists.id"
    cursor.execute(query)
    data = cursor.fetchall()
    conn.close()
    return pd.DataFrame(data)

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

def add_account(title, label, trafficologist_id):
    conn, cursor = connect()
    query = "INSERT INTO accounts (title, label, trafficologist_id) VALUES (%s, %s, %s)"
    cursor.execute(query, (title, label, trafficologist_id))
    conn.commit()
    conn.close()
    
def get_countries():
    conn, cursor = connect()
    query = "SELECT DISTINCT quiz_answers1 as country FROM leads"
    cursor.execute(query)
    data = cursor.fetchall()
    arr = [d['country'] for d in data]
    return arr

def get_ages():
    conn, cursor = connect()
    query = "SELECT DISTINCT quiz_answers2 as age FROM leads"
    cursor.execute(query)
    data = cursor.fetchall()
    arr = [d['age'] for d in data]
    return arr

def get_jobs():
    conn, cursor = connect()
    query = "SELECT DISTINCT quiz_answers3 as job FROM leads"
    cursor.execute(query)
    data = cursor.fetchall()
    arr = [d['job'] for d in data]
    return arr

def get_earnings():
    conn, cursor = connect()
    query = "SELECT DISTINCT quiz_answers4 as earning FROM leads"
    cursor.execute(query)
    data = cursor.fetchall()
    arr = [d['earning'] for d in data]
    return arr

def get_trainings():
    conn, cursor = connect()
    query = "SELECT DISTINCT quiz_answers5 as training FROM leads"
    cursor.execute(query)
    data = cursor.fetchall()
    arr = [d['training'] for d in data]
    return arr

def get_times():
    conn, cursor = connect()
    query = "SELECT DISTINCT quiz_answers6 as time FROM leads"
    cursor.execute(query)
    data = cursor.fetchall()
    arr = [d['time'] for d in data]
    return arr