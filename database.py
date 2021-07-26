from config import config
import pandas as pd
import pymysql
import pickle as pkl
from uuid import uuid4 as uuid

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
    query = "SELECT accounts.id, label, title, name FROM trafficologists, accounts WHERE accounts.trafficologist_id=trafficologists.id"
    cursor.execute(query)
    data = cursor.fetchall()
    conn.close()
    data = pd.DataFrame(data)
    # data.index = data.id
    # data.drop('id', axis=1, inplace=True)
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

def get_ta_filters(user_id):
    conn, cursor = connect()
    query = "SELECT id, title FROM target_audience_filters WHERE user_id=%s"
    cursor.execute(query, (user_id,))
    data = cursor.fetchall()
    conn.close()
    return data

def get_ta_filter(id):
    conn, cursor = connect()
    query = "SELECT id, title, pickle FROM target_audience_filters WHERE id=%s"
    cursor.execute(query, (id,))
    data = cursor.fetchone()
    conn.close()
    return data

def add_ta_filter(user_id, title, countries, ages, jobs, earnings, trainings, times):
    pickle = str(uuid()) + '.pkl'
    with open('filters/' + pickle, 'wb') as f:
        pkl.dump((countries, ages, jobs, earnings, trainings, times), f)
    conn, cursor = connect()
    query = "INSERT INTO target_audience_filters (user_id, title, pickle) VALUES (%s, %s, %s)"
    cursor.execute(query, (user_id, title, pickle))
    conn.commit()
    conn.close()

def edit_ta_filter(id, title, countries, ages, jobs, earnings, trainings, times):
    conn, cursor = connect()
    query = "SELECT * FROM target_audience_filters WHERE id=%s"
    cursor.execute(query, (id,))
    data = cursor.fetchone()
    query = "UPDATE target_audience_filters SET title=%s WHERE id=%s"
    cursor.execute(query, (title, id))
    conn.commit()
    conn.close()
    pickle = data['pickle']
    with open('filters/' + pickle, 'wb') as f:
        pkl.dump((countries, ages, jobs, earnings, trainings, times), f)

     
