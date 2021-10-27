from logging import log
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import pickle as pkl
import json
import pandas as pd
import os
import sys
import redis
import datetime

sys.path.append(Variable.get('APP_FOLDER'))

from app.database import get_leads_data
from app.database.get_crops import get_crops
from app.database.get_target_audience import get_target_audience
from app.database.get_trafficologists import get_trafficologists
from app.database.get_expenses import get_trafficologists_expenses
from app.database.get_statuses import get_statuses
from app.database.preprocessing import calculate_trafficologists_expenses, calculate_crops_expenses

from app.tables import calculate_clusters, calculate_segments, calculate_landings, calculate_traffic_sources
from app.tables import calculate_turnover, calculate_leads_ta_stats, calculate_segments_stats

from config import RESULTS_FOLDER, config

redis_config = config['redis']
redis_db = redis.StrictRedis(
    host=redis_config['host'],
    port=redis_config['port'],
    charset='utf-8',
    db=redis_config['db'],
)

def log_execution_time(param_name):
    def decorator(func):
        def wrapper():
            try:
                func()
                last_updated = datetime.datetime.now().isoformat()
                redis_db.hmset(param_name, {'status': 'ok', 'last_updated': last_updated})
            except Exception as e:
                redis_db.hmset(param_name, {'status': 'fail', 'message': str(e)})
        return wrapper
    return decorator

@log_execution_time('load_crops')
def load_crops():
    crops, crops_list = get_crops()
    with open(os.path.join(RESULTS_FOLDER, 'crops.pkl'), 'wb') as f:
        pkl.dump(crops, f)
    with open(os.path.join(RESULTS_FOLDER, 'crops_list.pkl'), 'wb') as f:
        pkl.dump(crops_list, f)

@log_execution_time('load_trafficologists_expenses')
def load_trafficologists_expenses():
    expenses = get_trafficologists_expenses()
    with open(os.path.join(RESULTS_FOLDER, 'expenses.json'), 'w') as f:
        json.dump(expenses, f)

@log_execution_time('load_target_audience')
def load_target_audience():
    target_audience = get_target_audience()
    with open(os.path.join(RESULTS_FOLDER, 'target_audience.pkl'), 'wb') as f:
        pkl.dump(target_audience, f)
    
@log_execution_time('load_trafficologists')
def load_trafficologists():
    trafficologists = get_trafficologists()
    with open(os.path.join(RESULTS_FOLDER, 'trafficologists.pkl'), 'wb') as f:
        pkl.dump(trafficologists, f)

@log_execution_time('load_statuses')
def load_status():
    statuses = get_statuses()
    with open(os.path.join(RESULTS_FOLDER, 'statuses.pkl'), 'wb') as f:
        pkl.dump(statuses, f)

@log_execution_time('load_data')
def load_data():
    data = get_leads_data()
    with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'wb') as f:
        pkl.dump(data, f)
    return None

@log_execution_time('calculate_channel_expense')
def calculate_channel_expense():
    with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'rb') as f:
        leads = pkl.load(f)
    with open(os.path.join(RESULTS_FOLDER, 'crops.pkl'), 'rb') as f:
        crops = pkl.load(f)
    with open(os.path.join(RESULTS_FOLDER, 'trafficologists.pkl'), 'rb') as f:
        trafficologists = pkl.load(f)
    leads = calculate_crops_expenses(leads, crops)
    leads = calculate_trafficologists_expenses(leads, trafficologists)
    with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'wb') as f:
        pkl.dump(leads, f)
    

@log_execution_time('segments')
def segments():
    with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'rb') as f:
        data = pkl.load(f)
    segments = calculate_segments(data)
    with open(os.path.join(RESULTS_FOLDER, 'segments.pkl'), 'wb') as f:
        pkl.dump(segments, f)
    return 'Success'


@log_execution_time('clusters')
def clusters():
    with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'rb') as f:
        data = pkl.load(f)
    clusters = calculate_clusters(data)
    with open(os.path.join(RESULTS_FOLDER, 'clusters.pkl'), 'wb') as f:
        pkl.dump(clusters, f)
    return 'Success'


@log_execution_time('landings')
def landings():
    with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'rb') as f:
        data = pkl.load(f)
    landings = calculate_landings(data)
    with open(os.path.join(RESULTS_FOLDER, 'landings.pkl'), 'wb') as f:
        pkl.dump(landings, f)


@log_execution_time('segments_stats')
def segments_stats():
    with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'rb') as f:
        data = pkl.load(f)
    segments_stats = calculate_segments_stats(data)
    with open(os.path.join(RESULTS_FOLDER, 'segments_stats.pkl'), 'wb') as f:
        pkl.dump(segments_stats, f)


@log_execution_time('turnover')
def turnover():
    with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'rb') as f:
        data = pkl.load(f)
    turnover = calculate_turnover(data)
    with open(os.path.join(RESULTS_FOLDER, 'turnover.pkl'), 'wb') as f:
        pkl.dump(turnover, f)


@log_execution_time('traffic_sources')
def traffic_sources():
    with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'rb') as f:
        data = pkl.load(f)
    traffic_sources = calculate_turnover(data)
    with open(os.path.join(RESULTS_FOLDER, 'traffic_sources.pkl'), 'wb') as f:
        pkl.dump(traffic_sources, f)


@log_execution_time('lead_ta_stats')
def leads_ta_stats():
    with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'rb') as f:
        data = pkl.load(f)
    leads_ta_stats = calculate_turnover(data)
    with open(os.path.join(RESULTS_FOLDER, 'leads_ta_stats.pkl'), 'wb') as f:
        pkl.dump(leads_ta_stats, f)

dag = DAG('calculate_cache', description='Calculates tables',
          schedule_interval='0 */4 * * *',
          start_date=datetime.datetime(2017, 3, 20), catchup=False)

crops_operator = PythonOperator(task_id='load_crops', python_callable=load_crops, dag=dag)
trafficologists_operator = PythonOperator(task_id='load_trafficologists', python_callable=load_trafficologists, dag=dag)
target_audience_operator = PythonOperator(task_id='load_target_audience', python_callable=load_target_audience, dag=dag) 
expenses_operator = PythonOperator(task_id='load_expenenses', python_callable=load_trafficologists_expenses, dag=dag)

channel_expense_operator = PythonOperator(task_id='channel_expense', python_callable=calculate_channel_expense, dag=dag)

clean_data_operator = PythonOperator(task_id='clean_data', python_callable=load_data, dag=dag)
segments_operator = PythonOperator(task_id='segments', python_callable=segments, dag=dag)
clusters_operator = PythonOperator(task_id='clusters', python_callable=clusters, dag=dag)
# landings_operator = PythonOperator(task_id='landings', python_callable=landings, dag=dag)
segments_stats_operator = PythonOperator(task_id='segments_stats', python_callable=segments_stats, dag=dag)
turnover_operator = PythonOperator(task_id='turnover', python_callable=turnover, dag=dag)
leads_ta_stats = PythonOperator(task_id='leads_ta_stats', python_callable=leads_ta_stats, dag=dag)
traffic_sources = PythonOperator(task_id='traffic_sources', python_callable=traffic_sources, dag=dag)

crops_operator >> clean_data_operator
trafficologists_operator >> clean_data_operator
target_audience_operator >> clean_data_operator
expenses_operator >> clean_data_operator

clean_data_operator >> channel_expense_operator

channel_expense_operator >> segments_operator 
channel_expense_operator >> clusters_operator
# clean_data_operator >> landings_operator
channel_expense_operator >> segments_stats_operator
channel_expense_operator >> turnover_operator
channel_expense_operator >> leads_ta_stats
channel_expense_operator >> traffic_sources