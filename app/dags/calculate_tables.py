from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import pickle as pkl
import json
import pandas as pd
import os
import sys

sys.path.append(Variable.get('APP_FOLDER'))

from app.database import get_leads_data
from app.database.get_crops import get_crops
from app.database.get_target_audience import get_target_audience
from app.database.get_trafficologists import get_trafficologists
from app.database.get_expenses import get_trafficologists_expenses
from app.database.preprocessing import calculate_trafficologists_expenses, calculate_crops_expenses

from app.tables import calculate_clusters, calculate_segments, calculate_landings, calculate_traffic_sources
from app.tables import calculate_turnover, calculate_leads_ta_stats, calculate_segments_stats

from config import RESULTS_FOLDER

def load_crops():
    crops = get_crops()
    crops.to_csv(os.path.join(RESULTS_FOLDER, 'crops.csv'), index=False)

def load_trafficologists_expenses():
    expenses = get_trafficologists_expenses()
    with open(os.path.join(RESULTS_FOLDER, 'expenses.json'), 'w') as f:
        json.dump(expenses, f)

def load_target_audience():
    target_audience = get_target_audience()
    with open(os.path.join(RESULTS_FOLDER, 'target_audience.pkl'), 'wb') as f:
        pkl.dump(target_audience, f)
    
def load_trafficologists():
    trafficologists = get_trafficologists()
    trafficologists.to_csv(os.path.join(RESULTS_FOLDER, 'trafficologists.csv'), index=False)

def load_data():
    data = get_leads_data()
    data.to_csv(os.path.join(RESULTS_FOLDER, 'leads.csv'), index=False)
    return None

def calculate_channel_expense():
    leads = pd.read_csv(os.path.join(RESULTS_FOLDER, 'leads.csv'))
    crops = pd.read_csv(os.path.join(RESULTS_FOLDER, 'crops.csv'))
    trafficologists = pd.read_csv(os.path.join(RESULTS_FOLDER, 'trafficologists.csv'))
    leads = calculate_crops_expenses(leads, crops)
    leads = calculate_trafficologists_expenses(leads, trafficologists)
    leads.to_csv(os.path.join(RESULTS_FOLDER, 'leads.csv'))
    

def segments():
    data = pd.read_csv(os.path.join(RESULTS_FOLDER, 'leads.csv'))
    segments = calculate_segments(data)
    with open(os.path.join(RESULTS_FOLDER, 'segments.csv'), 'wb') as f:
        pkl.dump(segments, f)
    return 'Success'

def clusters():
    data = pd.read_csv(os.path.join(RESULTS_FOLDER, 'leads.csv'))
    clusters = calculate_clusters(data)
    with open(os.path.join(RESULTS_FOLDER, 'clusters.csv'), 'wb') as f:
        pkl.dump(clusters, f)
    return 'Success'

def landings():
    data = pd.read_csv(os.path.join(RESULTS_FOLDER, 'leads.csv'))
    landings = calculate_landings(data)
    with open(os.path.join(RESULTS_FOLDER, 'landings.csv'), 'wb') as f:
        pkl.dump(landings, f)

def segments_stats():
    data = pd.read_csv(os.path.join(RESULTS_FOLDER, 'leads.csv'))
    segments_stats = calculate_segments_stats(data)
    with open(os.path.join(RESULTS_FOLDER, 'segments_stats.csv'), 'wb') as f:
        pkl.dump(segments_stats, f)

def turnover():
    data = pd.read_csv(os.path.join(RESULTS_FOLDER, 'leads.csv'))
    turnover = calculate_turnover(data)
    with open(os.path.join(RESULTS_FOLDER, 'turnover.csv'), 'wb') as f:
        pkl.dump(turnover, f)

def traffic_sources():
    data = pd.read_csv(os.path.join(RESULTS_FOLDER, 'leads.csv'))
    traffic_sources = calculate_turnover(data)
    with open(os.path.join(RESULTS_FOLDER, 'traffic_sources.csv'), 'wb') as f:
        pkl.dump(traffic_sources, f)

def leads_ta_stats():
    data = pd.read_csv(os.path.join(RESULTS_FOLDER, 'leads.csv'))
    leads_ta_stats = calculate_turnover(data)
    with open(os.path.join(RESULTS_FOLDER, 'leads_ta_stats.csv'), 'wb') as f:
        pkl.dump(leads_ta_stats, f)

dag = DAG('calculate_cache', description='Calculates tables',
          schedule_interval='0 */4 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

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