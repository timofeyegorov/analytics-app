from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import pickle as pkl
import pandas as pd
import os
import sys
sys.path.append(Variable.get('APP_FOLDER'))

from app.database import get_leads_data
from app.dags.segments import calculate_segments
from app.dags.clusters import calculate_clusters
from app.dags.landings import calculate_landings
from app.dags.leads_ta_stats import calculate_leads_ta_stats
from app.dags.segments_stats import calculate_segments_stats
from app.dags.turnover import calculate_turnover
from app.dags.traffic_sources import calculate_traffic_sources
from config import RESULTS_FOLDER

def load_data():
    data = get_leads_data()
    data.to_csv(os.path.join(RESULTS_FOLDER, 'leads.csv'), index=False)
    return None

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

clean_data_operator = PythonOperator(task_id='clean_data', python_callable=load_data, dag=dag)
segments_operator = PythonOperator(task_id='segments', python_callable=segments, dag=dag)
clusters_operator = PythonOperator(task_id='clusters', python_callable=clusters, dag=dag)
landings_operator = PythonOperator(task_id='landings', python_callable=landings, dag=dag)
segments_stats_operator = PythonOperator(task_id='segments_stats', python_callable=segments_stats, dag=dag)
turnover_operator = PythonOperator(task_id='turnover', python_callable=turnover, dag=dag)
leads_ta_stats = PythonOperator(task_id='leads_ta_stats', python_callable=leads_ta_stats, dag=dag)
traffic_sources = PythonOperator(task_id='traffic_sources', python_callable=traffic_sources, dag=dag)

clean_data_operator >> segments_operator 
clean_data_operator >> clusters_operator
clean_data_operator >> landings_operator
clean_data_operator >> segments_stats_operator
clean_data_operator >> turnover_operator
clean_data_operator >> leads_ta_stats
clean_data_operator >> traffic_sources