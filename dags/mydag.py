from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

import pickle as pkl
import pandas as pd
from database import get_leads_data
from segments import get_segments
from clusters import get_clusters
from landings import get_landings
from leads_ta_stats import get_leads_ta_stats
from segments_stats import get_segments_stats
from turnover import get_turnover
from traffic_sources import get_traffic_sources


def load_data():
    data = get_leads_data()
    data.to_csv('/data/projects/analytic/python/analytics-app/dags/results/leads.csv', idnex=False)
    return None

def segments():
    data = pd.read_csv('/data/projects/analytic/python/analytics-app/dags/results/leads.csv')
    segments = get_segments(data)
    with open('/data/projects/analytic/python/analytics-app/dags/results/segments.pkl', 'wb') as f:
        pkl.dump(segments, f)
    return 'Success'

def clusters():
    data = pd.read_csv('/data/projects/analytic/python/analytics-app/dags/results/leads.csv')
    clusters = get_clusters(data)
    with open('/data/projects/analytic/python/analytics-app/dags/results/clusters.pkl', 'wb') as f:
        pkl.dump(clusters, f)
    return 'Success'

def landings():
    data = pd.read_csv('/data/projects/analytic/python/analytics-app/dags/results/leads.csv')
    landings = get_landings(data)
    with open('/data/projects/analytic/python/analytics-app/dags/results/landings.pkl', 'wb') as f:
        pkl.dump(landings, f)

def segments_stats():
    data = pd.read_csv('/data/projects/analytic/python/analytics-app/dags/results/leads.csv')
    segments_stats = get_segments_stats(data)
    with open('/data/projects/analytic/python/analytics-app/dags/results/segments_stats.pkl', 'wb') as f:
        pkl.dump(segments_stats, f)

def turnover():
    data = pd.read_csv('/data/projects/analytic/python/analytics-app/dags/results/leads.csv')
    turnover = get_turnover(data)
    with open('/data/projects/analytic/python/analytics-app/dags/results/turnover.pkl', 'wb') as f:
        pkl.dump(turnover, f)

def traffic_sources():
    data = pd.read_csv('/data/projects/analytic/python/analytics-app/dags/results/leads.csv')
    traffic_sources = get_turnover(data)
    with open('/data/projects/analytic/python/analytics-app/dags/results/traffic_sources.pkl', 'wb') as f:
        pkl.dump(traffic_sources, f)

def leads_ta_stats():
    data = pd.read_csv('/data/projects/analytic/python/analytics-app/dags/results/leads.csv')
    leads_ta_stats = get_turnover(data)
    with open('/data/projects/analytic/python/analytics-app/dags/results/leads_ta_stats.pkl', 'wb') as f:
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