from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

import pickle as pkl
from database import get_leads_data
from segments import get_segments
from clusters import get_clusters

def segments():
    data = get_leads_data()
    segments = get_segments(data)
    with open('/data/projects/analytic/python/analytics-app/dags/results/segments.pkl', 'wb') as f:
        pkl.dump(segments, f)
    return 'Success'

def clusters():
    data = get_leads_data()
    clusters = get_clusters(data)
    with open('/data/projects/analytic/python/analytics-app/dags/results/clusters.pkl', 'wb') as f:
        pkl.dump(clusters, f)
    return 'Success'

dag = DAG('calculate_cache', description='Calculates tables',
          schedule_interval='0 */4 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

segments_operator = PythonOperator(task_id='segments', python_callable=segments, dag=dag)
clusters_operator = PythonOperator(task_id='clusters', python_callable=clusters, dag=dag)

segments_operator