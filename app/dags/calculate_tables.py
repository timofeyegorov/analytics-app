from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
import pickle as pkl
import json
import os
import sys
import datetime

sys.path.append(Variable.get("APP_FOLDER"))

from app.database import get_leads_data
from app.database.get_crops import get_crops
from app.database.get_target_audience import get_target_audience
from app.database.get_trafficologists import get_trafficologists
from app.database.get_expenses import (
    get_trafficologists_expenses,
    get_trafficologists_expenses_levels,
)
from app.database.get_statuses import get_statuses
from app.database.get_ca_payment_analytic import get_ca_payment_analytic
from app.database.get_payments_table import get_payments_table
from app.database.preprocessing import (
    calculate_trafficologists_expenses,
    calculate_crops_expenses,
)
from app.database.preprocessing import get_turnover_on_lead, get_marginality
from app.tables import (
    calculate_clusters,
    calculate_segments,
    calculate_landings,
    calculate_traffic_sources,
)
from app.tables import (
    calculate_turnover,
    calculate_leads_ta_stats,
    calculate_segments_stats,
)
from app.tables import (
    calculate_channels_summary,
    calculate_channels_detailed,
    calculate_payments_accumulation,
)
from app.tables import calculate_marginality
from app.tables import calculate_audience_tables_by_date
from app.tables import calculate_audience_type_result
from app.tables import calculate_audience_type_percent_result
from config import RESULTS_FOLDER

from .decorators import log_execution_time


@log_execution_time("load_crops")
def load_crops():
    crops, crops_list = get_crops()
    with open(os.path.join(RESULTS_FOLDER, "crops.pkl"), "wb") as f:
        pkl.dump(crops, f)
    with open(os.path.join(RESULTS_FOLDER, "crops_list.pkl"), "wb") as f:
        pkl.dump(crops_list, f)


@log_execution_time("load_trafficologists_expenses")
def load_trafficologists_expenses():
    expenses = get_trafficologists_expenses()
    with open(os.path.join(RESULTS_FOLDER, "expenses.json"), "w") as f:
        json.dump(expenses, f)


@log_execution_time("load_trafficologists_expenses_levels")
def load_trafficologists_expenses_levels():
    expenses = get_trafficologists_expenses_levels()
    with open(os.path.join(RESULTS_FOLDER, "expenses_levels.json"), "w") as f:
        json.dump(expenses, f)


@log_execution_time("load_target_audience")
def load_target_audience():
    target_audience = get_target_audience()
    with open(os.path.join(RESULTS_FOLDER, "target_audience.pkl"), "wb") as f:
        pkl.dump(target_audience, f)


@log_execution_time("load_trafficologists")
def load_trafficologists():
    trafficologists = get_trafficologists()
    with open(os.path.join(RESULTS_FOLDER, "trafficologists.pkl"), "wb") as f:
        pkl.dump(trafficologists, f)


@log_execution_time("load_statuses")
def load_status():
    statuses = get_statuses()
    with open(os.path.join(RESULTS_FOLDER, "statuses.pkl"), "wb") as f:
        pkl.dump(statuses, f)


@log_execution_time("load_ca_payment_analytic")
def load_ca_payment_analytic():
    ca_payment_analytic = get_ca_payment_analytic()
    with open(os.path.join(RESULTS_FOLDER, "ca_payment_analytic.pkl"), "wb") as f:
        pkl.dump(ca_payment_analytic, f)


@log_execution_time("load_payments_table")
def load_payments_table():
    payments_table = get_payments_table()
    with open(os.path.join(RESULTS_FOLDER, "payments_table.pkl"), "wb") as f:
        pkl.dump(payments_table, f)


@log_execution_time("load_data")
def load_data():
    data = get_leads_data()
    with open(os.path.join(RESULTS_FOLDER, "ca_payment_analytic.pkl"), "rb") as f:
        ca_payment_analytic = pkl.load(f)
    with open(os.path.join(RESULTS_FOLDER, "crops.pkl"), "rb") as f:
        crops = pkl.load(f)
    with open(os.path.join(RESULTS_FOLDER, "trafficologists.pkl"), "rb") as f:
        trafficologists = pkl.load(f)

    leads = get_turnover_on_lead(data, ca_payment_analytic)
    leads = get_marginality(leads)
    leads = calculate_crops_expenses(leads, crops)
    leads = calculate_trafficologists_expenses(leads, trafficologists)
    with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "wb") as f:
        pkl.dump(leads, f)


# @log_execution_time('calculate_channel_expense')
# def calculate_channel_expense():
#     with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'rb') as f:
#         leads = pkl.load(f)
#     with open(os.path.join(RESULTS_FOLDER, 'crops.pkl'), 'rb') as f:
#         crops = pkl.load(f)
#     with open(os.path.join(RESULTS_FOLDER, 'trafficologists.pkl'), 'rb') as f:
#         trafficologists = pkl.load(f)
#     leads = calculate_crops_expenses(leads, crops)
#     leads = calculate_trafficologists_expenses(leads, trafficologists)
#     with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'wb') as f:
#         pkl.dump(leads, f)

# @log_execution_time('calculate_turnover_on_lead')
# def calculate_turnover_on_lead():
#     with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'rb') as f:
#         leads = pkl.load(f)
#     with open(os.path.join(RESULTS_FOLDER, 'ca_payment_analytic.pkl'), 'rb') as f:
#         ca_payment_analytic = pkl.load(f)
#     leads = get_turnover_on_lead(leads, ca_payment_analytic)
#     leads = get_marginality(leads)
#     with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'wb') as f:
#         pkl.dump(leads, f)


@log_execution_time("marginality")
def marginality():
    with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
        data = pkl.load(f)
    marginality = calculate_marginality(data)
    with open(os.path.join(RESULTS_FOLDER, "marginality.pkl"), "wb") as f:
        pkl.dump(marginality, f)
    return "Success"


@log_execution_time("channels_summary")
def channels_summary():
    with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
        data = pkl.load(f)
    channels_summary = calculate_channels_summary(data)
    with open(os.path.join(RESULTS_FOLDER, "channels_summary.pkl"), "wb") as f:
        pkl.dump(channels_summary, f)
    return "Success"


@log_execution_time("channels_detailed")
def channels_detailed():
    with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
        data = pkl.load(f)
    channels_detailed = calculate_channels_detailed(data)
    with open(os.path.join(RESULTS_FOLDER, "channels_detailed.pkl"), "wb") as f:
        pkl.dump(channels_detailed, f)
    return "Success"


@log_execution_time("payments_accumulation")
def payments_accumulation():
    with open(os.path.join(RESULTS_FOLDER, "payments_table.pkl"), "rb") as f:
        data = pkl.load(f)
    payments_accumulation = calculate_payments_accumulation(data)
    with open(os.path.join(RESULTS_FOLDER, "payments_accumulation.pkl"), "wb") as f:
        pkl.dump(payments_accumulation, f)
    return "Success"


@log_execution_time("audience_type_by_date")
def audience_type_by_date():
    with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
        data = pkl.load(f)
    audience_type_by_date = calculate_audience_tables_by_date(data)
    with open(os.path.join(RESULTS_FOLDER, "audience_type_by_date.pkl"), "wb") as f:
        pkl.dump(audience_type_by_date, f)
    return "Success"


@log_execution_time("audience_type")
def audience_type():
    with open(os.path.join(RESULTS_FOLDER, "audience_type_by_date.pkl"), "rb") as f:
        data = pkl.load(f)
    audience_type = calculate_audience_type_result(data)
    with open(os.path.join(RESULTS_FOLDER, "audience_type.pkl"), "wb") as f:
        pkl.dump(audience_type, f)
    return "Success"


@log_execution_time("audience_type_percent")
def audience_type_percent():
    with open(os.path.join(RESULTS_FOLDER, "audience_type_by_date.pkl"), "rb") as f:
        data = pkl.load(f)
    audience_type_percent = calculate_audience_type_percent_result(data)
    with open(os.path.join(RESULTS_FOLDER, "audience_type_percent.pkl"), "wb") as f:
        pkl.dump(audience_type_percent, f)
    return "Success"


@log_execution_time("segments")
def segments():
    with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
        data = pkl.load(f)
    segments = calculate_segments(data)
    with open(os.path.join(RESULTS_FOLDER, "segments.pkl"), "wb") as f:
        pkl.dump(segments, f)
    return "Success"


@log_execution_time("clusters")
def clusters():
    with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
        data = pkl.load(f)
    clusters = calculate_clusters(data)
    with open(os.path.join(RESULTS_FOLDER, "clusters.pkl"), "wb") as f:
        pkl.dump(clusters, f)
    return "Success"


@log_execution_time("landings")
def landings():
    with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
        data = pkl.load(f)
    landings = calculate_landings(data)
    with open(os.path.join(RESULTS_FOLDER, "landings.pkl"), "wb") as f:
        pkl.dump(landings, f)


@log_execution_time("segments_stats")
def segments_stats():
    with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
        data = pkl.load(f)
    segments_stats = calculate_segments_stats(data)
    with open(os.path.join(RESULTS_FOLDER, "segments_stats.pkl"), "wb") as f:
        pkl.dump(segments_stats, f)


@log_execution_time("turnover")
def turnover():
    with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
        data = pkl.load(f)
    turnover = calculate_turnover(data)
    with open(os.path.join(RESULTS_FOLDER, "turnover.pkl"), "wb") as f:
        pkl.dump(turnover, f)


@log_execution_time("traffic_sources")
def traffic_sources():
    with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
        data = pkl.load(f)
    traffic_sources = calculate_traffic_sources(data)
    with open(os.path.join(RESULTS_FOLDER, "traffic_sources.pkl"), "wb") as f:
        pkl.dump(traffic_sources, f)


@log_execution_time("leads_ta_stats")
def leads_ta_stats():
    with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
        data = pkl.load(f)
    leads_ta_stats = calculate_leads_ta_stats(data)
    with open(os.path.join(RESULTS_FOLDER, "leads_ta_stats.pkl"), "wb") as f:
        pkl.dump(leads_ta_stats, f)


dag = DAG(
    "calculate_cache",
    description="Calculates tables",
    schedule_interval="0 */4 * * *",
    start_date=datetime.datetime(2017, 3, 20),
    catchup=False,
)

crops_operator = PythonOperator(
    task_id="load_crops", python_callable=load_crops, dag=dag
)
trafficologists_operator = PythonOperator(
    task_id="load_trafficologists", python_callable=load_trafficologists, dag=dag
)
target_audience_operator = PythonOperator(
    task_id="load_target_audience", python_callable=load_target_audience, dag=dag
)
expenses_operator = PythonOperator(
    task_id="load_trafficologists_expenses",
    python_callable=load_trafficologists_expenses,
    dag=dag,
)
expenses_levels_operator = PythonOperator(
    task_id="load_trafficologists_expenses_levels",
    python_callable=load_trafficologists_expenses_levels,
    dag=dag,
)
statuses_operator = PythonOperator(
    task_id="load_statuses", python_callable=load_status, dag=dag
)
ca_payment_analytic_operator = PythonOperator(
    task_id="load_ca_payment_analytic",
    python_callable=load_ca_payment_analytic,
    dag=dag,
)
payments_table_operator = PythonOperator(
    task_id="load_payments_table", python_callable=load_payments_table, dag=dag
)

payments_accumulation_operator = PythonOperator(
    task_id="payments_accumulation", python_callable=payments_accumulation, dag=dag
)

# channel_expense_operator = PythonOperator(task_id='calculate_channel_expense', python_callable=calculate_channel_expense, dag=dag)
# turnover_on_lead_operator = PythonOperator(task_id='calculate_turnover_on_lead', python_callable=calculate_turnover_on_lead, dag=dag)

clean_data_operator = PythonOperator(
    task_id="load_data", python_callable=load_data, dag=dag
)
channels_summary_operator = PythonOperator(
    task_id="channels_summary", python_callable=channels_summary, dag=dag
)
channels_detailed_operator = PythonOperator(
    task_id="channels_detailed", python_callable=channels_detailed, dag=dag
)
marginality_operator = PythonOperator(
    task_id="marginality", python_callable=marginality, dag=dag
)

audience_type_by_date_operator = PythonOperator(
    task_id="audience_type_by_date", python_callable=audience_type_by_date, dag=dag
)
audience_type_operator = PythonOperator(
    task_id="audience_type", python_callable=audience_type, dag=dag
)
audience_type_percent_operator = PythonOperator(
    task_id="audience_type_percent", python_callable=audience_type_percent, dag=dag
)

segments_operator = PythonOperator(
    task_id="segments", python_callable=segments, dag=dag
)
clusters_operator = PythonOperator(
    task_id="clusters", python_callable=clusters, dag=dag
)
landings_operator = PythonOperator(
    task_id="landings", python_callable=landings, dag=dag
)
segments_stats_operator = PythonOperator(
    task_id="segments_stats", python_callable=segments_stats, dag=dag
)
turnover_operator = PythonOperator(
    task_id="turnover", python_callable=turnover, dag=dag
)
leads_ta_stats = PythonOperator(
    task_id="leads_ta_stats", python_callable=leads_ta_stats, dag=dag
)
traffic_sources = PythonOperator(
    task_id="traffic_sources", python_callable=traffic_sources, dag=dag
)

crops_operator >> clean_data_operator
trafficologists_operator >> clean_data_operator
target_audience_operator >> clean_data_operator
expenses_operator >> clean_data_operator
expenses_levels_operator >> clean_data_operator
statuses_operator >> clean_data_operator
ca_payment_analytic_operator >> clean_data_operator
payments_table_operator >> clean_data_operator

# clean_data_operator >> channel_expense_operator
#  clean_data_operator >> turnover_on_lead_operator

clean_data_operator >> audience_type_by_date_operator
clean_data_operator >> audience_type_operator
clean_data_operator >> audience_type_percent_operator

clean_data_operator >> payments_accumulation_operator
clean_data_operator >> channels_summary_operator
clean_data_operator >> channels_detailed_operator
clean_data_operator >> marginality_operator
# turnover_on_lead_operator >> payments_accumulation_operator
# turnover_on_lead_operator >> channels_summary_operator
# turnover_on_lead_operator >> channels_detailed_operator
# turnover_on_lead_operator >> marginality_operator

clean_data_operator >> segments_operator
clean_data_operator >> clusters_operator
clean_data_operator >> landings_operator
clean_data_operator >> segments_stats_operator
clean_data_operator >> turnover_operator
clean_data_operator >> leads_ta_stats
clean_data_operator >> traffic_sources
# channel_expense_operator >> segments_operator
# channel_expense_operator >> clusters_operator
# channel_expense_operator >> landings_operator
# channel_expense_operator >> segments_stats_operator
# channel_expense_operator >> turnover_operator
# channel_expense_operator >> leads_ta_stats
# channel_expense_operator >> traffic_sources
