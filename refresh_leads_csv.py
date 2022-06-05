'''
    Module for testing on local server.
    It allows to overwrite csv and pickle files in /dags/data/ path.
    leads.csv is filtered from incorrect leads.
    Pickle files are considered relative from leads.csv and depends from changes inside functions count reports.
'''

from app.database import *
from app.database.get_crops import get_crops
from app.database.get_expenses import get_trafficologists_expenses
from app.database.get_statuses import get_statuses
from app.database.get_target_audience import get_target_audience
from app.database.get_trafficologists import get_trafficologists
from app.database.get_ca_payment_analytic import get_ca_payment_analytic
from app.database.get_payments_table import get_payments_table

from app.tables import calculate_clusters
from app.tables import calculate_segments
from app.tables import calculate_landings
from app.tables import calculate_turnover
from app.tables import calculate_segments_stats
from app.tables import calculate_leads_ta_stats
from app.tables import calculate_traffic_sources
from app.tables import calculate_channels_summary
from app.tables import calculate_channels_detailed
from app.tables import calculate_payments_accumulation, calculate_marginality
from app.tables import calculate_audience_tables_by_date, calculate_audience_type_result, calculate_audience_type_percent_result

from app.database.preprocessing import get_turnover_on_lead, get_marginality
from app.database.preprocessing import calculate_trafficologists_expenses, calculate_crops_expenses
import os
import pickle as pkl
from config import RESULTS_FOLDER
import json

def load_crops():
    crops, crops_list = get_crops()
    with open(os.path.join(RESULTS_FOLDER, 'crops.pkl'), 'wb') as f:
        pkl.dump(crops, f)
    with open(os.path.join(RESULTS_FOLDER, 'crops_list.pkl'), 'wb') as f:
        pkl.dump(crops_list, f)

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
    with open(os.path.join(RESULTS_FOLDER, 'trafficologists.pkl'), 'wb') as f:
        pkl.dump(trafficologists, f)

def load_status():
    statuses = get_statuses()
    with open(os.path.join(RESULTS_FOLDER, 'statuses.pkl'), 'wb') as f:
        pkl.dump(statuses, f)

def load_data():
    data = get_leads_data()
    with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'wb') as f:
        pkl.dump(data, f)
    return 'Success'

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

def load_ca_payment_analytic():
    out_df = get_ca_payment_analytic()
    with open(os.path.join(RESULTS_FOLDER, 'ca_payment_analytic.pkl'), 'wb') as f:
        pkl.dump(out_df, f)

def load_payments_table():
    payments_table = get_payments_table()
    with open(os.path.join(RESULTS_FOLDER, 'payments_table.pkl'), 'wb') as f:
        pkl.dump(payments_table, f)

def calculate_turnover_on_lead():
    with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'rb') as f:
        leads = pkl.load(f)
    with open(os.path.join(RESULTS_FOLDER, 'ca_payment_analytic.pkl'), 'rb') as f:
        ca_payment_analytic = pkl.load(f)
    leads = get_turnover_on_lead(leads, ca_payment_analytic)
    leads = get_marginality(leads)
    with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'wb') as f:
        pkl.dump(leads, f)

def channels_summary():
    with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'rb') as f:
        data = pkl.load(f)
    channels_summary = calculate_channels_summary(data)
    with open(os.path.join(RESULTS_FOLDER, 'channels_summary.pkl'), 'wb') as f:
        pkl.dump(channels_summary, f)
    return 'Success'

def channels_detailed():
    with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'rb') as f:
        data = pkl.load(f)
    channels_detailed = calculate_channels_detailed(data)
    with open(os.path.join(RESULTS_FOLDER, 'channels_detailed.pkl'), 'wb') as f:
        pkl.dump(channels_detailed, f)
    return 'Success'

def payments_accumulation():
    with open(os.path.join(RESULTS_FOLDER, 'payments_table.pkl'), 'rb') as f:
        data = pkl.load(f)
    payments_accumulation = calculate_payments_accumulation(data)
    with open(os.path.join(RESULTS_FOLDER, 'payments_accumulation.pkl'), 'wb') as f:
        pkl.dump(payments_accumulation, f)
    return 'Success'

def marginality():
    with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'rb') as f:
        data = pkl.load(f)
    marginality = calculate_marginality(data)
    with open(os.path.join(RESULTS_FOLDER, 'marginality.pkl'), 'wb') as f:
        pkl.dump(marginality, f)
    return 'Success'

def audience_type_by_date():
    with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'rb') as f:
        data = pkl.load(f)
    audience_type_by_date = calculate_audience_tables_by_date(data)
    with open(os.path.join(RESULTS_FOLDER, 'audience_type_by_date.pkl'), 'wb') as f:
        pkl.dump(audience_type_by_date, f)
    return 'Success'

def audience_type():
    with open(os.path.join(RESULTS_FOLDER, 'audience_type_by_date.pkl'), 'rb') as f:
        data = pkl.load(f)
    audience_type = calculate_audience_type_result(data)
    with open(os.path.join(RESULTS_FOLDER, 'audience_type.pkl'), 'wb') as f:
        pkl.dump(audience_type, f)
    return 'Success'

def audience_type_percent():
    with open(os.path.join(RESULTS_FOLDER, 'audience_type_by_date.pkl'), 'rb') as f:
        data = pkl.load(f)
    audience_type_percent = calculate_audience_type_percent_result(data)
    with open(os.path.join(RESULTS_FOLDER, 'audience_type_percent.pkl'), 'wb') as f:
        pkl.dump(audience_type_percent, f)
    return 'Success'

def segments():
    with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'rb') as f:
        data = pkl.load(f)
    segments = calculate_segments(data)
    with open(os.path.join(RESULTS_FOLDER, 'segments.pkl'), 'wb') as f:
        pkl.dump(segments, f)
    return 'Success'

def clusters():
    with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'rb') as f:
        data = pkl.load(f)
    clusters = calculate_clusters(data)
    with open(os.path.join(RESULTS_FOLDER, 'clusters.pkl'), 'wb') as f:
        pkl.dump(clusters, f)
    return 'Success'

def landings():
    with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'rb') as f:
        data = pkl.load(f)
    landings = calculate_landings(data)
    with open(os.path.join(RESULTS_FOLDER, 'landings.pkl'), 'wb') as f:
        pkl.dump(landings, f)
    return 'Success'

def segments_stats():
    with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'rb') as f:
        data = pkl.load(f)
    segments_stats = calculate_segments_stats(data)
    with open(os.path.join(RESULTS_FOLDER, 'segments_stats.pkl'), 'wb') as f:
        pkl.dump(segments_stats, f)
    return 'Success'

def turnover():
    with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'rb') as f:
        data = pkl.load(f)
    turnover = calculate_turnover(data)
    with open(os.path.join(RESULTS_FOLDER, 'turnover.pkl'), 'wb') as f:
        pkl.dump(turnover, f)
    return 'Success'

def traffic_sources():
    with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'rb') as f:
        data = pkl.load(f)
    traffic_sources = calculate_traffic_sources(data)
    with open(os.path.join(RESULTS_FOLDER, 'traffic_sources.pkl'), 'wb') as f:
        pkl.dump(traffic_sources, f)
    return 'Success'

def leads_ta_stats():
    with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'rb') as f:
        data = pkl.load(f)
    leads_ta_stats = calculate_leads_ta_stats(data)
    with open(os.path.join(RESULTS_FOLDER, 'leads_ta_stats.pkl'), 'wb') as f:
        pkl.dump(leads_ta_stats, f)
    return 'Success'

if __name__=='__main__':
    print('load data ...')
    load_crops()
    load_trafficologists_expenses()
    load_target_audience()
    load_trafficologists()
    load_status()
    load_data()
    load_ca_payment_analytic()
    load_payments_table()
    print('loading have finished correctly!')

    print('calculate_channel_expense ...')
    calculate_channel_expense()
    print('OK!')

    print('counting calculate_turnover_on_lead ...')
    calculate_turnover_on_lead()
    print('OK!')
    print('counting channels_summary ...')
    channels_summary()
    print('OK!')
    print('counting channels_detailed ...')
    channels_detailed()
    print('OK!')
    print('counting payments_accumulation ...')
    payments_accumulation()
    print('OK!')
    print('counting marginality ...')
    marginality()
    print('OK!')
    print('counting audience_type ...')
    audience_type_by_date()
    audience_type()
    audience_type_percent()
    print('OK!')

    print('other reports ...')
    segments()
    turnover()
    clusters()
    landings()
    traffic_sources()
    segments_stats()
    leads_ta_stats()
    print('OK!')
