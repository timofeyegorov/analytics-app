'''
    Module for testing on local server.
    It allows to overwrite csv and pickle files in /dags/data/ path.
    leads.csv is filtered from incorrect leads.
    Pickle files are considered relative from leads.csv and depends from changes inside functions count reports.
'''

from app.database import *
from app.tables import calculate_clusters
from app.tables import calculate_segments
from app.tables import calculate_landings
from app.tables import calculate_turnover
from app.tables import calculate_segments_stats
from app.tables import calculate_leads_ta_stats
from app.tables import calculate_traffic_sources
import os
import pickle as pkl
from config import RESULTS_FOLDER

def load_data():
    data = get_leads_data()
    # data.to_csv(os.path.join(RESULTS_FOLDER, 'leads.csv'), index=False)
    with open(os.path.join(RESULTS_FOLDER, 'leads.pkl'), 'wb') as f:
        pkl.dump(data, f)
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
    load_data()
    segments()
    turnover()
    clusters()
    landings()
    traffic_sources()
    segments_stats()
    leads_ta_stats()
    pass