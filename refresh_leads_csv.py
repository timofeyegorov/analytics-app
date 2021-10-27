'''
    Module for testing on local server.
    It allows to overwrite csv and pickle files in /dags/data/ path.
    leads.csv is filtered from incorrect leads.
    Pickle files are considered relative from leads.csv and depends from changes inside functions count reports.
'''

from app.database import *
from app.dags.clusters import calculate_clusters
from app.dags.segments import calculate_segments
from app.dags.landings import calculate_landings
from app.dags.turnover import calculate_turnover
from app.dags.segments_stats import calculate_segments_stats
from app.dags.leads_ta_stats import calculate_leads_ta_stats
from app.dags.traffic_sources import calculate_traffic_sources
import os
import pickle as pkl
from config import RESULTS_FOLDER

def load_data():
    data = get_leads_data()
    data.to_csv(os.path.join(RESULTS_FOLDER, 'leads.csv'), index=False)
    return None

def segments():
    data = pd.read_csv(os.path.join(RESULTS_FOLDER, 'leads.csv'))
    segments = calculate_segments(data)
    with open(os.path.join(RESULTS_FOLDER, 'segments.pkl'), 'wb') as f:
        pkl.dump(segments, f)
    return 'Success'

def clusters():
    data = pd.read_csv(os.path.join(RESULTS_FOLDER, 'leads.csv'))
    clusters = calculate_clusters(data)
    with open(os.path.join(RESULTS_FOLDER, 'clusters.pkl'), 'wb') as f:
        pkl.dump(clusters, f)
    return 'Success'

def landings():
    data = pd.read_csv(os.path.join(RESULTS_FOLDER, 'leads.csv'))
    landings = calculate_landings(data)
    with open(os.path.join(RESULTS_FOLDER, 'landings.pkl'), 'wb') as f:
        pkl.dump(landings, f)

def segments_stats():
    data = pd.read_csv(os.path.join(RESULTS_FOLDER, 'leads.csv'))
    segments_stats = calculate_segments_stats(data)
    with open(os.path.join(RESULTS_FOLDER, 'segments_stats.pkl'), 'wb') as f:
        pkl.dump(segments_stats, f)

def turnover():
    data = pd.read_csv(os.path.join(RESULTS_FOLDER, 'leads.csv'))
    turnover = calculate_turnover(data)
    with open(os.path.join(RESULTS_FOLDER, 'turnover.pkl'), 'wb') as f:
        pkl.dump(turnover, f)

def traffic_sources():
    data = pd.read_csv(os.path.join(RESULTS_FOLDER, 'leads.csv'))
    traffic_sources = calculate_traffic_sources(data)
    with open(os.path.join(RESULTS_FOLDER, 'traffic_sources.pkl'), 'wb') as f:
        pkl.dump(traffic_sources, f)

def leads_ta_stats():
    data = pd.read_csv(os.path.join(RESULTS_FOLDER, 'leads.csv'))
    leads_ta_stats = calculate_leads_ta_stats(data)
    with open(os.path.join(RESULTS_FOLDER, 'leads_ta_stats.pkl'), 'wb') as f:
        pkl.dump(leads_ta_stats, f)

if __name__=='__main__':
    # load_data()
    # segments()
    # turnover()
    # clusters()
    # landings()
    traffic_sources()
    segments_stats()
    leads_ta_stats()