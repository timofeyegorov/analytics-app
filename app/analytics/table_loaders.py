import os
import pickle as pkl
from config import RESULTS_FOLDER

def get_channels_summary():
    with open(os.path.join(RESULTS_FOLDER, 'channels_summary.pkl'), 'rb') as f:
        data = pkl.load(f)
    return data

def get_segments():
  with open(os.path.join(RESULTS_FOLDER, 'segments.pkl'), 'rb') as f:
    data = pkl.load(f)
  return data

def get_clusters():
  with open(os.path.join(RESULTS_FOLDER, 'clusters.pkl'), 'rb') as f:
    data = pkl.load(f)
  return data

def get_turnover():
  with open(os.path.join(RESULTS_FOLDER, 'turnover.pkl'), 'rb') as f:
    data = pkl.load(f)
  return data


def get_landings():
  with open(os.path.join(RESULTS_FOLDER, 'landings.pkl'), 'rb') as f:
    data = pkl.load(f)
  return data

def get_traffic_sources():
  with open(os.path.join(RESULTS_FOLDER, 'traffic_sources.pkl'), 'rb') as f:
    data = pkl.load(f)
  return data

def get_segments_stats():
  with open(os.path.join(RESULTS_FOLDER, 'segments_stats.pkl'), 'rb') as f:
    data = pkl.load(f)
  return data

def get_leads_ta_stats():
  with open(os.path.join(RESULTS_FOLDER, 'leads_ta_stats.pkl'), 'rb') as f:
    data = pkl.load(f)
  return data