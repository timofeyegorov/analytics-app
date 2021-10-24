import pickle as pkl
import os
from config import RESULTS_FOLDER

path = RESULTS_FOLDER
def get_leads_ta_stats():
    with open('dags/results/leads_ta_stats.pkl', 'rb') as f:
        result = pkl.load(f)
    return result

