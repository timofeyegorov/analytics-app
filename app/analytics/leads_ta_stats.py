import pickle as pkl

def get_leads_ta_stats():
    with open('dags/results/leads_ta_stats.pkl', 'rb') as f:
        result = pkl.load(f)
    return result
