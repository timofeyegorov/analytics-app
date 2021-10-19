import pickle as pkl

def get_traffic_sources():
    with open('dags/results/traffic_sources.pkl', 'rb') as f:
        result = pkl.load(f)
    return result
