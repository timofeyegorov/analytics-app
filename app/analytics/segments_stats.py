import pickle as pkl

def get_segments_stats():
    with open('dags/results/segments_stats.pkl', 'rb') as f:
        result = pkl.load(f)
    return result

