import pickle as pkl

def get_segments():
  with open('dags/results/segments.pkl', 'rb') as f:
    data = pkl.load(f)
  return data