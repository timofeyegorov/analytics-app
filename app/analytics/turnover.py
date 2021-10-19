import pickle as pkl

def get_turnover():
  with open('dags/results/turnover.pkl', 'rb') as f:
    result = pkl.load(f)
  return result