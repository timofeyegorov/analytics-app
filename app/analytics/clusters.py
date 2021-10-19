import numpy as np
import pandas as pd
import pickle as pkl

def get_clusters():
  with open('dags/results/clusters.pkl', 'rb') as f:
    res_dict = pkl.load(f)
  return res_dict
