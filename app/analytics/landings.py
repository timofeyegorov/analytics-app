from app.database import get_target_audience

import pandas as pd
from collections import defaultdict
import pickle as pkl


def get_landings():
  with open('dags/results/landings.pkl', 'rb') as f:
    result = pkl.load(f)
  return result
