from app import app

from flask import Flask, json, render_template, request, redirect, make_response, jsonify
# from app.database import get_all_data, get_accounts, get_trafficologists, add_account, add_trafficologist, get_countries, get_ages, get_jobs, get_earnings, get_trainings, get_times
from app.database import delete_trafficologist, delete_account
# from analytics import get_segments, get_clusters, get_traffic_sources, get_segments_stats, get_leads_ta_stats
# from analytics import get_landings
# from analytics import get_turnover
from app.database.auth import check_token, get_session
import pandas as pd
from datetime import datetime
import pickle as pkl
from hashlib import md5

app.run('0.0.0.0', 8000, debug=True)



# Источники трафика
# Источники трафика2 - 1
# Сегменты трифика2 - 1
# Лиды/ЦА 2 - 7

