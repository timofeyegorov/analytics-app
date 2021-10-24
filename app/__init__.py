import plotly.express as px
from plotly.io import to_json
import pandas as pd
from flask import Flask, request, render_template
from .database.auth import check_token
from .database import get_leads_data, get_target_audience
from config import DATA_FOLDER
import os
import pickle as pkl

# def fig_leads_dynamics():
#   result = get_leads_data()
#   result['created_at'] = pd.to_datetime(result['created_at'])
#   leads_day_df = result.resample('D', on='created_at')['id'].count().to_frame()
#   fig = px.histogram(result, x = 'created_at', nbins = leads_day_df.shape[0])
#   fig.update_layout(title = 'Количество лидов по дням', title_x = 0.5,
#                     xaxis_title='Дата',
#                     yaxis_title='Лиды',)
#   graphJSON = to_json(fig)
#   # graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
#   return graphJSON

app = Flask(__name__)

@app.route('/')
@app.route('/index')
def index():
    token = request.cookies.get('token')
    if check_token(token):
        return render_template('index.html') #, fig_leads_dynamics=fig_leads_dynamics)
    else:
        return redirect('/login')

@app.route('/leads')
def data():
    data = get_leads_data()
    data.traffic_channel = data.traffic_channel.str.split('?').str[0]
    return render_template('leads.html', tables={'Leads': data.drop(columns=['id'])})

@app.route('/target_audience')
def target_audience():
    with open(os.path.join(DATA_FOLDER, 'target_audience.pkl'), 'rb') as f:
        target_audience = pkl.load(f)
    # return render_template('target_audience.html', target_audience=get_target_audience())
    return render_template('target_audience.html', target_audience=target_audience)

@app.route('/crops')
def crops():
    with open(os.path.join(DATA_FOLDER, 'crops.pkl'), 'rb') as f:
        crops = pkl.load(f)
    # return render_template('target_audience.html', target_audience=get_target_audience())
    return render_template('crops.html', crops=crops)

from .auth import *
from .analytics import *
from .trafficologists import *