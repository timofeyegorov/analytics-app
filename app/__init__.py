import plotly.express as px
from plotly.io import to_json
import pandas as pd
from flask import Flask, request, render_template
from .database.auth import check_token
from .database import get_leads_data, get_target_audience
import json
from config import RESULTS_FOLDER
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

@app.route('/trafficologists')
def trafficologist_page(trafficologist_error=None, account_error=None):
    with open(os.path.join(RESULTS_FOLDER, 'trafficologists.pkl'), 'rb') as f:
        trafficologists = pkl.load(f)
    # accounts = get_accounts()
    # trafficologists = get_trafficologists()
    # trafficologists2 = get_trafficologists()
    # print(trafficologists)
    return render_template("trafficologists.html",
        tables=[trafficologists.to_html()],
        ) #,
        # trafficologists=zip(trafficologists.id, trafficologists.name),
        # trafficologists2=zip(trafficologists2.id, trafficologists2.name),
        # trafficologist_error=trafficologist_error,
        # account_error=account_error)

@app.route('/target_audience')
def target_audience():
    with open(os.path.join(RESULTS_FOLDER, 'target_audience.pkl'), 'rb') as f:
        target_audience = pkl.load(f)
    # return render_template('target_audience.html', target_audience=get_target_audience())
    return render_template('target_audience.html', target_audience=target_audience)

@app.route('/crops')
def crops():
    with open(os.path.join(RESULTS_FOLDER, 'crops_list.pkl'), 'rb') as f:
        crops = pkl.load(f)
    # return render_template('target_audience.html', target_audience=get_target_audience())
    return render_template('crops.html', crops=crops)

@app.route('/expenses')
def expenses():
    with open(os.path.join(RESULTS_FOLDER, 'expenses.json'), 'r', encoding='cp1251') as f:
        expenses = json.load(f)
    exp = []
    for i in range(len(expenses)):
        for k, v in expenses[i]['items'].items():
            exp.append([k, v,
                        expenses[i]['dateFrom'],
                        ])
    exp = pd.DataFrame(exp, columns=['Ройстат', 'Расход', 'Дата'])
    exp['Дата'] = pd.to_datetime(exp['Дата']).dt.normalize()
    output_dict = {'Кол-во записей - ': exp.shape,
                    'Расход с 01.11 по 30.11 - ':
                    round(exp[(exp['Дата'] >= '2021-11-01') & (exp['Дата'] <= '2021-11-30')]['Расход'].sum()),
                    'Расход с 01.12 по 31.12 - ':
                    round(exp[(exp['Дата'] >= '2021-12-01') & (exp['Дата'] <= '2021-12-31')]['Расход'].sum()),
                    'Расход по facebook32 с 01.11 по 30.11 - ': round(
                    exp[(exp['Ройстат'] == 'Facebook 32') & (exp['Дата'] >= '2021-11-01') & (exp['Дата'] <= '2021-11-30')][
                   'Расход'].sum()),
                    'Расход по Михаил с 01.11 по 30.11 - ':
                    round(exp[((exp['Ройстат'] == 'Facebook Michail Zh (ИП2)') | (exp['Ройстат'] == 'Facebook 31')) & \
                    (exp['Дата'] >= '2021-11-01') & (exp['Дата'] <= '2021-11-30')]['Расход'].sum()),
                    'Расход по facebook32 с 01.12 по 31.12 - ': round(
                    exp[(exp['Ройстат'] == 'Facebook 32') & (exp['Дата'] >= '2021-12-01') & (exp['Дата'] <= '2021-12-31')][
                        'Расход'].sum()),
                    'Расход по Михаил с 01.12 по 31.12 - ':
                    round(exp[((exp['Ройстат'] == 'Facebook Michail Zh (ИП2)') | (exp['Ройстат'] == 'Facebook 31')) & \
                                (exp['Дата'] >= '2021-12-01') & (exp['Дата'] <= '2021-12-31')]['Расход'].sum()),
                   'Расход с 1 по 20 января': [round(
                       exp[(exp['Дата'] >= '2022-01-01') & (exp['Дата'] <= '2022-01-20')]['Расход'].sum()),
                       round(
                           exp[(exp['Дата'] >= '2022-01-01') & (exp['Дата'] <= '2022-01-20')]['Расход'].sum()) * 1.2
                   ],
                   'Расход с 1 по 21 января': [round(
                       exp[(exp['Дата'] >= '2022-01-01') & (exp['Дата'] <= '2022-01-21')]['Расход'].sum()),
                       round(
                           exp[(exp['Дата'] >= '2022-01-01') & (exp['Дата'] <= '2022-01-21')]['Расход'].sum()) * 1.2
                   ],
                   'Расход за январь': round(exp[(exp['Дата'] >= '2022-01-01') & (exp['Дата'] <= '2022-01-31')]['Расход'].sum()),

                   }
    # return render_template('target_audience.html', target_audience=get_target_audience())
    return render_template('expenses.html', exp=output_dict)

@app.route('/statuses')
def statuses_page():
    with open(os.path.join(RESULTS_FOLDER, 'statuses.pkl'), 'rb') as f:
        statuses = pkl.load(f)
    # accounts = get_accounts()
    # trafficologists = get_trafficologists()
    # trafficologists2 = get_trafficologists()
    # print(trafficologists)
    return render_template("statuses.html",
        tables=[statuses.to_html()],
        ) #,
        # trafficologists=zip(trafficologists.id, trafficologists.name),
        # trafficologists2=zip(trafficologists2.id, trafficologists2.name),
        # trafficologist_error=trafficologist_error,
        # account_error=account_error)

from .auth import *
from .analytics import *
from .trafficologists import *
