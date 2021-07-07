from flask import Flask, render_template, request, redirect, make_response, jsonify
from database import get_all_data, get_accounts, get_trafficologists, add_account, add_trafficologist, get_countries, get_ages, get_jobs, get_earnings, get_trainings, get_times
from database import get_ta_filters
from analytics import get_segments, get_clusters
from turnover import get_turnover
from auth import check_token, get_session
import hashlib
import pandas as pd
from datetime import datetime
from hashlib import md5

app = Flask(__name__)

@app.route('/')
@app.route('/index')
def index():
    token = request.cookies.get('token')
    if check_token(token):
        return render_template('index.html')
    else:
        return redirect('/login')

@app.route('/login', methods=['get'])
def login_page():
    return render_template('login.html')

@app.route('/login', methods=['post'])
def login_action():
    login = request.form.get('login')
    password = request.form.get('password')
    password = md5(password.encode('utf-8')).hexdigest()
    session = get_session(login, password)
    if session is None:
        return render_template('login.html', error='Неверный логин или пароль')
    else:
        resp = make_response(redirect('/'))
        resp.set_cookie('token', session['token'])
        return resp

@app.route('/logout', methods=['get'])
def logout():
    resp = make_response(redirect('/'))
    resp.set_cookie('token', '')
    return resp

@app.route('/segments')
def segments():
    date_start = request.args.get('date_start')
    date_end = request.args.get('date_end')
    table = get_all_data()
    if date_start:
        table = table[table.date_request >= datetime.strptime(date_start, '%Y-%m-%d')]
    if date_end:
        table = table[table.date_request <= datetime.strptime(date_end, '%Y-%m-%d')]
    if len(table) == 0:
        return render_template('segments.html', error='Нет данных для заданного периода')
    tables = get_segments(table)
    return render_template('segments.html', tables=tables)

@app.route('/turnover')
def turnover():
    date_start = request.args.get('date-start')
    date_end = request.args.get('date-end')
    table = get_all_data()
    if date_start:
        table = table[table.date_request >= datetime.strptime(date_start, '%Y-%m-%d')]
    if date_end:
        table = table[table.date_request <= datetime.strptime(date_end, '%Y-%m-%d')]
    if len(table) == 0:
        return render_template('turonver.html', error='Not enough data')
    tables, traffic_channel = get_turnover(table)
    return render_template('turnover.html', tables=tables, traffic_channel=traffic_channel)

@app.route('/clusters')
def clusters():
    date_start = request.args.get('date-start')
    date_end = request.args.get('date-end')
    table = get_all_data()
    if date_start:
        table = table[table.date_request >= datetime.strptime(date_start, '%Y-%m-%d')]
    if date_end:
        table = table[table.date_request <= datetime.strptime(date_end, '%Y-%m-%d')]
    if len(table) == 0:
        return render_template('clusters.html', error='Not enough data')
    tables = get_clusters(table)
    def hash(str):
        return hashlib.md5(str.encode('utf-8')).hexdigest()
    return render_template('clusters.html', tables=tables, hash=hash, enumerate=enumerate)

@app.route('/trafficologists')
def trafficologist_page():
    accounts = get_accounts()
    trafficologists = get_trafficologists()
    return render_template("trafficologists.html", accounts=accounts.to_html(), trafficologists=zip(trafficologists.id, trafficologists.name))

@app.route('/trafficologists/add', methods=['post'])
def add_trafficologist_request():
    name = request.form.get('name')
    add_trafficologist(name)
    return redirect('/trafficologists')

@app.route('/trafficologists/add_account', methods=['post'])
def add_account_request():
    title = request.form.get('title')
    label = request.form.get('label')
    trafficologist_id = request.form.get('trafficologist_id')
    add_account(title, label, trafficologist_id)
    return redirect('/trafficologists')

@app.route('/target_audience')
def target_audience_page():
    ta_filters = get_ta_filters(1)
    print(ta_filters)
    return render_template('ta.html', filters=ta_filters)

@app.route('/target_audience/add', methods=['get'])
def add_ta_page():
    countries = get_countries()
    ages = get_ages()
    jobs = get_jobs()
    earnings = get_earnings()
    trainings = get_trainings()
    times = get_times()
    
    return render_template('create_ta_filter.html', 
        countries=countries,
        ages=ages,
        jobs=jobs,
        earnings=earnings,
        trainings=trainings,
        times=times)

@app.route('/target_audience/add', methods=['post'])
def add_ta_action():
    countries = request.form.get('countries')
    return jsonify(countries)

app.run('0.0.0.0', 8000, debug=True)



# МНОЖИТЕЛЬ
