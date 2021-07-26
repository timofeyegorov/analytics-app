from flask import Flask, json, render_template, request, redirect, make_response, jsonify
from database import get_all_data, get_accounts, get_trafficologists, add_account, add_trafficologist, get_countries, get_ages, get_jobs, get_earnings, get_trainings, get_times
from database import get_ta_filters, get_ta_filter, add_ta_filter, edit_ta_filter, delete_trafficologist, delete_account
from analytics import get_segments, get_clusters, get_traffic_sources, get_segments_stats, get_leads_ta_stats
from analytics import get_landings
from analytics import get_turnover
from auth import check_token, get_session
import pandas as pd
from datetime import datetime
import pickle as pkl
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
    ta_filter = request.args.get('ta_filter', 1)
    table = get_all_data()

    ta_filter = get_ta_filter(ta_filter)
    ta_filters = get_ta_filters(1)
    if date_start:
        table = table[table.date_request >= datetime.strptime(date_start, '%Y-%m-%d')]
    if date_end:
        table = table[table.date_request <= datetime.strptime(date_end, '%Y-%m-%d')]
    if len(table) == 0:
        return render_template('segments.html', error='Нет данных для заданного периода')
    tables = get_segments(table, ta_filter)
    return render_template(
        'segments.html', 
        ta_filters=ta_filters, filter=ta_filter, 
        tables=tables, date_start=date_start, date_end=date_end
    )

@app.route('/turnover')
def turnover():
    date_request_start = request.args.get('date_request_start')
    date_request_end = request.args.get('date_request_end')
    date_payment_start = request.args.get('date_payment_start')
    date_payment_end = request.args.get('date_payment_end')
    ta_filter = request.args.get('ta_filter', 1)
    tab = request.args.get('tab')
    table = get_all_data()

    ta_filter = get_ta_filter(ta_filter)
    ta_filters = get_ta_filters(1)
    if date_request_start:
        table = table[table.date_request >= datetime.strptime(date_request_start, '%Y-%m-%d')]
    if date_request_end:
        table = table[table.date_request <= datetime.strptime(date_request_end, '%Y-%m-%d')]
    if date_payment_start:
        table = table[table.date_payment >= datetime.strptime(date_payment_start, '%Y-%m-%d')]
    if date_payment_end:
        table = table[table.date_payment <= datetime.strptime(date_payment_end, '%Y-%m-%d')]
    if len(table) == 0:
        return render_template(
            'turnover.html', 
            error='Not enough data',
            date_request_start=date_request_start,
            date_request_end=date_request_end,
            date_payment_start=date_payment_start,
            date_payment_end=date_payment_end,
            tab=tab
        )
    tables, traffic_channel, ta = get_turnover(table, ta_filter)
    # traffic_channel['traffic_channel']['Сегмент'] = traffic_channel['traffic_channel']['Сегмент'].str[:100]
    # print(traffic_channel['traffic_channel'].columns)
    return render_template(
        'turnover.html', 
        ta_filters=ta_filters, filter=ta_filter, 
        tables=tables,
        traffic_channel=traffic_channel,
        date_request_start=date_request_start,
        date_request_end=date_request_end,
        date_payment_start=date_payment_start,
        date_payment_end=date_payment_end,
        tab=tab
    )

@app.route('/clusters')
def clusters():
    date_start = request.args.get('date_start')
    date_end = request.args.get('date_end')
    tab = request.args.get('tab')
    table = get_all_data()
    if date_start:
        table = table[table.date_request >= datetime.strptime(date_start, '%Y-%m-%d')]
    if date_end:
        table = table[table.date_request <= datetime.strptime(date_end, '%Y-%m-%d')]
    if len(table) == 0:
        return render_template('clusters.html', error='Not enough data', date_start=date_start, date_end=date_end, tab=tab)
    tables = get_clusters(table)
    return render_template('clusters.html', tables=tables, date_start=date_start, date_end=date_end, tab=tab)

@app.route('/traffic_sources')
def traffic_sources():
    date_start = request.args.get('date_start')
    date_end = request.args.get('date_end')
    tab = request.args.get('tab')
    table = get_all_data()
    if date_start:
        table = table[table.date_request >= datetime.strptime(date_start, '%Y-%m-%d')]
    if date_end:
        table = table[table.date_request <= datetime.strptime(date_end, '%Y-%m-%d')]
    if len(table) == 0:
        return render_template('traffic_sources.html', error='Not enough data', date_start=date_start, date_end=date_end, tab=tab)
    table = get_traffic_sources(table)
    return render_template('traffic_sources.html', table=table, date_start=date_start, date_end=date_end, tab=tab)

@app.route('/segments_stats')
def segments_stats():
    date_start = request.args.get('date_start')
    date_end = request.args.get('date_end')
    tab = request.args.get('tab')
    table = get_all_data()
    if date_start:
        table = table[table.date_request >= datetime.strptime(date_start, '%Y-%m-%d')]
    if date_end:
        table = table[table.date_request <= datetime.strptime(date_end, '%Y-%m-%d')]
    if len(table) == 0:
        return render_template('segments_stats.html', error='Not enough data', tab=tab, date_start=date_start, date_end=date_end)
    tables = get_segments_stats(table)
    return render_template('segments_stats.html', tables=tables, tab=tab, date_start=date_start, date_end=date_end)

@app.route('/leads_ta_stats')
def leads_ta_stats():
    date_start = request.args.get('date_start')
    date_end = request.args.get('date_end')
    table = get_all_data()
    if date_start:
        table = table[table.date_request >= datetime.strptime(date_start, '%Y-%m-%d')]
    if date_end:
        table = table[table.date_request <= datetime.strptime(date_end, '%Y-%m-%d')]
    if len(table) == 0:
        return render_template('leads_ta_stats.html', error='Not enough data', date_start=date_start, date_end=date_end)
    table = get_leads_ta_stats(table)
    # print(len(table.keys()))
    # print(table[list(table.keys())[0]].columns[0])
    # table[list(table.keys())[0]] = table[list(table.keys())[0]].str[:100]
    return render_template('leads_ta_stats.html', table=table, date_start=date_start, date_end=date_end)


@app.route('/landings')
def landings():
    date_start = request.args.get('date_start')
    date_end = request.args.get('date_end')
    ta_filter = request.args.get('ta_filter', 1)
    table = get_all_data()

    ta_filter = get_ta_filter(ta_filter)
    ta_filters = get_ta_filters(1)
    if date_start:
        table = table[table.date_request >= datetime.strptime(date_start, '%Y-%m-%d')]
    if date_end:
        table = table[table.date_request <= datetime.strptime(date_end, '%Y-%m-%d')]
    if len(table) == 0:
        return render_template('landings.html', error='Нет данных для заданного периода')
    tables = get_landings(table, ta_filter)
    return render_template(
        'landings.html', 
        ta_filters=ta_filters, filter=ta_filter, 
        tables=tables, date_start=date_start, date_end=date_end
    )


@app.route('/trafficologists/delete', methods=['post'])
def trafficologist_delete():
    id = request.form.get('id')
    delete_trafficologist(id)
    return redirect('/trafficologists')

@app.route('/accounts/delete', methods=['post'])
def accounts_delete():
    id = request.form.get('id')
    delete_account(id)
    return redirect('/trafficologists')

@app.route('/trafficologists')
def trafficologist_page(trafficologist_error=None, account_error=None):
    accounts = get_accounts()
    trafficologists = get_trafficologists()
    trafficologists2 = get_trafficologists()
    return render_template("trafficologists.html", 
        accounts=accounts, 
        trafficologists=zip(trafficologists.id, trafficologists.name), 
        trafficologists2=zip(trafficologists2.id, trafficologists2.name),
        trafficologist_error=trafficologist_error,
        account_error=account_error)

@app.route('/trafficologists/add', methods=['post'])
def add_trafficologist_request():
    name = request.form.get('name')
    trafficologists = get_trafficologists()
    if name in trafficologists.name.values:
        return trafficologist_page(trafficologist_error='Такой трафиколог уже есть')
    add_trafficologist(name)
    return redirect('/trafficologists')

@app.route('/trafficologists/add_account', methods=['post'])
def add_account_request():
    title = request.form.get('title')
    label = request.form.get('label')

    accounts = get_accounts()
    if title in accounts.title.values:
        return trafficologist_page(account_error='Такое название кабинета уже есть')
    if label in accounts.label.values:
        return trafficologist_page(account_error='Такая метка кабинета уже есть')

    trafficologist_id = request.form.get('trafficologist_id')
    add_account(title, label, trafficologist_id)
    return redirect('/trafficologists')

@app.route('/target_audience')
def target_audience_page():
    ta_filters = get_ta_filters(1)
    return render_template('ta.html', filters=ta_filters)

@app.route('/target_audience/<int:id>')
def specific_target_audience_page(id):
    ta_filter = get_ta_filter(id)
    title = ta_filter['title']
    pickle = ta_filter['pickle']
    with open('filters/' + pickle, 'rb') as f:
        countries, ages, jobs, earnings, trainings, times = pkl.load(f)
    return render_template('ta_display.html', title=title, countries=countries, ages=ages, jobs=jobs, earnings=earnings, trainings=trainings, times=times)

@app.route('/target_audience/<int:id>/edit')
def edit_target_audience_page(id):
    ta_filter = get_ta_filter(id)
    ta_title = ta_filter['title']
    ta_pickle = ta_filter['pickle']
    with open('filters/' + ta_pickle, 'rb') as f:
        ta_countries, ta_ages, ta_jobs, ta_earnings, ta_trainings, ta_times = pkl.load(f)

    countries = list(set([*get_countries(), *ta_countries]))
    ages = list(set([*get_ages(), *ta_ages]))
    jobs = list(set([*get_jobs(), *ta_jobs]))
    earnings = list(set([*get_earnings(), *ta_earnings]))
    trainings = list(set([*get_trainings(), *ta_trainings]))
    times = list(set([*get_times(), *ta_times]))

    return render_template(
        'edit_ta_filter.html', 
        id=id, title=ta_title, 
        ta_countries=ta_countries, countries=countries,
        ta_ages=ta_ages, ages=ages,
        ta_jobs=ta_jobs, jobs=jobs,
        ta_earnings=ta_earnings, earnings=earnings,
        ta_trainings=ta_trainings, trainings=trainings,
        ta_times=ta_times, times=times)


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
    id = request.form.get('id')
    countries = request.form.getlist('countries[]')
    ages = request.form.getlist('ages[]')
    jobs = request.form.getlist('jobs[]')
    earnings = request.form.getlist('earnings[]')
    trainings = request.form.getlist('trainings[]')
    times = request.form.getlist('times[]')
    title = request.form.get('title')
    if id:
        edit_ta_filter(id, title, countries, ages, jobs, earnings, trainings, times)
    else:
        add_ta_filter(1, title, countries, ages, jobs, earnings, trainings, times)
    return redirect('/target_audience')

app.run('0.0.0.0', 8000, debug=True)



# Источники трафика
# Источники трафика2 - 1
# Сегменты трифика2 - 1
# Лиды/ЦА 2 - 7

