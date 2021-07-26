from app import app
from app.database import get_leads_data
from .clusters import get_clusters
from .segments import get_segments
from .landings import get_landings
from .turnover import get_turnover
from .leads_ta_stats import get_leads_ta_stats
from .segments_stats import get_segments_stats
from .traffic_sources import get_traffic_sources

from flask import render_template, request
from datetime import datetime

@app.route('/segments')
def segments():
    date_start = request.args.get('date_start')
    date_end = request.args.get('date_end')
    table = get_leads_data()

    if date_start:
        table = table[table.date_request >= datetime.strptime(date_start, '%Y-%m-%d')]
    if date_end:
        table = table[table.date_request <= datetime.strptime(date_end, '%Y-%m-%d')]
    if len(table) == 0:
        return render_template('segments.html', error='Нет данных для заданного периода')
    tables = get_segments(table)
    return render_template(
        'segments.html', 
        tables=tables, date_start=date_start, date_end=date_end
    )

@app.route('/turnover')
def turnover():
    date_request_start = request.args.get('date_request_start')
    date_request_end = request.args.get('date_request_end')
    date_payment_start = request.args.get('date_payment_start')
    date_payment_end = request.args.get('date_payment_end')
    tab = request.args.get('tab')
    table = get_leads_data()
    
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
    tables, traffic_channel, ta = get_turnover(table)
    return render_template(
        'turnover.html', 
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
    table = get_leads_data()
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
    table = get_leads_data()
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
    table = get_leads_data()
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
    table = get_leads_data()
    if date_start:
        table = table[table.date_request >= datetime.strptime(date_start, '%Y-%m-%d')]
    if date_end:
        table = table[table.date_request <= datetime.strptime(date_end, '%Y-%m-%d')]
    if len(table) == 0:
        return render_template('leads_ta_stats.html', error='Not enough data', date_start=date_start, date_end=date_end)
    table = get_leads_ta_stats(table)
    return render_template('leads_ta_stats.html', table=table, date_start=date_start, date_end=date_end)


@app.route('/landings')
def landings():
    date_start = request.args.get('date_start')
    date_end = request.args.get('date_end')
    table = get_leads_data()

    if date_start:
        table = table[table.date_request >= datetime.strptime(date_start, '%Y-%m-%d')]
    if date_end:
        table = table[table.date_request <= datetime.strptime(date_end, '%Y-%m-%d')]
    if len(table) == 0:
        return render_template('landings.html', error='Нет данных для заданного периода')
    tables = get_landings(table)
    return render_template(
        'landings.html', 
        tables=tables, date_start=date_start, date_end=date_end
    )
