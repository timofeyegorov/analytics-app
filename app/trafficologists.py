from app import app
from app.database import delete_trafficologist, delete_account, get_trafficologists, get_accounts, add_account, add_trafficologist

from flask import redirect, render_template, request

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