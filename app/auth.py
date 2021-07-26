from app import app
from .database.auth import get_session

from flask import make_response, request, render_template, redirect
from hashlib import md5

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
