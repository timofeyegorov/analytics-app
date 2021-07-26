from flask import Flask, request, render_template
from .database.auth import check_token

app = Flask(__name__)

@app.route('/')
@app.route('/index')
def index():
    token = request.cookies.get('token')
    if check_token(token):
        return render_template('index.html')
    else:
        return redirect('/login')

from .auth import *
from .analytics import *
from .trafficologists import *