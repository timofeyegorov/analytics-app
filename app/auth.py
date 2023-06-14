from app import app, decorators
from app.database.auth import auth_user

from flask import request, session, render_template, redirect, url_for
from hashlib import md5


@app.route("/login", methods=["get"], endpoint="login")
@decorators.auth
def login_page():
    return render_template("login.html")


@app.route("/login", methods=["post"], endpoint="login_post")
@decorators.auth
def login_action():
    login = request.form.get("login")
    password = request.form.get("password")
    password = md5(password.encode("utf-8")).hexdigest()
    user = auth_user(login, password)
    if user is None:
        return render_template("login.html", error="Неверный логин или пароль")
    else:
        session.setdefault("uid", user.id)
        return redirect(url_for("root"))


@app.route("/logout", methods=["get"], endpoint="logout")
def logout():
    session.clear()
    return redirect(url_for("login"))
