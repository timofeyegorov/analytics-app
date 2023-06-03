from flask import current_app

from . import views


# current_app.append("login", "login", views.Login)
current_app.include("app.views.group1.urls")
current_app.append("", "index", views.IndexView)
current_app.append("login", "login", views.LoginView)
current_app.append("<path:path>", "not_found", views.NotFoundView)
