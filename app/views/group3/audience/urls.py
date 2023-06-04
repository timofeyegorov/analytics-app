from flask import current_app

from . import views


current_app.append("absolute", "absolute", views.AbsoluteView)
current_app.append("relative", "relative", views.RelativeView)
