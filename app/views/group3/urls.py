from flask import current_app

from . import views


current_app.append(
    "payments-accumulation", "payments_accumulation", views.PaymentsAccumulationView
)
current_app.append("marginality", "marginality", views.MarginalityView)
current_app.append("getPlotCSV", "get_plot_csv", views.GetPlotCSVView)
current_app.include("app.views.group3.audience.urls", "audience/")
