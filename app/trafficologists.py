from app import app
from app.database import (
    delete_trafficologist,
    delete_account,
    add_account,
    add_trafficologist,
)
from app.database import get_trafficologists
from app import decorators
from config import DATA_FOLDER
import os
import pickle as pkl
from flask import redirect, render_template, request


@app.route(
    "/trafficologists/delete", methods=["post"], endpoint="trafficologists_delete"
)
@decorators.auth
def trafficologist_delete():
    id = request.form.get("id")
    delete_trafficologist(id)
    return redirect("/trafficologists")


@app.route("/accounts/delete", methods=["post"], endpoint="accounts_delete")
@decorators.auth
def accounts_delete():
    id = request.form.get("id")
    delete_account(id)
    return redirect("/trafficologists")


@app.route("/trafficologists/add", methods=["post"], endpoint="trafficologists_add")
@decorators.auth
def add_trafficologist_request():
    name = request.form.get("name")
    trafficologists = get_trafficologists()
    if name in trafficologists.name.values:
        return trafficologist_page(trafficologist_error="Такой трафиколог уже есть")
    add_trafficologist(name)
    return redirect("/trafficologists")


@app.route(
    "/trafficologists/add_account",
    methods=["post"],
    endpoint="trafficologists_add_account",
)
@decorators.auth
def add_account_request():
    title = request.form.get("title")
    label = request.form.get("label")

    accounts = get_accounts()
    if title in accounts.title.values:
        return trafficologist_page(account_error="Такое название кабинета уже есть")
    if label in accounts.label.values:
        return trafficologist_page(account_error="Такая метка кабинета уже есть")

    trafficologist_id = request.form.get("trafficologist_id")
    add_account(title, label, trafficologist_id)
    return redirect("/trafficologists")
