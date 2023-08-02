import click
import datetime

from flask import Blueprint

from app.dags import commands as dags_commands

from . import types


bp = Blueprint("roistat", __name__)


@bp.cli.command("update-db")
@click.option("--date-from", "-df", required=True, type=types.date)
@click.option("--date-to", "-dt", required=True, type=types.date)
def update_db(date_from: datetime.date, date_to: datetime.date):
    """
    Update Roistat statistics in DB
    """
    assert date_from <= date_to, "`--date-from` must be less or equivalent `--date-to`"
    dags_commands.calculate_tables("roistat_to_db", date_from, date_to)


@bp.cli.command("statistics-processing")
def statistics_processing():
    """
    Processing Roistat statistics
    """
    dags_commands.calculate_tables("roistat_statistics")


@bp.cli.command("leads-processing")
def leads_processing():
    """
    Processing Roistat leads
    """
    dags_commands.calculate_tables("roistat_leads")


@bp.cli.command("update-levels")
def update_levels():
    """
    Update Roistat levels
    """
    dags_commands.calculate_tables("roistat_update_levels")
