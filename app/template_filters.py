from flask_babel import format_datetime as babel_format_datetime
from app import app

from flask_babel import Babel
babel = Babel(app)


@app.template_filter()
def format_datetime(value, date_format: str = "dd.mm.YYYY"):
    if not value:
        return ''
    return babel_format_datetime(value, date_format)
