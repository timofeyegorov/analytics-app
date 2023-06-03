from pathlib import Path
from dotenv import load_dotenv

from . import apps


BASE_DIR = Path(__file__).parent.parent
load_dotenv(BASE_DIR / ".env")

flask_app = apps.flask.FlaskApp(
    import_name=__name__,
    static_url_path="/assets",
    static_folder="assets",
    root_path=BASE_DIR,
)
redis_app = apps.redis.RedisApp(flask_app)
celery_app = apps.celery.CeleryApp(flask_app)
database_app = apps.database.DatabaseApp(flask_app)
csrf_app = apps.csrf.CSRFApp(flask_app)
minify_app = apps.minify.MinifyApp(flask_app)
pickle_app = apps.pickle.PickleApp(flask_app)

with flask_app.app_context():
    flask_app.include("app.views.urls", "/", True)
    flask_app.routing.build()
