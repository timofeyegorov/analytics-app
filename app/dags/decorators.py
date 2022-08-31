import redis

from datetime import datetime

from config import config


redis_config = config.get("redis", {})
redis_db = redis.StrictRedis(
    host=redis_config.get("host", ""),
    port=redis_config.get("port", ""),
    charset="utf-8",
    db=redis_config.get("db", ""),
)


def log_execution_time(param_name):
    def decorator(func):
        def wrapper():
            try:
                func()
                last_updated = datetime.now().isoformat()
                redis_db.hmset(
                    param_name, {"status": "ok", "last_updated": last_updated}
                )
            except Exception as e:
                redis_db.hmset(param_name, {"status": "fail", "message": str(e)})
                raise e

        return wrapper

    return decorator
