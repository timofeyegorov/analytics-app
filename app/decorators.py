from flask import request, session, redirect, url_for

from app.database.auth import get_user_by_id


def auth(method, *args, **kwargs):
    def wrapper(*args, **kwargs):
        login_endpoint = "login"
        login_post_endpoint = "login_post"
        index_endpoint = "root"

        request.user = get_user_by_id(session.get("uid"))

        if request.user is not None and request.endpoint in [
            login_endpoint,
            login_post_endpoint,
        ]:
            return redirect(url_for(index_endpoint))

        if request.user is None and request.endpoint not in [
            login_endpoint,
            login_post_endpoint,
        ]:
            return redirect(url_for(login_endpoint))

        return method(*args, **kwargs)

    return wrapper
