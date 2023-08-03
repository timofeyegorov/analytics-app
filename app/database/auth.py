import pydantic

from uuid import uuid4 as uuid
from typing import Dict, Any, Optional
from pymysql.err import OperationalError

from . import connect


class User(pydantic.BaseModel):
    id: int
    login: str
    username: str


def get_user_by_id(id_: Optional[int] = None) -> Optional[User]:
    if id_ is None:
        return

    try:
        conn, cursor = connect()
        cursor.execute(f"SELECT * FROM users WHERE id={id_}")
        user = User(**cursor.fetchone())
    except OperationalError:
        user = None

    return user


def auth_user(login, password) -> Optional[User]:
    user = get_user(login, password)
    if not user:
        return
    return User(**user)


def check_token(token):
    if token is None or token == "":
        return False
    conn, cursor = connect()
    query = "SELECT * FROM sessions WHERE token=%s"
    cursor.execute(query, (token,))
    session = cursor.fetchone()
    conn.close()
    if session is None:
        return False
    return True


def get_user(login, password=None):
    conn, cursor = connect()
    if password is None:
        query = "SELECT * FROM users WHERE login=%s"
        cursor.execute(query, (login,))
        data = cursor.fetchone()
    else:
        query = "SELECT * FROM users WHERE login=%s AND password=%s"
        cursor.execute(query, (login, password))
        data = cursor.fetchone()
    return data


def get_session(login, password):
    user = get_user(login, password)
    conn, cursor = connect()
    if user is None:
        return None
    query = "SELECT * FROM sessions WHERE user_id=%s"
    cursor.execute(query, (user["id"]))
    session = cursor.fetchone()
    if session is None:
        query = "INSERT INTO sessions (user_id, token) VALUES (%s, %s)"
        token = str(uuid())
        cursor.execute(query, (user["id"], token))
        conn.commit()
        query = "SELECT * FROM sessions WHERE user_id=%s"
        cursor.execute(query, (user["id"]))
        session = cursor.fetchone()
        conn.close()
        return session
    else:
        conn.close()
        return session
