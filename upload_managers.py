import pandas as pd

from app import database as db
from app.database.auth import User


class ManagerDB:

    def __init__(self):
        conn, cursor = db.connect()
        self.conn = conn
        self.cursor = cursor

    def read_users(self):
        query = "SELECT * FROM users"
        self.cursor.execute(query)
        data = self.cursor.fetchall()
        return data

    @staticmethod
    def __read_xls(path: str) -> list[tuple[str, str, str]]:
        df = pd.read_excel(path)
        return [tuple(v) for v in df.values]

    def upload_users(self, path: str) -> None:
        sql = "INSERT INTO users (username, login, password) VALUES (%s, %s, MD5(%s));"
        values = self.__read_xls(path)
        self.cursor.executemany(sql, values)
        self.conn.commit()
        self.conn.close()
        return None

    def upload_one_user(self, login, password, username):
        sql = f"INSERT INTO users (login, password, username) VALUES (%s, MD5(%s), %s);"
        value = (login, password, username)
        self.cursor.execute(sql, value)
        self.conn.commit()
        self.conn.close()
        print('status: ok')


if __name__ == '__main__':
    manager_service = ManagerDB()
    manager_service.upload_users('managers.xlsx')
    # print('-' * 20)
    # print(manager_service.read_users())