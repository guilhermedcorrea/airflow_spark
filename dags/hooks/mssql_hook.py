import pyodbc
from airflow.hooks.dbapi_hook import DbApiHook

class MsSqlHook(DbApiHook):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_conn(self):
        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=<YOUR_SERVER_NAME>;'
            'DATABASE=<YOUR_DATABASE_NAME>;'
            'UID=<YOUR_USERNAME>;'
            'PWD=<YOUR_PASSWORD>;'
        )
        return conn

    def get_records(self, sql):
        conn = self.get_conn()
        with conn.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()
        return rows
