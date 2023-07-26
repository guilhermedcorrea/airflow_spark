import psycopg2
from airflow.hooks.dbapi_hook import DbApiHook

class PostgresHook(DbApiHook):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_conn(self):
        conn = psycopg2.connect(
            host='<YOUR_HOST>',
            port='<YOUR_PORT>',
            database='<YOUR_DATABASE>',
            user='<YOUR_USERNAME>',
            password='<YOUR_PASSWORD>',
        )
        return conn

    def get_records(self, sql):
        conn = self.get_conn()
        with conn.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()
        return rows
