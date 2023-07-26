from airflow.hooks.base_hook import BaseHook
from livy.client import HttpClient

class LivyHook(BaseHook):
    def __init__(self, conn_id='livy_default'):
        self.conn_id = conn_id

    def get_livy_session(self):
        conn = self.get_connection(self.conn_id)
        return HttpClient(conn.host, auth=(conn.login, conn.password))
