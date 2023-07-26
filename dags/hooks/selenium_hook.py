from airflow.hooks.base_hook import BaseHook
from selenium import webdriver

class SeleniumHook(BaseHook):
    def __init__(self, conn_id='selenium_default'):
        self.conn_id = conn_id

    def get_driver(self):
        conn = self.get_connection(self.conn_id)
        driver = webdriver.Chrome(executable_path=conn.host)
        return driver
