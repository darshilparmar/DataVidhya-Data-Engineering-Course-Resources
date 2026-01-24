from airflow.hooks.base_hook import BaseHook
import requests

class MyCustomHook(BaseHook):
    def __init__(self, conn_id="my_conn_id"):
        self.conn_id = conn_id
        self.connection = self.get_connection(conn_id)
        self.base_url = self.connection.host
        self.api_key = self.connection.password

    def get_conn(self):
        return requests.Session()

    def make_request(self, endpoint, method='GET', data=None, headers=None):
        session = self.get_conn()
        url = f"{self.base_url}/{endpoint}"
        headers = headers or {}
        headers['Authorization'] = f"Bearer {self.api_key}"

        response = session.request(method, url, json=data, headers=headers)
        response.raise_for_status()
        return response.json()