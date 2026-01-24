import requests

class HttpService:
    def __init__(self):
        self.http_conn = requests.Session()

    def get(self, url, params={}):
        return self.http_conn.get(url, params=params)
    
    def post(self, url, data={}):
        return self.http_conn.post(url, data=data)
    
    def config_service(self, headers):
        self.http_conn.headers.update(headers)