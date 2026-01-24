from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from my_custom_hook import MyCustomHook

class MyCustomOperator(BaseOperator):
    @apply_defaults
    def __init__(self, endpoint, method='GET', data=None, headers=None, conn_id='my_custom_conn_id', *args, **kwargs):
        super(MyCustomOperator, self).__init__(*args, **kwargs)
        self.endpoint = endpoint
        self.method = method
        self.data = data
        self.headers = headers
        self.conn_id = conn_id

    def execute(self, context):
        hook = MyCustomHook(conn_id=self.conn_id)
        response = hook.make_request(
            endpoint=self.endpoint,
            method=self.method,
            data=self.data,
            headers=self.headers
        )
        self.log.info("Response: %s", response)