from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from my_custom_hook import MyCustomHook

class MyCustomSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, endpoint, expected_value, method='GET', data=None, headers=None, conn_id='my_custom_conn_id', *args, **kwargs):
        super(MyCustomSensor, self).__init__(*args, **kwargs)
        self.endpoint = endpoint
        self.expected_value = expected_value
        self.method = method
        self.data = data
        self.headers = headers
        self.conn_id = conn_id

    def poke(self, context):
        hook = MyCustomHook(conn_id=self.conn_id)
        response = hook.make_request(
            endpoint=self.endpoint,
            method=self.method,
            data=self.data,
            headers=self.headers
        )
        self.log.info("Response: %s", response)
        return response.get('status') == self.expected_value

