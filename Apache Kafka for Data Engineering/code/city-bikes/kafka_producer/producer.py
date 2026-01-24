import json
from kafka import KafkaProducer

class Producer:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=["localhost:9092"],
                                      value_serializer = lambda v: json.dumps(v).encode("utf-8"))
        
    def data_producer(self, topic, msg):
        self.producer.send(topic, msg)