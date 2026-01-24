import json 
from kafka import KafkaConsumer
from helpers import ConsumerRebalanceListenerHandler

class Consumer:
    def __init__(self, group_id):
        self.consumer= KafkaConsumer(
            group_id=group_id,
            bootstrap_servers=["localhost:9092"],
            auto_offset_reset='earliest',
            value_deserializer = lambda v:json.loads(v.decode("utf-8"))
        )
        self.rebalance_listener = ConsumerRebalanceListenerHandler(self.consumer)

    def subscribe_consumer(self, topics):
        self.consumer.subscribe(topics)

    def ubsubscribe_consumer(self):
        self.consumer.unsubscribe()