from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
import json
from kafka.errors import CommitFailedError
import signal

consumer = KafkaConsumer(
    bootstrap_servers = ["localhost:9092"],
    group_id = "CountryCounter",
    key_deserializer = lambda m: m if isinstance(m, bytes) else m.decode("utf-8"),
    value_deserializer = lambda m: json.loads(m),
    enable_auto_commit=False,
    # auto_commit_interval_ms=1000 
)

topic = "customerCountries"
partition=0
specific_offset = 129

topic_partition = TopicPartition(topic, partition)
consumer.assign([topic_partition])

consumer.seek(topic_partition, specific_offset)

running = True 

def shutdown(sig, frame):
    global running

    print("shutting down consumer")

    running = False 

signal.signal(signal.SIGINT, shutdown)

try:
    while running:
        records = consumer.poll(timeout_ms=100)
        for tp, messages in records.items():
            for message in messages:
                print(f"Topic: {message.topic}, Partition: {message.partition}, Offset: {message.offset}")
                print(f"Key: {message.key}, Value: {message.value}") 

            consumer.commit_async({tp:OffsetAndMetadata(message.offset + 1, None)})

except Exception as e:
    print(f"Exception {e}")

finally:
    consumer.close()