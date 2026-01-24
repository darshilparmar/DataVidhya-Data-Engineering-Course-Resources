from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
import json
from kafka.errors import CommitFailedError

consumer = KafkaConsumer(
    bootstrap_servers = ["localhost:9092"],
    group_id = "CountryCounter",
    key_deserializer = lambda m: m if isinstance(m, bytes) else m.decode("utf-8"),
    value_deserializer = lambda m: json.loads(m),
    enable_auto_commit=False,
    # auto_commit_interval_ms=1000 
)

consumer.subscribe(['customerCountries'])
# cust_country_map = {}

def process_record(record):
    print(f"Processing record: {record.value}")

#tracking offset
current_offsets = {}
count = 0

try:
    while True:
        records = consumer.poll(timeout_ms=100)
        for topic_partition, messages in records.items():
            for message in messages:
                process_record(message)
                print(f"Topic: {message.topic}, Partition: {message.partition}, Offset: {message.offset}")
                current_offsets[TopicPartition(message.topic,message.partition)] = OffsetAndMetadata(message.offset + 1, None)
                count += 1

                #Commit offset every 1000 messages
                if count % 2 == 0:
                    consumer.commit_async(current_offsets)
except Exception as e:
    print(f"Unexpected error: {e}")

finally:
    try:
        consumer.commit(current_offsets)
    finally:
        consumer.close()
