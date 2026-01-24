from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from kafka.errors import CommitFailedError
import json

consumer = KafkaConsumer(
    bootstrap_servers = ['localhost:9092'],
    group_id = "CountryCounter",
    key_deserializer = lambda m: m if isinstance(m, bytes) else m.decode("utf-8"),
    value_deserializer = lambda m: json.loads(m),
    enable_auto_commit=False 
)

consumer.subscribe(['customerCountries'])

def process_message(message):
    print(f"Processing message: {message.value}")

current_offsets = {}
count = 0

# def commit_callback(offsets, exception):
#     if exception is not None:
#         print(f"Commit failed for offset {offsets} : {exception}")
#     else:
#         print(f"Commit succeeded for offsets: {offsets}")

try:
    while True:
        records = consumer.poll(timeout_ms=1000)
        for topic_partition, messages in records.items():
            for message in messages:
                process_message(message)
                print(f"Topic: {message.topic}, Partition: {message.partition}, Offset: {message.offset}")
                current_offsets[TopicPartition(message.topic,message.partition)] = OffsetAndMetadata(message.offset + 1, None)
                count += 1

                if count % 2 == 0:
                    print(f"Commit offset: {message.offset}")
                    consumer.commit_async(current_offsets)


        # consumer.commit_async(callback=commit_callback)

except Exception as e:
    print(f"Unexpected error: {e}")

finally:
    try:
        print("Commit offset manually")
        print(count)
        consumer.commit(current_offsets)
    finally:
        consumer.close()
