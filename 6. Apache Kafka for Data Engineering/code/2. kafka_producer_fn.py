from kafka import KafkaProducer
import json
import time 

producer = KafkaProducer(
    bootstrap_servers = ["localhost:9092"],
    key_serializer = lambda m: m if isinstance(m, bytes) else m.encode("utf-8"),
    value_serializer = lambda m: json.dumps(m).encode("utf-8")
)

# List of messages to send
messages = [
    {"customer_id": 1, "customer_name": "Alice", "country": "USA"},
    {"customer_id": 2, "customer_name": "Bob", "country": "India"},
    {"customer_id": 3, "customer_name": "Charlie", "country": "UK"},
    {"customer_id": 4, "customer_name": "Diana", "country": "Canada"},
    {"customer_id": 5, "customer_name": "Eve", "country": "Australia"}
]

try:
    for idx, message in enumerate(messages):
        producer.send(
            topic = "customerCountries",
            key=f"customer-{idx}".encode("utf-8"),
            value = message
        )
        print(f"Sent: {message}")
        time.sleep(1)
finally:
    producer.flush()
    producer.close()