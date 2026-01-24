from dataclasses import asdict, dataclass, field
import json
import time
import random

import requests
from confluent_kafka import avro, Consumer, Producer
from confluent_kafka.avro import AvroConsumer, AvroProducer, CachedSchemaRegistryClient
from faker import Faker


faker = Faker()
REST_PROXY_URL = "http://localhost:8084"


AVRO_SCHEMA = """{
    "type": "record",
    "name": "click_event",
    "fields": [
        {"name": "email", "type": "string"},
        {"name": "timestamp", "type": "string"},
        {"name": "uri", "type": "string"},
        {"name": "number", "type": "int"}
    ]
}"""


def produce():
    ## Write producer using REST proxy
    headers = {"Content-Type" : "application/vnd.kafka.avro.v2+json"}

    data = {
        "value_schema": AVRO_SCHEMA,
        "records": [{"value": asdict(ClickEvent())}]
    }

    print(data)

    resp = requests.post(
        f"{REST_PROXY_URL}/topics/clickeventproxy",
        data=json.dumps(data),
        headers=headers,
    )

    try:
        resp.raise_for_status()
    except:
        print(f"Failed to send data to REST proxy {json.dumps(resp.json(), indent=2)}")

    print(f"Sent data to REST proxy {json.dumps(resp.json(), indent=2)}")


@dataclass
class ClickEvent:
    email: str = field(default_factory=faker.email)
    timestamp: str = field(default_factory=faker.iso8601)
    uri: str = field(default_factory=faker.uri)
    number: int = field(default_factory=lambda: random.randint(0, 999))


def main():
    """Runs the simulation against REST Proxy"""
    try:
        while True:
            produce()
            time.sleep(0.5)
    except KeyboardInterrupt as e:
        print("shutting down")


if __name__ == "__main__":
    main()
