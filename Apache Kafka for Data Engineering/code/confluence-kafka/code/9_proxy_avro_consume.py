import asyncio
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

TOPIC_NAME = "clickeventproxy"
CONSUMER_GROUP = f"clickeventcg-{random.randint(0,100000)}"

async def consume():
    consumer_name = "clickeventconsumer"
    headers = {"Content-Type" : "application/vnd.kafka.avro.v2+json"}

    # creates consumer group
    data = {
        "name": consumer_name,
        "format": "avro"
    }

    resp = requests.post(
        f"{REST_PROXY_URL}/consumers/{CONSUMER_GROUP}",
        data=json.dumps(data),
        headers=headers
    )

    try:
        resp.raise_for_status()
        print(resp.raise_for_status())
    except:
        print("Error creating consumer")
        return
    
    print("REST Proxy consumer group created")

    resp_data = resp.json()
    print(resp_data)
    resp_data['base_uri'] = resp_data['base_uri'].replace("rest_proxy","localhost")

    #creates subscription to the topic
    data = {
        "topics": [TOPIC_NAME],
    }

    resp = requests.post(
        f"{resp_data['base_uri']}/subscription",
        data=json.dumps(data),
        headers=headers
    )

    try:
        resp.raise_for_status()
    except:
        print("Error subscribing to topic")

    print("REST PROXY consumer subscription created")

    while True:
        headers = {"Accept" : "application/vnd.kafka.avro.v2+json"}
        resp_data['base_uri'] = resp_data['base_uri'].replace("rest_proxy","localhost")

        resp = requests.get(
            f"{resp_data['base_uri']}/records?timeout=10000",
            headers=headers,
        )

        try:
            resp.raise_for_status()
        except:
            print("Failed to fetch recrods")

        print("Consuming records via REST proxy")
        print(f"{json.dumps(resp.json())}")
        await asyncio.sleep(0.1)



async def produce_consume(topic_name):
    """Runs the Producer tasks"""
    t2 = asyncio.create_task(consume())
    await t2


def main():
    """Runs the simulation against REST Proxy"""
    try:
        asyncio.run(produce_consume(TOPIC_NAME))
    except KeyboardInterrupt as e:
        print("shutting down")


if __name__ == "__main__":
    main()
