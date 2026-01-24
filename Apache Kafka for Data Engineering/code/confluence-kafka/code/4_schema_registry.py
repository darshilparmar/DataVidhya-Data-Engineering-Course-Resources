#  sudo sh -c 'echo "127.0.0.1 schema_registry" >> /etc/hosts'   
# On Windows, add this entry to C:\Windows\System32\drivers\etc\hosts with admin privileges.

import asyncio
from dataclasses import asdict, dataclass, field
from io import BytesIO
import io
import json
import random

from confluent_kafka import avro, Producer, Consumer
from confluent_kafka.avro import AvroConsumer, AvroProducer, CachedSchemaRegistryClient
from faker import Faker

faker = Faker()

BROKER_URL = "PLAINTEXT://localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"


@dataclass
class ClickAttribute:
    element: str = field(default_factory=lambda: random.choice(['div',"a",'button']))
    content: str = field(default_factory=faker.bs)

    @classmethod
    def attributes(self):
        return {faker.uri_page(): ClickAttribute() for _ in range(random.randint(1,5))}

@dataclass
class ClickEvent:
    email: str = field(default_factory=faker.email)
    timestamp: str = field(default_factory=faker.iso8601)
    uri: str = field(default_factory=faker.uri)
    number: str = field(default_factory=lambda: random.randint(0, 999))
    attributes: str = field(default_factory=ClickAttribute.attributes)

    schema = avro.loads("""{
        "type": "record",
        "name": "click_event",
        "namespace": "com.datavidhya.complexavro",
        "fields": [
            {"name": "email", "type":"string"},
            {"name": "timestamp", "type":"string"},
            {"name": "uri", "type":"string"},
            {"name": "number", "type":"int"},
            {
                "name": "attributes",
                "type": {
                    "type": "map",
                    "values": {
                        "type": "record",
                        "name": "click_attribute",
                        "fields": [
                            {"name": "element", "type": "string"},
                            {"name": "content", "type": "string"}
                        ]
                    }
                }
            }
        ]
    }""")

async def produce(topic_name):
    schema_registry = CachedSchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    p = AvroProducer({"bootstrap.servers": BROKER_URL}, schema_registry=schema_registry)

    while True:
        try:
            event = ClickEvent()
            print(event)


            p.produce(
                topic=topic_name,
                value=asdict(event),
                value_schema=ClickEvent.schema,
            )
            p.flush(timeout=1)
        except Exception as e:
            print(f"Error in producer: {e}")
      
        
        await asyncio.sleep(1.0)

async def consume(topic_name):
    schema_registry = CachedSchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    c = AvroConsumer(
        {"bootstrap.servers": BROKER_URL, "group.id":"clickevent0", "auto.offset.reset": "earliest"},
        schema_registry=schema_registry,
    )

    c.subscribe([topic_name])

    while True:
        message = c.poll(1.0)
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            try:
                print("consumer start.....")
                print(message.value())
            except KeyError as e:
                print(f"Failed to unpack message {e}")
        await asyncio.sleep(1.0)



async def produce_consume(topic_name):
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce(topic_name))
    t2 = asyncio.create_task(consume(topic_name))
    
    await asyncio.gather(t1, t2)



def main():
    """Checks for topic and creates the topic if it does not exist"""

    try:
        asyncio.run(produce_consume("clickeventsg"))
    except KeyboardInterrupt as e:
        print("shutting down")



if __name__ == "__main__":
    main()