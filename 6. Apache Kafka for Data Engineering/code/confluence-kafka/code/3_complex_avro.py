import asyncio
from dataclasses import asdict, dataclass, field
from io import BytesIO
import io
import json
import random

from confluent_kafka import Producer, Consumer
from faker import Faker
from fastavro import parse_schema, reader, writer

faker = Faker()

BROKER_URL = "PLAINTEXT://localhost:9092"

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

    schema = parse_schema({
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
    })

    def serialize(self):
        out = io.BytesIO()
        writer(out, 
               ClickEvent.schema,
               [asdict(self)],
               )
        print(out.getvalue())
        return out.getvalue()
    
async def produce(topic_name):
    """Produces data into the Kafka Topic"""
    p = Producer({"bootstrap.servers": BROKER_URL})
    while True:
        p.produce(topic_name, ClickEvent().serialize())
        await asyncio.sleep(1.0)

async def produce_consume(topic_name):
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce(topic_name))
    # t2 = asyncio.create_task(consume(topic_name))
    await t1
    # await t2

def main():
    """Checks for topic and creates the topic if it does not exist"""

    try:
        asyncio.run(produce_consume("clickevent"))
    except KeyboardInterrupt as e:
        print("shutting down")


if __name__ == "__main__":
    main()