import asyncio
from dataclasses import asdict, dataclass, field
import io
import json
import random

from confluent_kafka import Producer, Consumer
from faker import Faker
from fastavro import parse_schema, writer, reader

faker = Faker()

BROKER_URL = "PLAINTEXT://localhost:9092"


@dataclass
class Purchase:
    username: str = field(default_factory=faker.user_name)
    currency: str = field(default_factory=faker.currency_code)
    amount: int = field(default_factory=lambda: random.randint(100, 200000))

    #avro schema
    schema = parse_schema({
        "type": "record",
        "name": "stockmarket",
        "namespace": "com.datavidya.stockmarket",
        "fields": [
            {"name": "username", "type": "string"},
            {"name": "currency", "type": "string"},
            {"name": "amount", "type": "int"}
        ]
    })

    def serialize(self):
        out = io.BytesIO()
        writer(out, 
               Purchase.schema,
               [asdict(self)],
               )
        # print(out.getvalue())
        return out.getvalue()


async def produce(topic_name):
    """Produces data into the Kafka Topic"""
    p = Producer({"bootstrap.servers": BROKER_URL})
    while True:
        p.produce(topic_name, Purchase().serialize())
        await asyncio.sleep(1.0)



async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    c = Consumer({
        "bootstrap.servers": BROKER_URL,
        "group.id": "stockmarket-consumer",
        "auto.offset.reset": "earliest"
    })
    c.subscribe([topic_name])
    while True:
        message = c.poll(1.0)
        print(message)
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            try:
                bytes_reader = io.BytesIO(message.value())

                avro_reader = reader(bytes_reader)

                for record in avro_reader:
                    purchase = Purchase(
                        username=record["username"],
                        currency=record["currency"],
                        amount=record["amount"],
                    )
                    print(f"Received message: {purchase}")
            except Exception as e:
                print(f"Error processing message: {e}")
        await asyncio.sleep(1.0)


async def produce_consume(topic_name):
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce(topic_name))
    t2 = asyncio.create_task(consume(topic_name))
    await t1
    await t2

def main():
    """Checks for topic and creates the topic if it does not exist"""

    try:
        asyncio.run(produce_consume("stockmarket"))
    except KeyboardInterrupt as e:
        print("shutting down")





if __name__ == "__main__":
    main()