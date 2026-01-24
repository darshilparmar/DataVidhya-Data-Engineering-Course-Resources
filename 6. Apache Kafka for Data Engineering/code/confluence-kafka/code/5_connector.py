import asyncio
import json

import requests

KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "dvconnect8"

def configure_connector():

    print("creating kafka connector...")

    rest_method = requests.post
    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        print(f"found connector {CONNECTOR_NAME}, skipping creation")

    
    resp = rest_method(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {
                "name": CONNECTOR_NAME,
                "config": {
                    "connector.class": "FileStreamSource",
                    "topic": CONNECTOR_NAME,
                    "tasks.max": 1,
                    "file": f"/tmp/{CONNECTOR_NAME}.log",
                    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "key.converter.schemas.enable": "false",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter.schemas.enable": "false",
                    "offset.storage.file.filename": "/tmp/connect.offsets",
                    "file.mode": "tail",
                },
            }
        ),
    )

    resp.raise_for_status()
    print("connector created successfully")

async def log():
    iteration = 0

    while True:
        with open(f"/tmp/{CONNECTOR_NAME}.log", "a") as f:
            message = f"log number {iteration}\n"
            f.write(message)
            f.flush()
            print(f"my message {message}")
            await asyncio.sleep(1.0)
            iteration += 1

async def log_task():
    configure_connector()
    task = asyncio.create_task(log())
    await task

def run():
    try:
        asyncio.run(log_task())
    except KeyboardInterrupt as e:
        print("stopping connector...")


if __name__ == "__main__":
    run()