import asyncio
import json 

import requests

KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "dbconnect"


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
                    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                    "topic": CONNECTOR_NAME,
                    "mode": "bulk",
                    "tasks.max": 1,
                    "incrementing.column.name": "actor_id",  
                    "table.whitelist": "actor",
                    "tasks.max": 1,
                    "connection.url": "jdbc:postgresql://db:5432/sample",
                    "connection.user": "sample",
                    "connection.password": "sample",
                    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "key.converter.schemas.enable": "false",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter.schemas.enable": "false",
                },
            }
        ),
    )

    try:
        resp.raise_for_status()
    except:
        print(f"failed to create connector {CONNECTOR_NAME}")
        exit(1)

    print("connector created successfully")

if __name__ == "__main__":
    configure_connector()