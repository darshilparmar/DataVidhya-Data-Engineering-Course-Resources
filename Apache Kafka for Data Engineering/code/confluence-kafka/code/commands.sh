
#connect ksql cli 
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088

# Connect to the broker container
docker exec -it broker bash

# Create the purchases topic with 3 partitions and replication factor of 1
kafka-topics --create --topic purchases --bootstrap-server broker:9092 --partitions 3 --replication-factor 1

# Start the console producer with key and value
docker exec -it broker kafka-console-producer --topic purchases --broker-list broker:9092 --property "parse.key=true" --property "key.separator=:"

john:{"username":"john","currency":"USD","amount":125.40}
alice:{"username":"alice","currency":"EUR","amount":99.99}
bob:{"username":"bob","currency":"GBP","amount":72.50}
john:{"username":"john","currency":"USD","amount":20.00}

CREATE STREAM purchases_stream (
    username VARCHAR KEY,
    username_value VARCHAR,
    currency VARCHAR,
    amount DOUBLE
) WITH (
    KAFKA_TOPIC='purchases',
    VALUE_FORMAT='JSON'
);


SHOW TOPICS;

 SHOW STREAMS;

  SELECT * FROM purchases limit 10;


CREATE STREAM high_value_purchase AS SELECT * FROM purchases_stream WHERE AMOUNT > 20;

 DROP STREAM high_value_purchase;


SHOW QUERIES;

TERMINATE QUERTY_ID;


SET