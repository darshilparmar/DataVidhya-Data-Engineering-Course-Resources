# Databricks notebook source
# MAGIC %md
# MAGIC # ShopStream event generator
# MAGIC Simulates the live order feed. Every few seconds it drops a small JSON file
# MAGIC of order events into the `events` Volume — exactly how many real systems land
# MAGIC data (Kinesis Firehose, Kafka sink connectors, app exports all write files).
# MAGIC Auto Loader in the next notebook picks these up as a stream.
# MAGIC
# MAGIC Run this notebook, leave it running, and switch to the streaming notebook.

# COMMAND ----------

import json
import random
import time
import uuid
from datetime import datetime, timezone

CATALOG = "shopstream"
SCHEMA = "core"
EVENTS_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/events/orders_stream"

dbutils.fs.mkdirs(EVENTS_PATH)

random.seed()  # live feed, so genuinely random

PRODUCT_IDS = [f"P{i:04d}" for i in range(1, 198)]
CUSTOMER_IDS = [f"C{i:05d}" for i in range(1, 1001)]
STATUSES = ["completed"] * 9 + ["cancelled"]

BATCHES = 60          # how many files to drop
EVENTS_PER_BATCH = 25 # order lines per file
SLEEP_SECONDS = 5     # gap between files

for batch in range(BATCHES):
    events = []
    for _ in range(EVENTS_PER_BATCH):
        events.append({
            "event_id": str(uuid.uuid4()),
            "order_id": f"S{random.randint(1, 999999):06d}",
            "customer_id": random.choice(CUSTOMER_IDS),
            "product_id": random.choice(PRODUCT_IDS),
            "quantity": random.choices([1, 2, 3], weights=[75, 18, 7])[0],
            "unit_price": round(random.uniform(9, 400), 2),
            "status": random.choice(STATUSES),
            "event_ts": datetime.now(timezone.utc).isoformat(),
        })
    fname = f"{EVENTS_PATH}/events_{int(time.time())}_{batch:03d}.json"
    dbutils.fs.put(fname, "\n".join(json.dumps(e) for e in events), overwrite=True)
    print(f"[{batch + 1}/{BATCHES}] wrote {EVENTS_PER_BATCH} events -> {fname}")
    time.sleep(SLEEP_SECONDS)

print("Done. The feed has stopped — re-run this cell to send more events.")
