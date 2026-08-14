# Databricks notebook source
# MAGIC %md
# MAGIC # AirLens: bronze layer with Auto Loader
# MAGIC The landing Volume now receives small JSON files from three APIs, and more
# MAGIC arrive with every ingestion run. Bronze picks up every new file exactly once
# MAGIC and preserves it in a Delta table. One bronze table per source keeps the raw
# MAGIC payload contracts separate until silver harmonizes them.

# COMMAND ----------

LANDING = "/Volumes/airlens/core/landing"
CHECKPOINTS = "/Volumes/airlens/core/landing/_checkpoints"

# COMMAND ----------

# Peek first: what did one OpenWeather envelope actually look like?
df = spark.read.json(f"{LANDING}/openweather/*/*.json")
df.printSchema()
print(df.count(), "openweather envelopes on disk")

# COMMAND ----------

def bronze_stream(source):
    """Run one available-now Auto Loader pass for one API source."""
    (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINTS}/bronze_{source}")
        .load(f"{LANDING}/{source}")
        .writeStream
        .option("checkpointLocation", f"{CHECKPOINTS}/bronze_{source}")
        .trigger(availableNow=True)
        .toTable(f"airlens.core.bronze_{source}")
        .awaitTermination())
    return spark.table(f"airlens.core.bronze_{source}").count()

# COMMAND ----------

LANDING = "/Volumes/airlens/core/landing"
CHECKPOINTS = "/Volumes/airlens/core/landing/_checkpoints"
# One-time reset: schema inference changed, so bronze must be rebuilt from scratch.
spark.sql("DROP TABLE IF EXISTS airlens.core.bronze_openweather")
spark.sql("DROP TABLE IF EXISTS airlens.core.bronze_waqi")
spark.sql("DROP TABLE IF EXISTS airlens.core.bronze_openaq")
dbutils.fs.rm(CHECKPOINTS, recurse=True)
print("bronze reset: tables dropped, checkpoints cleared")

# COMMAND ----------

for source in ["openweather", "waqi", "openaq"]:
    rows = bronze_stream(source)
    print(f"bronze_{source}: {rows} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The small-file problem, measured
# MAGIC Every hourly run drops one small file per city and source. Query engines pay a
# MAGIC fixed cost per file. Delta tracks that growth, so compaction can be added when
# MAGIC the tables become large enough to justify it.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL airlens.core.bronze_openweather

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Every envelope is traceable: which run, which city, fetched when.
# MAGIC SELECT source, city, run_id, fetched_at_utc
# MAGIC FROM airlens.core.bronze_waqi
# MAGIC ORDER BY city LIMIT 10
