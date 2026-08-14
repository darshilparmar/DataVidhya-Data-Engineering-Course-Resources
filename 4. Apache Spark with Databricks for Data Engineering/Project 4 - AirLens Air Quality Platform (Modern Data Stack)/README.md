# AirLens: Air Quality Monitoring on the Modern Data Stack

All code for the AirLens project on Data Vidhya:
https://datavidhya.com/learn/projects/airlens-aqi-modern-data-stack/

Three public air quality APIs land in a Databricks Free Edition lakehouse,
dbt builds the gold marts, DuckDB serves them locally, Streamlit visualises
them, and Airflow orchestrates the whole chain. Every tool is on a free tier.

## Run order

1. **`ingestion/airlens_ingest_local.py`** runs on your laptop, not in Databricks.
   Fetches 20 cities from OpenWeather, WAQI and OpenAQ, writes one wrapped JSON
   envelope per city per source, then uploads them to the Unity Catalog volume.

   ```bash
   python3 -m venv venv && source venv/bin/activate
   pip install requests databricks-sdk
   export OPENWEATHER_KEY=... WAQI_TOKEN=... OPENAQ_KEY=...
   export DATABRICKS_HOST=https://<your-workspace>.cloud.databricks.com
   python3 ingestion/airlens_ingest_local.py --no-upload   # fetch only, inspect first
   python3 ingestion/airlens_ingest_local.py               # fetch and upload
   ```

2. **`notebooks/airlens_bronze.py`** imports into Databricks (Workspace, kebab menu,
   Import). Auto Loader reads the landing volume into three bronze Delta tables,
   exactly once per file.

   The drop-and-clear-checkpoints cell at the bottom is a **recovery tool**, not part
   of the normal flow. Run it only when schema inference changes, because it deletes
   the tables and the Auto Loader checkpoints.

3. **`notebooks/airlens_silver.py`** harmonises the three payloads into
   `silver_aq_readings`, `silver_weather`, `silver_stations` and `silver_cities`,
   keeping `aqi_scale` explicit on every reading.

4. **`dbt/`** is the transformation layer, targeting a Databricks SQL warehouse.
   Copy `profiles.yml.example` to `~/.dbt/profiles.yml` for dbt Core, or paste the
   files into the dbt Cloud IDE.

   ```bash
   dbt debug && dbt build
   ```

   `dbt/compiled_manual_run.sql` is the same logic as plain Databricks SQL, for
   running the gold layer without dbt at all.

5. **`serving/build_duckdb.py`** builds a local DuckDB file from the landed JSON,
   so the serving layer runs with no cloud access.

   ```bash
   pip install duckdb pandas
   python3 serving/build_duckdb.py
   ```

6. **`app/streamlit_app.py`** is the dashboard over that DuckDB file.

   ```bash
   pip install -r app/requirements.txt
   streamlit run app/streamlit_app.py
   ```

7. **`airflow/`** orchestrates the whole chain as a five task DAG: edge ingestion,
   upload to the volume, a Databricks job that rebuilds bronze and silver, the dbt
   Cloud job, and the DuckDB refresh. Requires Docker Desktop.

   ```bash
   cd airflow
   docker compose build && docker compose up -d
   docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
   # then open http://localhost:8081 and log in as admin
   ```

   Fill in `DATABRICKS_TOKEN`, `DATABRICKS_JOB_ID`, `DBT_CLOUD_API_TOKEN` and
   `DBT_CLOUD_JOB_ID` in `.env` first: a scheduler cannot use browser OAuth.
   Create the Databricks job with `airlens_bronze` and `airlens_silver` as two
   dependent notebook tasks, and the dbt Cloud job in a deployment environment.

## Notes

- Copy `.env.example` to `.env` and fill it in. Never commit real keys.
- Two ingestion runs on different days are needed to see the incremental behaviour
  in `mart_city_daily`. Run it, come back later, run it again.
- The two dbt tests under `dbt/tests/` are deliberately strict. The final lesson
  breaks one on purpose to practise diagnosing a failing pipeline.
