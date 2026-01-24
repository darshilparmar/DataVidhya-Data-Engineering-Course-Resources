import json
import requests
from datetime import datetime, timedelta
import pandas as pd 

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator

GCP_PROJECT = "learn-airflow-428415"
GCS_BUCKET = "crypto-exchange-pipeline-darshil"
GCS_RAW_DATA_PATH = "raw_data/crypto_raw_data"
GCS_TRANSFORMED_PATH = "transformed_data/crypto_transformed_data"
BIGQUERY_DATASET = "crypto_db"
BIGQUERY_TABLE = "tbl_crypto"

BQ_SCHEMA = [
    {"name": "id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "symbol", "type": "STRING", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "current_price", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "market_cap", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "total_volume", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "last_updated", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
]

def _fetch_data_from_api():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        'vs_currency' : "usd",
        'order' : 'market_cap_desc',
        'per_page' : 10,
        'page' : 1,
        'sparkline' : False 
    }

    response = requests.get(url, params=params)
    data = response.json()
    
    with open("crypto_data.json", 'w') as f:
        json.dump(data, f)

def _transform_data():
    with open("crypto_data.json", 'r') as f:
        data = json.load(f)

    transformed_data = []
    for item in data:
        transformed_data.append({
            'id' : item['id'],
            'symbol' : item['symbol'],
            'name' : item['name'],
            'current_price' : item['current_price'],
            'market_cap' : item['market_cap'],
            'total_volume' : item['total_volume'],
            'last_updated' : item['last_updated'],
            'timestamp' : datetime.utcnow().isoformat()
        })

    df = pd.DataFrame(transformed_data)

    df.to_csv("transformed_data.csv", index=False)


default_args = {
    "owner": "darshil",
    "depends_on_past": False
}

dag = DAG(
    dag_id = "crypto_exchange_pipeline",
    default_args=default_args,
    description="Fetch data from coingecko api",
    schedule_interval=timedelta(minutes=10),
    start_date=datetime(2024, 7, 4),
    catchup=False
)

# Fetch data from Cypto API
fetch_data_task = PythonOperator(
    task_id = "fetch_data_from_api",
    python_callable = _fetch_data_from_api,
    dag=dag,
)

# Crate GCS Bucket
create_bucket_task = GCSCreateBucketOperator(
    task_id = "create_bucket",
    bucket_name = GCS_BUCKET,
    storage_class= "MULTI_REGIONAL",
    location="US",
    gcp_conn_id="google_cloud_default",
    dag=dag,
)

#Upload Raw Data to GCS
upload_raw_data_to_gcs_task = LocalFilesystemToGCSOperator(
    task_id = "upload_raw_data_to_gcs",
    src="crypto_data.json",
    dst=GCS_RAW_DATA_PATH + "_{{ ts_nodash }}.json",
    bucket=GCS_BUCKET,
    gcp_conn_id="google_cloud_default",
    dag=dag,
)

# Transform Data
transformed_data_task = PythonOperator(
    task_id = 'trasnformed_data',
    python_callable = _transform_data,
    dag=dag,
)

#Upload transformed data to GCS
upload_transformed_data_to_gcs_task = LocalFilesystemToGCSOperator(
    task_id = "upload_transformed_data_to_gcs",
    src="transformed_data.csv",
    dst=GCS_TRANSFORMED_PATH + "_{{ ts_nodash }}.csv",
    bucket=GCS_BUCKET,
    gcp_conn_id="google_cloud_default",
    dag=dag,
)

# Create BigQuery dataset
create_bigquery_dataset_task = BigQueryCreateEmptyDatasetOperator(
    task_id = "create_bigquery_dataset",
    dataset_id=BIGQUERY_DATASET,
    gcp_conn_id="google_cloud_default",
    dag=dag,
)

# Create BigQuery Table
create_bigquery_table_task = BigQueryCreateEmptyTableOperator(
    task_id = "create_bigquery_table",
    dataset_id=BIGQUERY_DATASET,
    table_id=BIGQUERY_TABLE,
    schema_fields=BQ_SCHEMA,
    gcp_conn_id="google_cloud_default",
    dag=dag,
)

# Load Data to BigQuery
load_to_bigquery = GCSToBigQueryOperator(
    task_id="load_to_bigquery",
    bucket=GCS_BUCKET,
    source_objects=[GCS_TRANSFORMED_PATH + "_{{ ts_nodash }}.csv"],
    destination_project_dataset_table=f'{GCP_PROJECT}:{BIGQUERY_DATASET}.{BIGQUERY_TABLE}',
    source_format="csv",
    schema_fields=BQ_SCHEMA,
    write_disposition="WRITE_APPEND",
    skip_leading_rows=1,
    gcp_conn_id="google_cloud_default",
    dag=dag,
)

fetch_data_task >> create_bucket_task >> upload_raw_data_to_gcs_task 
upload_raw_data_to_gcs_task >> transformed_data_task >> upload_transformed_data_to_gcs_task
upload_transformed_data_to_gcs_task >> create_bigquery_dataset_task >> create_bigquery_table_task
create_bigquery_table_task >> load_to_bigquery
