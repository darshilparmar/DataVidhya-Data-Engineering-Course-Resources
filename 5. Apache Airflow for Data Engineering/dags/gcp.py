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

# Set GCP project variables
GCP_PROJECT = 'forward-cab-126312'
GCS_BUCKET = 'crypto-airlfow-learn-project-test'
GCS_RAW_DATA_PATH = 'raw_data/crypto_raw_data'
GCS_TRANSFORMED_DATA_PATH = 'transformed_data/crypto_transformed_data'
BIGQUERY_DATASET = 'crypto_db'
BIGQUERY_TABLE = 'tbl_crypto'
current_datetime = datetime.now().strftime("%Y%m%d%H%M%S")

BQ_SCHEMA = [
        {'name': 'id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'symbol', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'name', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'current_price', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'market_cap', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'total_volume', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
    ]

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

# Define the DAG
dag = DAG(
    'coingecko_to_bigquery',
    default_args=default_args,
    description='Fetch data from CoinGecko API, store on GCS, transform, and load to BigQuery',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 7, 1),
    catchup=False,
)

def _fetch_data_from_api():
    url = 'https://api.coingecko.com/api/v3/coins/markets'
    params = {
        'vs_currency': 'usd',
        'order': 'market_cap_desc',
        'per_page': 10,
        'page': 1,
        'sparkline': False
    }
    response = requests.get(url, params=params)
    data = response.json()

    with open(f'crypto_data.json', 'w') as f:
        json.dump(data, f)

def _transform_data():
    with open(f'crypto_data.json', 'r') as f:
        data = json.load(f)

    transformed_data = []
    for item in data:
        transformed_data.append({
            'id': item['id'],
            'symbol': item['symbol'],
            'name': item['name'],
            'current_price': item['current_price'],
            'market_cap': float(item['market_cap']),
            'total_volume': float(item['total_volume']),
            'timestamp': datetime.utcnow().isoformat()
        })

    # Convert to DataFrame
    df = pd.DataFrame(transformed_data)
    # Write CSV
    df.to_csv(f'transformed_data.csv', index=False)


# Create GCS bucket if not exists
create_bucket_task = GCSCreateBucketOperator(
    task_id='create_bucket',
    bucket_name=GCS_BUCKET,
    storage_class='MULTI_REGIONAL',
    location='US',
    gcp_conn_id='google_cloud_default',
    dag=dag,
)

# Fetch data from CoinGecko API
fetch_data_task = PythonOperator(
    task_id='fetch_data_from_api',
    python_callable=_fetch_data_from_api,
    dag=dag,
)

# Upload raw data to GCS
upload_raw_data_to_gcs_task = LocalFilesystemToGCSOperator(
    task_id='upload_raw_data_to_gcs',
    src=f'crypto_data.json',
    dst=GCS_RAW_DATA_PATH + "_{{ ts_nodash }}.json",
    bucket=GCS_BUCKET,
    gcp_conn_id='google_cloud_default',
    dag=dag,
)

# Transform data
transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=_transform_data,
    dag=dag,
)

# Upload transformed data to GCS
upload_transformed_data_to_gcs_task = LocalFilesystemToGCSOperator(
    task_id='upload_transformed_data_to_gcs',
    src='transformed_data.csv',
    dst=GCS_TRANSFORMED_DATA_PATH + "_{{ ts_nodash }}.csv",
    bucket=GCS_BUCKET,
    gcp_conn_id='google_cloud_default',
    dag=dag,
)

# Create BigQuery dataset
create_bigquery_dataset_task = BigQueryCreateEmptyDatasetOperator(
    task_id='create_bigquery_dataset',
    dataset_id=BIGQUERY_DATASET,
    gcp_conn_id='google_cloud_default',
    dag=dag,
)

# Create BigQuery table
create_bigquery_table_task = BigQueryCreateEmptyTableOperator(
    task_id='create_bigquery_table',
    dataset_id=BIGQUERY_DATASET,
    table_id=BIGQUERY_TABLE,
    schema_fields=BQ_SCHEMA,
    gcp_conn_id='google_cloud_default',
    dag=dag,
)

# Load data from GCS to BigQuery
load_to_bigquery_task = GCSToBigQueryOperator(
    task_id='load_to_bigquery',
    bucket=GCS_BUCKET,
    source_objects=[GCS_TRANSFORMED_DATA_PATH + "_{{ ts_nodash }}.csv"],
    destination_project_dataset_table=f'{GCP_PROJECT}:{BIGQUERY_DATASET}.{BIGQUERY_TABLE}',
    source_format='CSV',
    schema_fields=BQ_SCHEMA,
    write_disposition='WRITE_APPEND',
    skip_leading_rows=1,
    gcp_conn_id='google_cloud_default',
    dag=dag,
)
create_bucket_task >> fetch_data_task >> upload_raw_data_to_gcs_task
upload_raw_data_to_gcs_task >> transform_data_task >> upload_transformed_data_to_gcs_task
upload_transformed_data_to_gcs_task >> create_bigquery_dataset_task >> create_bigquery_table_task
create_bigquery_table_task >> load_to_bigquery_task



