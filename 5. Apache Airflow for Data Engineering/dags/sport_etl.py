from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud import storage, bigquery
from datetime import datetime, timedelta
import requests
import pandas as pd
import json
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'cryptocurrency_data_pipeline',
    default_args=default_args,
    description='A pipeline to fetch and process cryptocurrency data',
    schedule_interval=timedelta(minutes=5),
)

BUCKET_NAME = 'crpyto-airflow-project-learn'
LANDING_ZONE = 'raw-data/to_processed'
LANDING_ZONE_PROCESSED = 'raw-data/processed'
PROCESSED_CSV = 'processed-data'
PROCESSED_FOLDER = 'processed_csv'
# FILE_NAME = 'cryptocurrency_data.json'
# CSV_FILE_NAME = 'cryptocurrency_data.csv'
COINGECKO_API_URL = "https://api.coingecko.com/api/v3/coins/markets"
PARAMETERS = {
    'vs_currency': 'usd',
    'order': 'market_cap_desc',
    'per_page': 100,
    'page': 1,
    'sparkline': 'false'
}

def _fetch_cryptocurrency_data(**kwargs):
    response = requests.get(COINGECKO_API_URL, params=PARAMETERS)
    data = response.json()
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    client = storage.Client()

    file_name = f'{LANDING_ZONE}/crypto_raw_{timestamp}.json'

    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(file_name)
    blob.upload_from_string(json.dumps(data), 'application/json')

    kwargs['ti'].xcom_push(key='spotify_filename', value=file_name)
    kwargs['ti'].xcom_push(key='spotify_data', value=json.dumps(data))
   
    
# def convert_json_to_csv(**kwargs):
#     client = storage.Client()
#     bucket = client.bucket(BUCKET_NAME)
#     blobs = list(bucket.list_blobs(prefix=RAW_FOLDER))
#     for blob in blobs:
#         if blob.name.endswith('.json'):
#             json_data = json.loads(blob.download_as_string())
#             df = pd.DataFrame(json_data)
#             # Add timestamp to the dataframe
#             df['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#             csv_blob_name = f'{CSV_FOLDER}/{blob.name.split("/")[-1].replace(".json", ".csv")}'
#             csv_blob = bucket.blob(csv_blob_name)
#             csv_blob.upload_from_string(df.to_csv(index=False), 'text/csv')

# def move_processed_files(**kwargs):
#     client = storage.Client()
#     bucket = client.bucket(BUCKET_NAME)
#     blobs = list(bucket.list_blobs(prefix=CSV_FOLDER))
#     for blob in blobs:
#         new_blob_name = f'{PROCESSED_FOLDER}/{blob.name.split("/")[-1]}'
#         bucket.rename_blob(blob, new_blob_name)

fetch_data = PythonOperator(
    task_id='fetch_cryptocurrency_data',
    python_callable=_fetch_cryptocurrency_data,
    dag=dag,
)

# convert_data = PythonOperator(
#     task_id='convert_json_to_csv',
#     python_callable=convert_json_to_csv,
#     dag=dag,
# )

# load_to_bq = GCSToBigQueryOperator(
#     task_id='load_to_bq',
#     bucket=BUCKET_NAME,
#     source_objects=[f'{CSV_FOLDER}/*.csv'],
#     destination_project_dataset_table='your_project.your_dataset.your_table',
#     source_format='CSV',
#     skip_leading_rows=1,
#     write_disposition='WRITE_APPEND',
#     dag=dag,
# )

# move_files = PythonOperator(
#     task_id='move_processed_files',
#     python_callable=move_processed_files,
#     dag=dag,
# )

fetch_data 
# >> convert_data >> load_to_bq >> move_files
