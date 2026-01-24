#ds = YYYY-MM-DD
#nodash = YYYYMMDD

from datetime import datetime
from pathlib import Path 

import pandas as pd 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


dag = DAG(
    dag_id="incremental_data_processing",
    start_date=datetime(2024, 6, 1),
    # end_date=datetime(2024,6,7),
    schedule_interval='@daily',
    catchup=False,
)


fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
        "curl -o /tmp/data/events_{{ds}}.json http://events_api:5000/events?"
        "start_date=2024-06-01&"
        "end_date=2024-06-07"
        # "start_date={{execution_date.strftime('%Y-%m-%d')}}&"
        # "end_date={{next_execution_date.strftime('%Y-%m-%d')}}"
        # "start_date={{ds}}&"
        # "end_date={{next_ds}}"
    ),
    dag=dag,
)
def _calculate_stats(input_path, output_path):
    Path(output_path).parent.mkdir(exist_ok=True)

    events= pd.read_json(input_path)
    stats = events.groupby(['date', 'user']).size().reset_index()

    stats.to_csv(output_path, index=False)

calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    op_kwargs={"input_path":"/tmp/data/events_{{ds}}.json", "output_path":"/tmp/data/output_{{ds}}.csv"},
    dag=dag,
)

fetch_events >> calculate_stats