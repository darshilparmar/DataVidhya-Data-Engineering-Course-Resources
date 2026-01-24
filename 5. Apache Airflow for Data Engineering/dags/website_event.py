from datetime import datetime
from pathlib import Path 

import pandas as pd 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


# dag = DAG(
#     dag_id="01_unscheduled",
#     start_date=datetime(2019, 1, 1),
#     schedule_interval=None
# )

dag = DAG(
    dag_id="02_daily_schedule",
    start_date=datetime(2024, 1, 6),
    schedule_interval='@hourly'
)

# dag = DAG(
#     dag_id="03_with_end_date",
#     start_date=datetime(2019, 1, 1),
#     end_date=datetime(2019,1,5),
#     schedule_interval='@daily'
# )

# dag = DAG(  
#     dag_id="04_time_delta", 
#     schedule_interval=dt.timedelta(days=3),
#     start_date=dt.datetime(year=2019, month=1, day=1), 
#     end_date=dt.datetime(year=2019, month=1, day=5),
# )

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
        "curl -o /tmp/data/events.json http://events_api:5000/events" 
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
    op_kwargs={"input_path":"/tmp/data/events.json", "output_path":"/tmp/data/output.csv"},
    dag=dag,
)

fetch_events >> calculate_stats