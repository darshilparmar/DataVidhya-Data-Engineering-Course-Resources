import datetime as dt
from pathlib import Path 

import pandas as pd 

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator 

dag = DAG(
    dag_id="atomic_idem",
    # schedule_interval="@daily"
    start_date=dt.datetime(year=2024, month=6, day=7),
    # end_date=dt.datetime(year=2024, month=6, day=7)
    catchup=True
)

fetch_events = BashOperator(
    task_id = "fetch_events",
    bash_command=(
        "curl -o /tmp/data/events_{{ds}}.json http://events_api:5000/events?"
        "start_date=2024-06-06&"
        "end_date=2024-06-07"
    ),
    dag=dag,
)


def __calculate_stats(**context):
    input_path = context['templates_dict']["input_path"]
    output_path = context['templates_dict']["output_path"]

    events = pd.read_json(input_path)
    stats = events.groupby(['date', 'user']).size().reset_index()

    Path(output_path).parent.mkdir(exist_ok=True)
    stats.to_csv(output_path, index=False)


calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=__calculate_stats,
    templates_dict={
        "input_path": "/tmp/data/events_{{ds}}.json",
        "output_path":"/tmp/data/output_{{ds}}.csv"
    },
    dag=dag,
)

def email_stats(stats, email):
    """Send an email..."""
    print(f"Sending stats to {email}")

def _send_stats(email, **context):
    stats = pd.read_csv(context['templates_dict']["output_path"])
    email_stats(stats, email=email)

send_stats = PythonOperator(
    task_id='send_stats',
    python_callable=_send_stats,
    op_kwargs={"email":"darshil@gmail.com"},
    templates_dict={
        "output_path":"/tmp/data/output_{{ds}}.csv"
    },
    dag=dag

)

fetch_events >> calculate_stats >> send_stats



    

