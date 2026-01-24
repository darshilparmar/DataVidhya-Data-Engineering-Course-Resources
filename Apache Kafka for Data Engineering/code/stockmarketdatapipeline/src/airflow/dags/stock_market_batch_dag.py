import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "darshil",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

#Define DAG
dag = DAG(
    "stock_market_batch_pipeline",
    default_args=default_args,
    description="Stock Market Data Pipeline",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 4, 1),
    catchup=False,
)

# Task to fetch historical data
fetch_historical_data = BashOperator(
    task_id="fetch_historical_data",
    bash_command="python /opt/airflow/dags/scripts/batch_data_producer.py {{ ds }}",
    dag=dag,
)

# Task to fetch historical data
consume_historical_data = BashOperator(
    task_id="fetch_historical_data",
    bash_command="python /opt/airflow/dags/scripts/batch_data_consumer.py {{ ds }}",
    dag=dag,
)

process_data = BashOperator(
    task_id="process_data",
    bash_command="""
    docker exec stockmarketdatapipeline_spark-master_1 \
        spark-submit \
            --master spark://spark-master:7077 \
            --packages org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901 \
                /opt/spark/jobs/spark_batch_processor.py {{ ds }}

    """,
    dag=dag,
)

load_to_snowflake = BashOperator(
    task_id="load_historical_to_snowflake",
    bash_command = "python /opt/airflow/dags/scripts/load_to_snowflake.py {{ ds }}",
    dag=dag
)

process_complete = BashOperator(
    task_id="process_complete",
    bash_command="""
    echo "Batch Process for time {{ ds }} is complete"
    """
)
fetch_historical_data >> consume_historical_data >> process_data >> load_to_snowflake >> process_complete
