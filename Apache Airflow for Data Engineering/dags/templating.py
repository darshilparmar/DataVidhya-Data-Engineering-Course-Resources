import airflow.utils.dates
from airflow import DAG
from airflow.operators.python import PythonOperator
import uuid
from airflow.operators.dummy import DummyOperator

from airflow.decorators import task

with DAG(
    dag_id="task_flow_api",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@daily",
) as dag:
    
    start = DummyOperator(task_id="start")

    fetch_sales = DummyOperator(task_id="fetch_sales")
    clean_sales = DummyOperator(task_id="clean_sales")

    fetch_weather = DummyOperator(task_id="fetch_weather")
    clean_weather = DummyOperator(task_id="clean_weather")

    join_datasets = DummyOperator(task_id="join_datasets")

    start >> [fetch_sales, fetch_weather]
    fetch_sales >> clean_sales
    fetch_weather >> clean_weather
    [clean_sales, clean_weather] >> join_datasets

    @task
    def train_model():
        "logic"
        model_id = str(uuid.uuid4())
        return model_id

    @task
    def deploy_model(model_id: str):
        print(f"Deploying model {model_id}")

    model_id = train_model()
    deploy_model(model_id)

    join_datasets >> model_id

# dag = DAG(
#     dag_id="templating",
#     start_date=airflow.utils.dates.days_ago(3),
#     schedule_interval="@daily",
# )

# def _print_context(**kwargs):
#     print(kwargs)


# print_context = PythonOperator(
#     task_id="print_context", python_callable=_print_context, dag=dag
# )

