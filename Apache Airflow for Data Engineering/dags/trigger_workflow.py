import airflow.utils.dates
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.python import PythonSensor
from pathlib import Path
from airflow.operators.dummy import DummyOperator
import os

dag = DAG(
    dag_id = "trigger_workflow",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="0 16 * * *",
    description="A batch workflow for ingesting supermarket promotions data, demonstrating the FileSensor.",
    default_args={"depeneds_on_past": True},
)


create_metrics = DummyOperator(task_id="create_metrics", dag=dag)


def _wait_for_supermarket(supermarket_id_):
    supermarket_path = Path("/tmp/market_data/" + str(supermarket_id_))
    print(os.path.realpath('.'))
    #collect data-*.csv files
    data_files = supermarket_path.glob("data-*.csv")
    print(data_files)
    #collect _SUCCESS file
    success_file = supermarket_path / "_SUCCESS"
    print(success_file)
    print(success_file.exists())
    #return whatvever both data and sucess file exists
    return data_files and success_file.exists()



# wait_for_supermarket_1 = PythonSensor(
#     task_id="wait_for_supermarket_1",
#     python_callable=_wait_for_supermarket,
#     op_kwargs={"supermarket_id_": "supermarket1"},
#     dag=dag,
# )

# wait_for_supermarket_1 >> create_metrics


for supermaket_id in range(1, 5):
    wait = PythonSensor(
        task_id=f"wait_for_supermarket_{supermaket_id}",
        python_callable=_wait_for_supermarket,
        op_kwargs={"supermarket_id_": f"supermarket{supermaket_id}"},
        timeout=600,
        dag=dag,
    )
    copy = DummyOperator(task_id=f"copy_to_raw_supermarket_{supermaket_id}", dag=dag)
    process = DummyOperator(task_id=f"process_supermarket_{supermaket_id}", dag=dag)

    wait >> copy >> process >> create_metrics

# wait = FileSensor(
#     task_id="wait_for_supermarket_1",
#     filepath="tmp/market_data/supermarket1/data.csv",
#     dag=dag
# )

# wait
