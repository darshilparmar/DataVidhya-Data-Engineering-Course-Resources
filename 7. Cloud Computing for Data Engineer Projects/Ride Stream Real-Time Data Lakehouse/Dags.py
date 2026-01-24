from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.utils.dates import days_ago

# 🔹 Replace with your details
EMR_SERVERLESS_APP_ID = "00fvktfvpj62n509"
JOB_ROLE_ARN = "arn:aws:iam::640958509818:role/service-role/AmazonEMR-ExecutionRole-1757850783950"
S3_SCRIPT_PATH = "s3://aws-glue-assets-640958509818-us-east-1/glue/spark_job.py"
S3_OUTPUT_PATH = "s3://aws-glue-assets-640958509818-us-east-1/output/"
S3_LOGS_PATH = "s3://aws-glue-assets-640958509818-us-east-1/emr-logs/"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="emr_serverless_taxi_etl",
    default_args=default_args,
    description="Run EMR Serverless Spark Job for Taxi ETL",
    schedule_interval="@daily",   # 🔹 change to None if you want manual only
    start_date=days_ago(1),
    catchup=False,
    tags=["emr", "spark", "etl"],
) as dag:

    run_emr_etl = EmrServerlessStartJobOperator(
        task_id="run_taxi_etl",
        application_id=EMR_SERVERLESS_APP_ID,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": S3_SCRIPT_PATH,
                "entryPointArguments": [
                    "--output_path", S3_OUTPUT_PATH
                ],
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": S3_LOGS_PATH
                }
            }
        },
    )

    run_emr_etl
