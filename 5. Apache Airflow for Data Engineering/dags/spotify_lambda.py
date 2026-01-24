from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.glue import GlueJobHook
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 19),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag =  DAG('lambda_trigger_dag',
         default_args=default_args,
         description='DAG to trigger Lambda functions and check S3 upload',
         schedule_interval=timedelta(days=1),
         catchup=False) 

# Task to invoke the first Lambda function
trigger_first_lambda = LambdaInvokeFunctionOperator(
    task_id='trigger_extract_lambda',
    function_name='spotify_data_extract',
    aws_conn_id='aws_s3_airbnb',
    region_name="ap-south-1",
    dag=dag,
)

# Task to check if data is uploaded to S3
check_s3_upload = S3KeySensor(
    task_id='check_s3_upload',
    bucket_key='s3://spotify-etl-project-darshil/raw_data/to_processed/*',
    wildcard_match=True,
    aws_conn_id='aws_s3_airbnb',
    timeout=60 * 60,  # wait for up to 1 hour
    poke_interval=60,  # check every 60 seconds
    dag=dag,
)

# # Task to invoke the second Lambda function
# trigger_second_lambda = LambdaInvokeFunctionOperator(
#     task_id='trigger_transformation_lambda',
#     function_name='spotify_data_transfrom_load',
#     aws_conn_id='aws_s3_airbnb',
#     region_name="ap-south-1",
#     dag=dag,
# )


trigger_glue_job = GlueJobOperator(
        task_id='trigger_glue_job',
        job_name='spotify_transformation_job',
        script_location='s3://aws-glue-assets-206986907456-ap-south-1/scripts/spotify_transformation_job.py',  # Replace with the location of your Glue script
        aws_conn_id='aws_s3_airbnb',
        region_name='ap-south-1',  # Replace with your AWS region
        iam_role_name='spotify_glue_iam_role',  # Replace with the IAM role for Glue
        s3_bucket='aws-glue-assets-206986907456-ap-south-1',  # Replace with the S3 bucket for Glue temporary data
    )

# trigger_first_lambda >> check_s3_upload >> trigger_second_lambda
trigger_first_lambda >> check_s3_upload >> trigger_glue_job
