from airflow.sdk import dag, task
from pendulum import datetime
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

@dag(
    dag_id="send_to_S3",
    start_date=datetime(year=2026, month=2, day=11, tz="Asia/Kolkata"),
    is_paused_upon_creation=False,
    catchup=True,
    tags=["aws", "s3", "local_to_s3"]
)
def send_to_s3():

    pc_to_s3= LocalFilesystemToS3Operator(
        task_id="Upload_to_S3",
        filename="",
        dest_key="raw/pc_to_s3.txt",
        dest_bucket="localpc-airflow",
        aws_conn_id="arn:aws:iam::673538734257:role/airflow"
    )