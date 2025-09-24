import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.s3 import (
    S3DeleteObjectsOperator,
)

AWS_CONNECTION_ID = "co-clientes-transversal-gl-aws"
task_logger = logging.getLogger("airflow.task")

with DAG(
    dag_id="LOCAL_TESTING_DAG",
    start_date=datetime(2023, 1, 11),
    catchup=False,
    schedule_interval="59 23 * * *",
    params={"reprocess": True},
) as dag:
    await_for_s3_input_presence = S3KeySensor(
        task_id="await_for_s3_input_presence",
        aws_conn_id=AWS_CONNECTION_ID,
        bucket_name="nequi-data",
        bucket_key="sandbox_co/ejguerra/{{ execution_date }}/{{ next_execution_date }}/{{ data_interval_start }}/{{ data_interval_end }}",
        wildcard_match=True,
        timeout=5,
        mode="poke",
        poke_interval=2,
    )

    summit_purge_s3_folder = S3DeleteObjectsOperator(
        task_id="summit_purge_s3_folder",
        bucket="nequi-data",
        prefix="sandbox_co/ejguerra/sandbox/mv_files/day=01/file-002.parquet",
        aws_conn_id=AWS_CONNECTION_ID,
        execution_timeout=timedelta(seconds=60),
    )

    with TaskGroup("execution_tasks") as execution_tasks:
        @task(task_id="main_execution_task")
        def main_execution_task(date: str):
            task_logger.info("This is the execution of the main task")
            task_logger.info(f"This is the Date: {date}")

            return 0

        @task(task_id="backup_execution_task")
        def backup_execution_task():
            task_logger.info("This is the execution of the backup task")

            return 0

        @task.branch(task_id=f"choosing_execution_task")
        def decide_which_task(**kwargs):
            task_instance = kwargs['ti']
            previous_task_state = task_instance.xcom_pull(
                task_ids='execution_tasks.main_execution_task', key='return_value'
            )

            task_logger.info(f"The latest state of the main task is [{previous_task_state}]")

            if not previous_task_state:
                return 'execution_tasks.main_execution_task'
            else:
                return 'execution_tasks.backup_execution_task'


        decide_which_task() >> [main_execution_task(date="{{data_interval_end.strftime('%Y-%m-%d')}}"), backup_execution_task()]


    await_for_s3_input_presence >> summit_purge_s3_folder >> execution_tasks
