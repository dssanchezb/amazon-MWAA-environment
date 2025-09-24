"""DAG CO_DELFOS_CLIENTES_PRN_MODELO_BI_EMPRENDEDORES_ME01"""
import logging
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.glue_catalog import GlueCatalogHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.opsgenie.operators.opsgenie import OpsgenieCreateAlertOperator
from airflow.providers.amazon.aws.operators.sagemaker import SageMakerProcessingOperator
from airflow.providers.amazon.aws.sensors.sagemaker import SageMakerBaseSensor
from airflow.providers.amazon.aws.hooks.sagemaker import SageMakerHook
from typing import Sequence
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.s3 import (
    S3DeleteObjectsOperator,
)
from airflow.providers.ssh.operators.ssh import SSHOperator

DAG_ID = "CO_DELFOS_CLIENTES_PRN_MODELO_BI_EMPRENDEDORES_ME01"
DAG_DESCRIPTION = "Este dag ejecuta el proceso de identificacion de emprendedores"
EXECUTION_DAY = 2
DAG_SCHEDULE = f"59 23 {EXECUTION_DAY} * *"
BASE_DATE = f"(data_interval_end - macros.timedelta(days={EXECUTION_DAY}))"
EXECUTION_DATE_YEAR = f"{{{{ {BASE_DATE}.strftime('%Y') }}}}"
EXECUTION_DATE_MONTH = f"{{{{ {BASE_DATE}.strftime('%m') }}}}"
ENV = Variable.get("env")
DOMAIN = "clientes"
SUBDOMAIN = "prn"
COUNTRY = "co"
AWS_GLUE_CONNECTION_ID = "co-clientes-transversal-gl-aws"
AWS_SM_CONNECTION_ID = "co-clientes-transversal-sm-aws"
OPSGENIE_CONNECTION_ID = "opsgenie-dataops"

if ENV == "dev":
    ACCOUNT_NUMBER = '460311739291'
elif ENV == "qa":
    ACCOUNT_NUMBER = '465924778008'
elif ENV == "pdn":
    ACCOUNT_NUMBER = '158304509201'

MODEL_IMAGE = "modelo_emprendedores_nx"
MODEL_IMAGE_ECR_URI = f"{ACCOUNT_NUMBER}.dkr.ecr.us-east-1.amazonaws.com/{MODEL_IMAGE}:latest" # TODO: Use the right tag
BUCKET = 'nequi-data'
ATHENA_BUCKET = 'nequi-data'
OUTPUT_ATHENA_QUERY = f's3://{ATHENA_BUCKET}/sandbox_co/ejguerra/emprendedores_grafo/query_athena_results/'
RAW_DATABASE = 'nequi_co'
OUTPUT_UNLOAD_PREFIX = "sandbox_co/ejguerra/emprendedores_grafo/modelo_emprendedores_data_input/"
PROCESSING_JOB_OUTPUT_PREFIX = (
    "prn/modelo_emprendedores/modelo_emprendedores_data_inference/"
    + f"year={EXECUTION_DATE_YEAR}/month={EXECUTION_DATE_MONTH}/"
)

QUERY_UNLOAD_TRANSACCIONAL = f"""
UNLOAD (
SELECT
    numero_cuenta_contraparte as cuenta_beneficiario,
    numero_producto as numero_cuenta_cliente,
    codigo_concepto,
    valor_transaccion,
    tran_info,
    year,
    month
FROM {RAW_DATABASE}.finacle_transaccional_uso
WHERE naturaleza_transaccion = 'CREDITO' and codigo_concepto in ('R004', 'T001')
and year = '{EXECUTION_DATE_YEAR}' and month = '{EXECUTION_DATE_MONTH}')
TO 's3://{BUCKET}/{OUTPUT_UNLOAD_PREFIX}'
WITH (format = 'PARQUET')
"""

QUERY_ADD_PARTITION = f"""
ALTER TABLE {RAW_DATABASE}.modelo_emprendedores_data_inference 
ADD IF NOT EXISTS PARTITION (year='{EXECUTION_DATE_YEAR}', month='{EXECUTION_DATE_MONTH}') 
location 's3://{BUCKET}/{PROCESSING_JOB_OUTPUT_PREFIX}'
"""

TAGS = [
    f"env.{ENV.upper()}",
    "teams.DELFOS",
    "dominio.GESTIONFRAUDE",
    "subdominio.FRE",
    "pais.CO",
    "criticidad.P3",
]
SLACK_CHANNEL = f"airflow-data-notifications-{ENV}"
DEFAULT_ARGS = {
    "owner": "INTELIGENCIA_DE_NEGOCIO",
    "start_date": datetime(2024, 5, 1),
}

task_logger = logging.getLogger("airflow.task")

def send_error_message_on_slack(context):
    """Send a failure message over slack

    Send a custom message to a slack channel base
    on a context variable.
    """
    error_message: str = f"""
        Ha ocurrio un error procesando la inferencia del modelo de churn:
            dag: {context.get("task_instance").dag_id}
            tarea: {context.get("task_instance").task_id}
            url del log: {context.get('task_instance').log_url}
    """
    error_message_on_slack = SlackWebhookOperator(
        task_id="error_message_on_slack",
        slack_webhook_conn_id=f"slack-{SLACK_CHANNEL}",
        message=error_message,
    )
    return error_message_on_slack.execute(context=context)

def send_error_message_on_opsgenie(context):
    """Send a failure message over opsgenie

    Send a custom message over opsenie base on a
    context variable.
    """
    error_header: str = (
        f"""Error de ejecución en el DAG {context.get("task_instance").dag_id}"""
    )

    error_message: str = f"""
        Ha ocurrio un error:
            dag: {context.get("task_instance").dag_id}
            tarea: {context.get("task_instance").task_id}
            url del log: {context.get('task_instance').log_url}
    """
    error_message_on_opsgenie = OpsgenieCreateAlertOperator(
        opsgenie_conn_id=OPSGENIE_CONNECTION_ID,
        task_id="error_message_on_opsgenie",
        message=error_header,
        description=error_message,
    )
    return error_message_on_opsgenie.execute(context=context)

def orchestrate_error_message(context):
    """Orchestrates the sending of error messages

    Handle the error messages over slack and Opsgenie.
    """
    send_error_message_on_slack(context)
    send_error_message_on_opsgenie(context)

@task(task_id="glue_catalog_valida_particion")
def validate_partition(database, tablename, expression, aws_conn_id):
    """Creates a Airflow Task using the GlueCatalogHook.

    Args:
        database (str): Name of the Glue Database.
        tablename (str): Name of the Glue Table.
        expression (str): Expression to filter the partition of a table.
        aws_conn_id (str): AWS Connection ID.

    """
    hook = GlueCatalogHook(aws_conn_id=aws_conn_id)
    validation = hook.check_for_partition(database, tablename, expression)

    task_logger.info(f"Validation result: {validation}")
    task_logger.info(f"Validation expression: {expression}")
    if not validation:
        raise ValueError(f"The Data Source {database}.{tablename} is NOT up to date.")
    task_logger.info(f"The Data Source {database}.{tablename} is up to date.")

def generate_processing_input(input_data_uri: str, processing_local_input_path: str) -> dict:
    """Generates the processing input dictionary for a given input data URI and 
    processing local input path.

    Args:
        input_data_uri (str): The URI of the input data.
        processing_local_input_path (str): The local path where the input data will be stored.

    Returns:
        dict: The processing input dictionary containing the input name, S3 input details, 
        and other properties.

    """
    return {
        "InputName": "input",
        "AppManaged": False,
        "S3Input": {
            "S3Uri": input_data_uri,
            "LocalPath": processing_local_input_path,
            "S3DataType": "S3Prefix",
            "S3InputMode": "File",
            "S3DataDistributionType": "FullyReplicated",
            "S3CompressionType": "None",
        },
    }

def generate_processing_output(output_data_uri: str, processing_local_output_path: str) -> dict:
    """Generates the processing output dictionary for a given output data URI 
    and processing local output path.

    Args:
        output_data_uri (str): The URI of the output data.
        processing_local_output_path (str): The local path where the output
        data will be stored.

    Returns:
        dict: The processing output dictionary containing the output name,
        S3 output details, and other properties.

    """
    return {
        "OutputName": "output",
        "S3Output": {
            "S3Uri": output_data_uri,
            "LocalPath": processing_local_output_path,
            "S3UploadMode": "EndOfJob",
        },
        "AppManaged": False,
    }

def processing_config(
    processing_job_name: str,
    inputs: list,
    outputs: list,
    instance_count: int,
    instance_type: str,
    volume_size_in_gb: int,
    ecr_repository_uri: str,
    aws_conn_id: str,
) -> dict:
    """Generates a configuration dictionary for a processing job.

    Args:
        processing_job_name (str): The name of the processing job.
        inputs (list): A list of input configurations for the processing job.
        outputs (list): A list of output configurations for the processing job.
        instance_count (int): The number of instances to use for the processing job.
        instance_type (str): The type of instance to use for the processing job.
        volume_size_in_gb (int): The size of the volume in gigabytes for the processing job.
        ecr_repository_uri (str): The URI of the ECR repository containing the
        image for the processing job.
        aws_conn_id (str): AWS Connection ID.

    Returns:
        dict: A dictionary containing the configuration for the processing job.

    """
    return {
        'RoleArn': (
            BaseHook.get_connection(aws_conn_id)
            .extra_dejson.get("role_arn", None)
        ),
        "ProcessingJobName": processing_job_name,
        "ProcessingInputs": inputs,
        "ProcessingOutputConfig": {"Outputs": outputs},
        "ProcessingResources": {
            "ClusterConfig": {
                "InstanceCount": instance_count,
                "InstanceType": instance_type,
                "VolumeSizeInGB": volume_size_in_gb,
            }
        },
        "StoppingCondition": {"MaxRuntimeInSeconds": 86400},
        "AppSpecification": {
            "ImageUri": ecr_repository_uri,
        }
    }

class SageMakerProcessingSensor(SageMakerBaseSensor):
    """Poll the processing job until it reaches a terminal state; 
    raise AirflowException with the failure reason.

    :param job_name: Name of the processing job to watch.

    """

    template_fields: Sequence[str] = ("job_name",)
    template_ext: Sequence[str] = ()

    def __init__(self, *, job_name, **kwargs):
        super().__init__(**kwargs)
        self.job_name = job_name
        self.positions = {}
        self.stream_names = []
        self.instance_count: int | None = None
        self.state: int | None = None
        self.last_description = None
        self.last_describe_job_call = None

    def non_terminal_states(self):
        return SageMakerHook.non_terminal_states

    def failed_states(self):
        return SageMakerHook.failed_states

    def get_sagemaker_response(self):
        self.last_description = self.hook.describe_processing_job(self.job_name)
        status = self.state_from_response(self.last_description)
        if (status not in self.non_terminal_states()) and (status not in self.failed_states()):
            billable_time = (
                self.last_description["ProcessingEndTime"] -
                self.last_description["ProcessingStartTime"]
            ) * self.last_description["ProcessingResources"]["ClusterConfig"]["InstanceCount"]
            self.log.info("Billable seconds: %s", (int(billable_time.total_seconds()) + 1))
        return self.last_description

    def get_failed_reason_from_response(self, response):
        return response["FailureReason"]

    def state_from_response(self, response):
        return response["ProcessingJobStatus"]

@dag(
    dag_id=DAG_ID,
    description=DAG_DESCRIPTION,
    default_args=DEFAULT_ARGS,
    schedule_interval=DAG_SCHEDULE,
    catchup=False,
    tags=TAGS,
    on_success_callback=orchestrate_error_message,
    on_failure_callback=orchestrate_error_message,
    params={"reprocess": True},
)
def workflow():
    """Executes the workflow for the CO_DELFOS_CLIENTES_PRN_MODELO_BI_EMPRENDEDORES_ME01 DAG.

    This function defines the tasks and their dependencies for the DAG workflow. 
    It consists of two TaskGroups:
    - ingestion_tasks: Contains tasks related to data ingestion and processing.
    - prediction_tasks: Contains tasks related to prediction and model processing.

    Task Dependencies:
    - The ingestion_tasks are executed sequentially, with each task depending on
    the successful completion of the previous task.
    - The prediction_tasks are executed after the ingestion_tasks have completed 
    successfully.

    """
    task_start = EmptyOperator(task_id="start_process")

    with TaskGroup("ingestion_tasks") as ingestion_tasks:
        validation_task = validate_partition(
            database=RAW_DATABASE,
            tablename="finacle_transaccional_uso",
            expression=(
                f"year = '{EXECUTION_DATE_YEAR}' and month = '{EXECUTION_DATE_MONTH}'"
            ),
            aws_conn_id=AWS_GLUE_CONNECTION_ID,
        )

        summit_purge_s3_folder = S3DeleteObjectsOperator(
            task_id="summit_purge_s3_folder",
            bucket=BUCKET,
            prefix=OUTPUT_UNLOAD_PREFIX,
            aws_conn_id=AWS_GLUE_CONNECTION_ID,
            execution_timeout=timedelta(seconds=60),
        )

        athena_unload_transaccional = AthenaOperator(
            task_id='athena_unload_transaccional',
            #workgroup="delfos",
            query=QUERY_UNLOAD_TRANSACCIONAL,
            database=RAW_DATABASE,
            output_location=OUTPUT_ATHENA_QUERY,
            aws_conn_id=AWS_GLUE_CONNECTION_ID,
            execution_timeout=timedelta(seconds=20*60),
            sleep_time=60,
        )

        await_for_s3_input_presence = S3KeySensor(
            task_id="await_for_s3_input_presence",
            bucket_name=BUCKET,
            bucket_key=f"{OUTPUT_UNLOAD_PREFIX}*",
            wildcard_match=True,
            aws_conn_id=AWS_GLUE_CONNECTION_ID,
            timeout=30 * 60,
            mode="reschedule",
            poke_interval=5 * 60,
        )

        validation_task >> summit_purge_s3_folder
        summit_purge_s3_folder >> athena_unload_transaccional
        athena_unload_transaccional  >> await_for_s3_input_presence

    with TaskGroup("prediction_tasks") as prediction_tasks:
        summit_processing_job = SageMakerProcessingOperator(
            task_id="summit_processing_job",
            config=processing_config(
                processing_job_name=f"pj-emprendedores-{ENV}",
                inputs=[
                    generate_processing_input(
                        input_data_uri=f"s3://{BUCKET}/{OUTPUT_UNLOAD_PREFIX}",
                        processing_local_input_path="/opt/ml/processing/input"
                    ),
                ],
                outputs=[
                    generate_processing_output(
                        output_data_uri=f"s3://{BUCKET}/{PROCESSING_JOB_OUTPUT_PREFIX}",
                        processing_local_output_path="/opt/ml/processing/output"
                    )
                ],
                instance_count=1,
                instance_type="ml.m5.24xlarge",
                volume_size_in_gb=100,
                ecr_repository_uri=MODEL_IMAGE_ECR_URI,
                aws_conn_id=AWS_SM_CONNECTION_ID,
            ),
            aws_conn_id=AWS_SM_CONNECTION_ID,
            wait_for_completion=False,
        )

        xcom_expression = (
            str(summit_processing_job.output)
            .replace("{{ ", "")
            .replace(" }}", "")
        )

        await_for_preprocess_data_to_completed = SageMakerProcessingSensor(
            task_id=f"await_preprocess_data_to_finish",
            job_name=(
                f"{{{{ {xcom_expression}['Processing']['ProcessingJobName'] }}}}"
            ),
            aws_conn_id=AWS_SM_CONNECTION_ID,
            timeout=60 * 60,
            mode="reschedule",
            poke_interval=10 * 60,
        )

        await_for_s3_preprocess_presence = S3KeySensor(
            task_id="await_for_s3_preprocess_presence",
            bucket_name=BUCKET,
            bucket_key=f"{PROCESSING_JOB_OUTPUT_PREFIX}*",
            wildcard_match=True,
            aws_conn_id=AWS_SM_CONNECTION_ID,
            timeout=30 * 60,
            mode="reschedule",
            poke_interval=5 * 60,
        )

        summit_processing_job >> await_for_preprocess_data_to_completed
        await_for_preprocess_data_to_completed >> await_for_s3_preprocess_presence

    with TaskGroup("postprocessing_tasks") as postprocessing_tasks:
        athena_add_partition_inference_table = AthenaOperator(
            task_id='athena_add_partition_inference_table',
            workgroup="delfos",
            query=QUERY_ADD_PARTITION,
            database=RAW_DATABASE,
            output_location=OUTPUT_ATHENA_QUERY,
            aws_conn_id=AWS_GLUE_CONNECTION_ID,
            execution_timeout=timedelta(seconds=20*60),
            sleep_time=60,
        )

        dbt_redshift_run_model = SSHOperator(
            task_id="dbt_redshift_run_model", 
            ssh_conn_id="",
            command="",
            conn_timeout = 60,
            cmd_timeout = 10 * 60, # TODO: Check this value
            do_xcom_push= True
        )

        dbt_athena_run_model = SSHOperator(
            task_id="dbt_athena_run_model", 
            ssh_conn_id="",
            command="",
            conn_timeout = 60,
            cmd_timeout = 10 * 60, # TODO: Check this value
            do_xcom_push= True
        )

        athena_add_partition_inference_table >> dbt_redshift_run_model
        athena_add_partition_inference_table >> dbt_athena_run_model

    task_end = EmptyOperator(task_id="end_process")

    task_start >> ingestion_tasks >> prediction_tasks >> postprocessing_tasks >> task_end

workflow()