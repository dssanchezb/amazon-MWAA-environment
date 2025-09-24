"""DAG CO_DELFOS_CLIENTES_PRN_GENDER_MODEL_DI01"""
import sys
import logging
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
from dbt_delfos_operator import DBTDelfosOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.hooks.glue_catalog import GlueCatalogHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.amazon.aws.sensors.sagemaker import SageMakerTransformSensor
from airflow.providers.opsgenie.operators.opsgenie import OpsgenieCreateAlertOperator
from airflow.providers.amazon.aws.operators.sagemaker import SageMakerTransformOperator
from airflow.providers.amazon.aws.operators.s3 import (
    S3DeleteObjectsOperator,
)

DAG_ID = "CO_DELFOS_CLIENTES_PRN_GENDER_MODEL_DI01"
DAG_DESCRIPTION = "Este dag ejecuta el proceso de identificación de genero"
DAG_SCHEDULE = "00 12  * * *"
BASE_DATE = "(execution_date - macros.timedelta(days=1))"
BASE_DATE_N_1 = "(execution_date - macros.timedelta(days=2))"
ENV = Variable.get("env")
DOMAIN = "clientes"
SUBDOMAIN = "prn"
COUNTRY = "co"
AWS_GLUE_CONNECTION_ID = f"{COUNTRY}-clientes-transversal-gl-aws"
AWS_SM_CONNECTION_ID = f"{COUNTRY}-clientes-transversal-sm-aws"
OPSGENIE_CONNECTION_ID = "opsgenie-dataops"
DBT_CONNECTION_ID = "ssh-delfos-datakitchen"

if ENV == "dev":
    ACCOUNT_NUMBER = "460311739291"
    ENV_MESH = "dev"
    RAW_KMS_ID = "549a76fb-c919-4fb2-bd34-a8993aa150f8"
elif ENV == "qa":
    ACCOUNT_NUMBER = "465924778008"
    ENV_MESH = "qc"
    RAW_KMS_ID = "62ee6c67-1824-47e6-893b-b3e98ab48ec7"
elif ENV == "pdn":
    ACCOUNT_NUMBER = "158304509201"
    ENV_MESH = "pdn"
    RAW_KMS_ID = "6e0c1382-170f-4084-af5b-e6869481fbd1"

BUCKET = f"{COUNTRY}-delfos-{DOMAIN}-raw-{ACCOUNT_NUMBER}-{ENV_MESH}"
ATHENA_BUCKET = f"{COUNTRY}-delfos-{DOMAIN}-athena-{ACCOUNT_NUMBER}-{ENV_MESH}"
ARTIFACTS_BUCKET = f"{COUNTRY}-delfos-{DOMAIN}-artifacts-{ACCOUNT_NUMBER}-{ENV_MESH}"
RAW_DATABASE = f"{COUNTRY}_delfos_{DOMAIN}_raw_{ENV_MESH}_rl"
MODEL_INPUT_DATABASE = f"{COUNTRY}_delfos_{DOMAIN}_analytics_{ENV_MESH}_rl"
OUTPUT_UNLOAD_PREFIX = f"{SUBDOMAIN}/gender_model/gender_model_data_input/"
OUTPUT_ATHENA_QUERY = f"s3://{ATHENA_BUCKET}f/{SUBDOMAIN}/query_athena_results/"
OUTPUT_PROCESSING_PREFIX = f"{SUBDOMAIN}/gender_model/gender_model_data_processing/"
INFERENCE_OUTPUT_KEY = (
    f"{SUBDOMAIN}/gender_model/gender_model_data_inference_v1/"
    + f"year={{{{ {BASE_DATE}.strftime('%Y') }}}}/"
    + f"month={{{{ {BASE_DATE}.strftime('%m') }}}}/"
    + f"day={{{{ {BASE_DATE}.strftime('%d') }}}}/"
)

QUERY_ADD_PARTITION = f"""
ALTER TABLE {RAW_DATABASE}.gender_model_data_inference_v1
ADD IF NOT EXISTS PARTITION (
    year='{{{{ {BASE_DATE}.strftime('%Y') }}}}', 
    month='{{{{ {BASE_DATE}.strftime('%m') }}}}', 
    day='{{{{ {BASE_DATE}.strftime('%d') }}}}'
)
location 's3://{BUCKET}/{INFERENCE_OUTPUT_KEY}'
"""

QUERY_UNLOAD_MODEL_INPUT = f"""
UNLOAD (
    SELECT *
    FROM {MODEL_INPUT_DATABASE}.gender_model_data_input
)
TO 's3://{BUCKET}/{OUTPUT_UNLOAD_PREFIX}'
WITH (
    format = 'TEXTFILE',
    field_delimiter = ',',
    compression = 'NONE'
)
"""

TAGS = [
    f"env.{ENV.upper()}",
    "teams.DELFOS",
    f"dominio.{DOMAIN.upper()}",
    f"subdominio.{SUBDOMAIN.upper()}",
    f"pais.{COUNTRY.upper()}",
    "criticidad.P3",
]
SLACK_CHANNEL = f"airflow-data-notifications-{ENV}"
DEFAULT_ARGS = {
    "owner": "INTELIGENCIA_DE_NEGOCIO",
    "start_date": datetime(2020, 1, 1),
}

task_logger = logging.getLogger("airflow.task")

def send_error_message_on_slack(context):
    """Send a failure message over slack

    Send a custom message to a slack channel base
    on a context variable.
    """
    error_message: str = f"""
        Ha ocurrio un error procesando la inferencia del modelo de genero:
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

def get_validation_operator(
    task_id: str, database: str, tablename: str, aws_conn_id: str
) -> PythonOperator:
    """Create a Operator that validates the partition of a Data Source.

    Args:
        task_id (str): ID of the task.
        database (str): Name of the Database of the Data Source.
        tablename (str): Name of the table of the Data Source.
        aws_conn_id (str): AWS Connection ID.

    Returns:
        (task): Airflow task that uses the GlueCatalogHook.

    """

    @task(task_id=task_id)
    def validation_operator(expression: str):
        """Creates a Airflow Task using the GlueCatalogHook.

        Args:
            expression (str): Expression to filter the partition of a table.

        """
        hook = GlueCatalogHook(aws_conn_id=aws_conn_id)

        validation = hook.check_for_partition(database, tablename, expression)

        if not validation:
            task_logger.error(
                f"The Data Source {database}.{tablename} DOESN'T fit the expression: {expression}"
            )
            sys.exit()
        task_logger.info(f"The Data Source {database}.{tablename} fit the expression: {expression}")

    return validation_operator

def transform_config(
    job_name: str,
    model_name: str,
    s3_input_path: str,
    s3_output_path: str,
    kms_id: str,
    input_filter: str,
    output_filter: str,
    join_source: str,
    content_type: str,
    instance_type: str = "ml.m5.xlarge",
    instance_count: int = 2,
    max_payload: int = 6,
) -> dict:
    """Get the configurations of a batch transform job.

    Create a batch transform configuration dict in order
    to execute a job using Airflow.

    Args:
        job_name (str): Name of the batch transform job.
        model_name (str): Name of the model to used.
        s3_input_path (str): Path of the input of the batch transform.
        s3_output_path (str): Path of the output of the batch transform.
        kms_id (str): ID of the KMS Key used to encrypt the data.
        input_filter (str): Input filter for the batch transform job.
        output_filter (str): Output filter for the batch transform job.
        join_source (str): Source of the join in the batch transform job.
        content_type (str): Type of the input file (csv or json).
        instance_type (str): Type of the instance used in the batch transform.
        instance_count (int): Number of instances used in the batch transform.
        max_payload (int): Max payload of the batch transform job.

    Returns:
        (dict): Dictionary with the Batch Transform Configurations.

    """
    return {
        "Transform": {
            "TransformJobName": job_name,
            "ModelName": model_name,
            "MaxConcurrentTransforms": 4,
            "MaxPayloadInMB": max_payload,
            "BatchStrategy": "MultiRecord",
            "TransformInput": {
                "DataSource": {
                    "S3DataSource": {
                        "S3DataType": "S3Prefix",
                        "S3Uri": s3_input_path,
                    }
                },
                "ContentType": content_type,
                "SplitType": "Line",
            },
            "TransformOutput": {
                "S3OutputPath": s3_output_path,
                "Accept": content_type,
                "AssembleWith": "Line",
                "KmsKeyId": kms_id,
            },
            "TransformResources": {
                "InstanceType": instance_type,
                "InstanceCount": instance_count,
            },
            "DataProcessing": {
                "InputFilter": input_filter,
                "OutputFilter": output_filter,
                "JoinSource": join_source,
            },
        }
    }

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
    """Define the main workflow for the Gender Model DAG.

    This DAG orchestrates the process of validating data sources, ingesting input data,
    preprocessing the model for gender prediction, performing model inference, and
    postprocessing the results. It consists of five main task groups:

    1. data_source_validation: Validates the presence and correctness of required data
        in the 'producto_hist' table for the current date and the previous day.

    2. input_ingestion: Prepares and loads the input data for the gender model.
        It includes running a DBT model, purging the S3 output folder, unloading data
        from Athena, and confirming the presence of the unloaded data in S3.

    3. model_preprocessing: Handles the preprocessing of data for the gender model.
        This includes purging the preprocessing S3 folder, submitting a SageMaker
        batch transform job for preprocessing, and waiting for the job to complete.

    4. model_inference: Executes the gender prediction model on the preprocessed data.
        This likely involves running a SageMaker batch transform job with the trained
        gender model and waiting for the inference results.

    5. model_postprocessing: Processes the model's output, potentially including tasks
        such as formatting the results, storing them in a database or S3 bucket, and
        generating any necessary reports or metrics.

    The workflow uses various Airflow operators including EmptyOperator, TaskGroup,
    DBTDelfosOperator, S3DeleteObjectsOperator, AthenaOperator, S3KeySensor,
    SageMakerTransformOperator, and SageMakerTransformSensor.

    Error handling is managed through the 'orchestrate_error_message' function,
    which is set as both the success and failure callback.

    Returns:
        None. The function defines the DAG structure but does not return any value.
    """
    task_start = EmptyOperator(task_id="start_process")

    with TaskGroup("data_source_validation") as data_source_validation:
        SOURCE_DICT = {
            "product_hist_n": f"""
            (
                year = '{{{{ {BASE_DATE}.strftime('%Y') }}}}' and
                month = '{{{{ {BASE_DATE}.strftime('%m') }}}}' and
                day = '{{{{ {BASE_DATE}.strftime('%d') }}}}'
            )
            """,
            "product_hist_n_1": f"""
            (
                year = '{{{{ {BASE_DATE_N_1}.strftime('%Y') }}}}' and
                month = '{{{{ {BASE_DATE_N_1}.strftime('%m') }}}}' and
                day = '{{{{ {BASE_DATE_N_1}.strftime('%d') }}}}'
            )
            """
        }

        for x in SOURCE_DICT.items():
            get_validation_operator(
                database=RAW_DATABASE,
                tablename="producto_hist",
                aws_conn_id=AWS_GLUE_CONNECTION_ID,
                task_id=f"validate_{x[0]}"
            )(expression=x[1])

    with TaskGroup("input_ingestion") as input_ingestion:
        summit_dbt_model_input = DBTDelfosOperator(
            task_id="dbt_athena_run_model_input",
            ssh_conn_id=DBT_CONNECTION_ID,
            domain=DOMAIN,
            subdomain=SUBDOMAIN,
            environment=ENV_MESH,
            account=ACCOUNT_NUMBER,
            repository=f"{COUNTRY}Delfos{DOMAIN.capitalize()}_{SUBDOMAIN}_DBT",
            mode="run",
            args_mode="--model gender.gender_model_data_input",
            dbt_type="ATHENA",
            variables={
                "execution_year": f"{{{{ {BASE_DATE}.strftime('%Y') }}}}",
                "execution_month": f"{{{{ {BASE_DATE}.strftime('%m') }}}}",
                "execution_day": f"{{{{ {BASE_DATE}.strftime('%d') }}}}",
                "env": ENV_MESH
            }
        )

        summit_purge_s3_folder = S3DeleteObjectsOperator(
            task_id="summit_purge_s3_folder",
            bucket=BUCKET,
            prefix=OUTPUT_UNLOAD_PREFIX,
            aws_conn_id=AWS_GLUE_CONNECTION_ID,
            execution_timeout=timedelta(seconds=60),
        )

        athena_unload_gender_model_data_input = AthenaOperator(
            task_id='athena_unload_gender_model_data_input',
            workgroup="delfos",
            query=QUERY_UNLOAD_MODEL_INPUT,
            database=MODEL_INPUT_DATABASE,
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

        summit_dbt_model_input >> summit_purge_s3_folder
        summit_purge_s3_folder >> athena_unload_gender_model_data_input
        athena_unload_gender_model_data_input >> await_for_s3_input_presence

    with TaskGroup("model_preprocessing") as model_preprocessing:
        summit_purge_s3_folder = S3DeleteObjectsOperator(
            task_id="summit_purge_preprocessing_s3_folder",
            bucket=BUCKET,
            prefix=OUTPUT_PROCESSING_PREFIX,
            aws_conn_id=AWS_GLUE_CONNECTION_ID,
            execution_timeout=timedelta(seconds=60),
        )

        submit_batch_transform_job = SageMakerTransformOperator(
            task_id="submit_preprocessing_batch_transform_job",
            config=transform_config(
                job_name="model-processor-job",
                model_name=(
                    f"{COUNTRY}-delfos-{DOMAIN}-{SUBDOMAIN}-"
                    + f"gender-model-processor-v1-{ENV_MESH}"
                ),
                s3_input_path=f"s3://{BUCKET}/{OUTPUT_UNLOAD_PREFIX}",
                s3_output_path=f"s3://{BUCKET}/{OUTPUT_PROCESSING_PREFIX}",
                join_source="Input",
                kms_id=RAW_KMS_ID,
                input_filter="$[1:]",
                output_filter="$",
                content_type="text/csv",
            ),
            wait_for_completion=False,
            aws_conn_id=AWS_SM_CONNECTION_ID,
        )

        xcom_expression = (
            str(submit_batch_transform_job.output)
            .replace("{{ ", "")
            .replace(" }}", "")
        )

        await_batch_transform_job_to_finish = SageMakerTransformSensor(
            task_id="await_preprocessing_batch_transform_job_to_finish",
            job_name=(
                f"{{{{ {xcom_expression}['Transform']['TransformJobName'] }}}}"
            ),
            aws_conn_id=AWS_SM_CONNECTION_ID,
            timeout=60 * 60,
            mode="reschedule",
            poke_interval=5 * 60,
        )

        await_for_s3_input_presence = S3KeySensor(
            task_id="await_for_s3_preprocessing_input_presence",
            aws_conn_id=AWS_GLUE_CONNECTION_ID,
            bucket_name=BUCKET,
            bucket_key=f"{OUTPUT_PROCESSING_PREFIX}*.out",
            wildcard_match=True,
            timeout=1 * 60,
            mode="poke",
            poke_interval=0.5 * 60,
        )

        summit_purge_s3_folder >> submit_batch_transform_job
        submit_batch_transform_job >> await_batch_transform_job_to_finish
        await_batch_transform_job_to_finish >> await_for_s3_input_presence

    with TaskGroup("model_inference") as model_inference:
        summit_purge_s3_folder = S3DeleteObjectsOperator(
            task_id="summit_purge_inference_s3_folder",
            bucket=BUCKET,
            prefix=INFERENCE_OUTPUT_KEY,
            aws_conn_id=AWS_GLUE_CONNECTION_ID,
            execution_timeout=timedelta(seconds=60),
        )

        submit_batch_transform_job = SageMakerTransformOperator(
            task_id="submit_inference_batch_transform_job",
            config=transform_config(
                job_name="model-estimator-job",
                model_name=(
                    f"{COUNTRY}-delfos-{DOMAIN}-{SUBDOMAIN}-"
                    + f"gender-model-estimator-v1-{ENV_MESH}"
                ),
                s3_input_path=f"s3://{BUCKET}/{OUTPUT_PROCESSING_PREFIX}",
                s3_output_path=f"s3://{BUCKET}/{INFERENCE_OUTPUT_KEY}",
                kms_id=RAW_KMS_ID,
                input_filter="$[3:]",
                output_filter="$",
                join_source="Input",
                content_type="text/csv",
                instance_type="ml.g4dn.xlarge",
                instance_count=1,
            ),
            wait_for_completion=False,
            aws_conn_id=AWS_SM_CONNECTION_ID,
        )

        xcom_expression = (
            str(submit_batch_transform_job.output)
            .replace("{{ ", "")
            .replace(" }}", "")
        )

        await_batch_transform_job_to_finish = SageMakerTransformSensor(
            task_id="await_inference_batch_transform_job_to_finish",
            job_name=(
                f"{{{{ {xcom_expression}['Transform']['TransformJobName'] }}}}"
            ),
            aws_conn_id=AWS_SM_CONNECTION_ID,
            timeout=60 * 60,
            mode="reschedule",
            poke_interval=5 * 60,
        )

        await_for_s3_input_presence = S3KeySensor(
            task_id="await_for_s3_inference_input_presence",
            aws_conn_id=AWS_GLUE_CONNECTION_ID,
            bucket_name=BUCKET,
            bucket_key=f"{INFERENCE_OUTPUT_KEY}*.out",
            wildcard_match=True,
            timeout=1 * 60,
            mode="poke",
            poke_interval=0.5 * 60,
        )

        summit_purge_s3_folder >> submit_batch_transform_job
        submit_batch_transform_job >> await_batch_transform_job_to_finish
        await_batch_transform_job_to_finish >> await_for_s3_input_presence

    with TaskGroup("inference_curated") as inference_curated:
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

        summit_dbt_model_input = DBTDelfosOperator(
            task_id="dbt_athena_run_model_output",
            ssh_conn_id=DBT_CONNECTION_ID,
            domain=DOMAIN,
            subdomain=SUBDOMAIN,
            environment=ENV_MESH,
            account=ACCOUNT_NUMBER,
            repository=f"{COUNTRY}Delfos{DOMAIN.capitalize()}_{SUBDOMAIN}_DBT",
            mode="run",
            args_mode="--model gender.gender_model",
            dbt_type="ATHENA",
            variables={
                "execution_year": f"{{{{ {BASE_DATE}.strftime('%Y') }}}}",
                "execution_month": f"{{{{ {BASE_DATE}.strftime('%m') }}}}",
                "execution_day": f"{{{{ {BASE_DATE}.strftime('%d') }}}}",
                "env": ENV_MESH
            }
        )

        athena_add_partition_inference_table >> summit_dbt_model_input

    task_end = EmptyOperator(task_id="end_process")

    task_start >> data_source_validation
    data_source_validation >> input_ingestion
    input_ingestion >> model_preprocessing
    model_preprocessing >> model_inference
    model_inference >> inference_curated
    inference_curated >> task_end

workflow()
