"""DAG to make the inference of the Confidence Score of Nequi."""
import sys
import logging
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.s3 import (
    S3DeleteObjectsOperator,
)
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.glue_catalog import GlueCatalogHook
from airflow.providers.opsgenie.operators.opsgenie import OpsgenieCreateAlertOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.amazon.aws.operators.sagemaker import SageMakerTransformOperator
from airflow.providers.amazon.aws.sensors.sagemaker import SageMakerTransformSensor

DAG_ID = "CO_DELFOS_GESTIONFRAUDE_FRE_CONFIDENCE_SCORE_ME01"
DAG_DESCRIPTION = "DAG que calcula las inferencias del Score de Confianza de Nequi"
DAG_SCHEDULE = "59 23 2 * *"
BASE_DATE = "(data_interval_end - macros.timedelta(days=2))"
EXECUTION_DATE = f"{{{{ {BASE_DATE}.strftime('%Y-%m-%d') }}}}"
EXECUTION_DATETIME = f"{{{{ {BASE_DATE}.strftime('%Y-%m-%d-%H-%M-%S') }}}}"
PARTITION = f"{{{{ {BASE_DATE}.strftime('%Y%m') }}}}"
ENV = Variable.get("env")
DOMAIN = "gestionfraude"
SUBDOMAIN = "fre"
COUNTRY = "co"
BUCKET = "nequi-analytics" if (ENV == "pdn") else "nequi-analytics-qa"
PROJECT_DIRECTORY = "nequi_confidence_score"
OUTPUT_BUCKET = "nequi-data" if (ENV == "pdn") else "nequi-data-qa"
OUTPUT_DIRECTORY = "data_co/incremental/nequi_confidence_score"
AWS_CONNECTION_ID = "apt0013-dataops-sherpa"
OPSGENIE_CONNECTION_ID = "opsgenie-dataops"
TAGS = [
    f"env.{ENV.upper()}",
    "teams.DELFOS",
    "dominio.GESTIONFRAUDE",
    "subdominio.FRE",
    "pais.CO",
    "criticidad.P3",
]
INGESTION_GLUE_JOB = (
    f"{COUNTRY}-delfos-{DOMAIN}-{SUBDOMAIN}-ingesta_tbl_input_confidence_score-{ENV}"
)
MODEL_OUTPUT_GLUE_JOB = (
    f"{COUNTRY}-delfos-{DOMAIN}-{SUBDOMAIN}-curado-output_confidence_score-{ENV}"
)
PREPROCESS_SM_MODEL = (
    f"{COUNTRY}-delfos-{DOMAIN}-{SUBDOMAIN}-confidence-score-processor-v1-{ENV}"
)
ESTIMATOR_SM_MODEL = (
    f"{COUNTRY}-delfos-{DOMAIN}-{SUBDOMAIN}-confidence-score-estimator-v1-{ENV}"
)
PREPROCESS_BT_JOB = "cs-processor-bt-job-{0}-{1}".format(EXECUTION_DATETIME, ENV)
ESTIMATOR_BT_JOB = "cs-estimator-bt-job-{0}-{1}".format(EXECUTION_DATETIME, ENV)
SLACK_CHANNEL = f"airflow-data-notifications-{ENV}"
DEFAULT_ARGS = {
    "owner": "APOLO",
    "start_date": datetime(2024, 1, 15),
}
DATA_SOURCES = [
    {
        "databasename": "nequi_co",
        "tablename": "finacle_producto_hist",
        "expression": f"""
            year='{{{{ {BASE_DATE}.strftime('%Y') }}}}' 
            and month='{{{{ {BASE_DATE}.strftime('%m') }}}}' 
            and day='{{{{ {BASE_DATE}.strftime('%d') }}}}'
        """,
    },
    {
        "databasename": "nequi_co",
        "tablename": "scv_identity_score",
        "expression": f"""
            year='{{{{ {BASE_DATE}.strftime('%Y') }}}}' 
            and month='{{{{ {BASE_DATE}.strftime('%m') }}}}' 
            and day='{{{{ {BASE_DATE}.strftime('%d') }}}}'
        """,
    },
    {
        "databasename": "nequi_co",
        "tablename": "nequi_segmentation_hist",
        "expression": f"""
            ingestion_year='{{{{ {BASE_DATE}.strftime('%Y') }}}}' 
            and ingestion_month='{{{{ {BASE_DATE}.strftime('%m') }}}}'
        """,
    },
]
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


class InferencePipelineTaskGroups:
    """Class that instantiates an interface that can create some
    Airflow Task Groups to create an Inference Pipeline using Airflow.

    Args:
        aws_conn_id (str): ID of the AWS Connection.

    """

    def __init__(self, aws_conn_id: str):
        self.aws_conn_id = aws_conn_id

    def __transform_config(
        self,
        job_name: str,
        model_name: str,
        s3_input_path: str,
        s3_output_path: str,
        input_filter: str,
        output_filter: str,
        content_type: str,
    ) -> dict:
        """Get the configurations of a batch transform job.

        Create a batch transform configuration dict in order
        to execute a job using Airflow.

        Args:
            job_name (str): Name of the batch transform job.
            model_name (str): Name of the model to used.
            s3_input_path (str): Path of the input of the batch transform.
            s3_output_path (str): Path of the output of the batch transform.
            input_filter (str): Input filter for the batch transform job.
            output_filter (str): Output filter for the batch transform job.
            content_type (str): Type of the input file (csv or json).

        Returns:
            (dict): Dictionary with the Batch Transform Configurations.

        """
        return {
            "Transform": {
                "TransformJobName": job_name,
                "ModelName": model_name,
                "MaxConcurrentTransforms": 4,
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
                },
                "TransformResources": {
                    "InstanceType": "ml.m5.xlarge",
                    "InstanceCount": 2,
                },
                "DataProcessing": {
                    "InputFilter": input_filter,
                    "OutputFilter": output_filter,
                    "JoinSource": "Input",
                },
            }
        }

    def __get_validation_operator(
        self,
        task_id: str,
        database: str,
        tablename: str,
    ) -> object:
        """Create a Operator that validates the partition of a Data Source.

        Args:
            task_id (str): ID of the task.
            database (str): Name of the Database of the Data Source.
            tablename (str): Name of the table of the Data Source.

        Returns:
            (@task): Airflow task that uses the GlueCatalogHook.

        """

        @task(task_id=task_id)
        def validation_operator(expression: str):
            """Creates a Airflow Task using the GlueCatalogHook.

            Args:
                expression (str): Expression to filter the partition of a table.

            """
            hook = GlueCatalogHook(aws_conn_id=self.aws_conn_id)

            validation = hook.check_for_partition(database, tablename, expression)

            if not validation:
                task_logger.error(
                    f"The Data Source {database}.{tablename} is NOT up to date."
                )
                sys.exit()
            task_logger.info(f"The Data Source {database}.{tablename} is up to date.")

        return validation_operator

    def get_data_sources_validator_task_group(self, data_sources: list) -> TaskGroup:
        """Creates an Airflow Task Group that validates the partition of a group of data sources.

        Args:
            data_sources (list): List with all the information about the Data Sources.

        Returns:
            (TaskGroup): Task Group to validate the Data Sources.

        """
        with TaskGroup("data_source_validation") as data_source_validation_tasks:
            _ = [
                self.__get_validation_operator(
                    task_id=f"validate_{x['databasename']}_{x['tablename']}",
                    database=x["databasename"],
                    tablename=x["tablename"],
                )(expression=x["expression"])
                for x in data_sources
            ]

        return data_source_validation_tasks

    def get_glue_executor_task_group(
        self,
        process_name: str,
        job_name: str,
        script_args: dict,
        timeout: int,
        poke_interval: int,
        bucket_name: str,
        bucket_key: str,
    ) -> TaskGroup:
        """Creates an Airflow Task Group that executes a Glue Job and validates
        its state and output.

        Args:
            process_name (str): Name of the process that is going to be executed using the job.
            job_name (str): Name of the Glue Job.
            script_args (dict): Arguments of the Glue Job.
            timeout (int): Timeout for the Glue Job Sensor.
            poke_interval (int): Poke Interval for the Glue Job Sensor.
            bucket_name (str): Name of the Bucket where the output of the job is going to be saved.
            bucket_key (str): Key of the output of the job.

        Returns:
            (TaskGroup): Task Group to execute a Glue Job.

        """
        with TaskGroup(f"glue_execution_{process_name}") as glue_execution_tg:
            submit_glue_job = GlueJobOperator(
                task_id=f"submit_{process_name}_glue_job",
                job_name=job_name,
                script_args=script_args,
                wait_for_completion=False,
                aws_conn_id=self.aws_conn_id,
            )

            await_for_glue_job_to_finish = GlueJobSensor(
                task_id=f"await_for_{process_name}_glue_job_to_finish",
                run_id=submit_glue_job.output,
                job_name=job_name,
                aws_conn_id=self.aws_conn_id,
                timeout=timeout,
                mode="reschedule",
                poke_interval=poke_interval,
            )

            await_for_s3_presence = S3KeySensor(
                task_id=f"await_for_{process_name}_s3_presence",
                aws_conn_id=self.aws_conn_id,
                bucket_name=bucket_name,
                bucket_key=bucket_key,
                wildcard_match=True,
                timeout=1 * 60,
                mode="poke",
                poke_interval=0.5 * 60,
            )

            submit_glue_job >> await_for_glue_job_to_finish
            await_for_glue_job_to_finish >> await_for_s3_presence

        return glue_execution_tg

    def get_sm_bt_executor_task_group(
        self,
        process_name: str,
        bucket_name: str,
        prefix: str,
        bucket_key: str,
        job_name: str,
        model_name: str,
        bt_s3_input_path: str,
        bt_s3_output_path: str,
        input_filter: str,
        output_filter: str,
        timeout: int,
        poke_interval: int,
    ) -> TaskGroup:
        """Creates an Airflow Task Group that executes a SageMaker Batch Transform Job
        and validates its state and output.

        Args:
            process_name (str): Name of the process that is going to be executed using the job.
            bucket_name (str): Name of the Bucket where the output of the job is going to be saved.
            prefix (str): Prefix that describes the directory that is going to be purged.
            bucket_key (str): Key of the output of the job.
            job_name (str): Name of the Glue Job.
            model_name (str): Name of the Model that the Job is going to use.
            bt_s3_input_path: (str): Input path for the SageMaker BT Job.
            bt_s3_output_path: (str): Output path for the SageMaker BT Job.
            input_filter: (str): Input Filter for the SageMaker BT Job.
            output_filter: (str): Output Filter for the SageMaker BT Job.
            timeout (int): Timeout for the Sagemaker BT Job Sensor.
            poke_interval (int): Poke Interval for the Sagemaker BT Job Sensor.

        Returns:
            (TaskGroup): Task Group to execute a SageMaker BT Job.

        """
        with TaskGroup(f"sm_bt_execution_{process_name}") as sm_bt_execution_tg:
            summit_purge_s3_folder = S3DeleteObjectsOperator(
                task_id=f"summit_purge_{process_name}_s3_folder",
                bucket=bucket_name,
                prefix=bucket_key,
                aws_conn_id=self.aws_conn_id,
                execution_timeout=timedelta(seconds=60),
            )

            submit_batch_transform_job = SageMakerTransformOperator(
                task_id=f"submit_{process_name}_batch_transform_job",
                config=self.__transform_config(
                    job_name=job_name,
                    model_name=model_name,
                    s3_input_path=bt_s3_input_path,
                    s3_output_path=bt_s3_output_path,
                    input_filter=input_filter,
                    output_filter=output_filter,
                    content_type="text/csv",
                ),
                wait_for_completion=False,
                aws_conn_id=self.aws_conn_id,
            )

            xcom_expression = (
                str(submit_batch_transform_job.output)
                .replace("{{ ", "")
                .replace(" }}", "")
            )

            await_batch_transform_job_to_finish = SageMakerTransformSensor(
                task_id=f"await_{process_name}_batch_transform_job_to_finish",
                job_name=(
                    f"{{{{ {xcom_expression}['Transform']['TransformJobName'] }}}}"
                ),
                aws_conn_id=self.aws_conn_id,
                timeout=timeout,
                mode="reschedule",
                poke_interval=poke_interval,
            )

            await_for_s3_input_presence = S3KeySensor(
                task_id=f"await_for_s3_{process_name}_input_presence",
                aws_conn_id=self.aws_conn_id,
                bucket_name=bucket_name,
                bucket_key=f"{bucket_key}/{prefix}",
                wildcard_match=True,
                timeout=1 * 60,
                mode="poke",
                poke_interval=0.5 * 60,
            )

            summit_purge_s3_folder >> submit_batch_transform_job
            submit_batch_transform_job >> await_batch_transform_job_to_finish
            await_batch_transform_job_to_finish >> await_for_s3_input_presence

        return sm_bt_execution_tg


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
    """DAG for the Confidence Score model of Nequi.

    Implement the Airflow operators of the workflow, and also manage the
    order of the execution for the different tasks to calculate the inferences
    of this machine learning model.
    """
    ip = InferencePipelineTaskGroups(aws_conn_id=AWS_CONNECTION_ID)

    start_inference_pipeline = EmptyOperator(task_id="start_inference_pipeline")

    validate_data_sources = ip.get_data_sources_validator_task_group(
        data_sources=DATA_SOURCES
    )

    create_model_input = ip.get_glue_executor_task_group(
        process_name="model_input",
        job_name=INGESTION_GLUE_JOB,
        script_args={
            "--execution_date": EXECUTION_DATE,
            "--env_mesh": ENV,
            "--month_horizon": "6",
            "--month_threshold": "18",
        },
        timeout=30 * 60,
        poke_interval=5 * 60,
        bucket_name=BUCKET,
        bucket_key=(
            f"{PROJECT_DIRECTORY}/data/input/"
            + f"year={{{{ {BASE_DATE}.strftime('%Y') }}}}/"
            + f"month={{{{ {BASE_DATE}.strftime('%m') }}}}/*"
        ),
    )

    with TaskGroup("model_predictions") as model_predictions:
        execute_preprocess = ip.get_sm_bt_executor_task_group(
            process_name="model_preprocess",
            bucket_name=BUCKET,
            prefix="*.out",
            bucket_key=(
                f"{PROJECT_DIRECTORY}/data/preprocess/"
                + f"year={{{{ {BASE_DATE}.strftime('%Y') }}}}/"
                + f"month={{{{ {BASE_DATE}.strftime('%m') }}}}"
            ),
            job_name=PREPROCESS_BT_JOB,
            model_name=PREPROCESS_SM_MODEL,
            bt_s3_input_path=(
                f"s3://{BUCKET}/{PROJECT_DIRECTORY}/data/input/"
                + f"year={{{{ {BASE_DATE}.strftime('%Y') }}}}/"
                + f"month={{{{ {BASE_DATE}.strftime('%m') }}}}/"
            ),
            bt_s3_output_path=(
                f"s3://{BUCKET}/{PROJECT_DIRECTORY}/data/preprocess/"
                + f"year={{{{ {BASE_DATE}.strftime('%Y') }}}}/"
                + f"month={{{{ {BASE_DATE}.strftime('%m') }}}}/"
            ),
            input_filter="$",
            output_filter="$",
            timeout=15 * 60,
            poke_interval=4 * 60,
        )

        execute_inference = ip.get_sm_bt_executor_task_group(
            process_name="model_inference",
            bucket_name=BUCKET,
            prefix="*.out",
            bucket_key=(
                f"{PROJECT_DIRECTORY}/data/inferences/"
                + f"year={{{{ {BASE_DATE}.strftime('%Y') }}}}/"
                + f"month={{{{ {BASE_DATE}.strftime('%m') }}}}"
            ),
            job_name=ESTIMATOR_BT_JOB,
            model_name=ESTIMATOR_SM_MODEL,
            bt_s3_input_path=(
                f"s3://{BUCKET}/{PROJECT_DIRECTORY}/data/preprocess/"
                + f"year={{{{ {BASE_DATE}.strftime('%Y') }}}}/"
                + f"month={{{{ {BASE_DATE}.strftime('%m') }}}}/"
            ),
            bt_s3_output_path=(
                f"s3://{BUCKET}/{PROJECT_DIRECTORY}/data/inferences/"
                + f"year={{{{ {BASE_DATE}.strftime('%Y') }}}}/"
                + f"month={{{{ {BASE_DATE}.strftime('%m') }}}}/"
            ),
            input_filter="$[33:]",
            output_filter="$",
            timeout=15 * 60,
            poke_interval=4 * 60,
        )

        execute_preprocess >> execute_inference

    create_model_output = ip.get_glue_executor_task_group(
        process_name="model_output",
        job_name=MODEL_OUTPUT_GLUE_JOB,
        script_args={
            "--partition": PARTITION,
            "--input_path": (
                f"s3://{BUCKET}/{PROJECT_DIRECTORY}/data/inferences/"
                + f"year={{{{ {BASE_DATE}.strftime('%Y') }}}}/"
                + f"month={{{{ {BASE_DATE}.strftime('%m') }}}}/"
            ),
            "--input_stage_path": (
                f"s3://{BUCKET}/{PROJECT_DIRECTORY}/data/stage_inferences/"
                + f"year={{{{ {BASE_DATE}.strftime('%Y') }}}}/"
                + f"month={{{{ {BASE_DATE}.strftime('%m') }}}}/"
            ),
            "--output_path": f"s3://{OUTPUT_BUCKET}/{OUTPUT_DIRECTORY}",
            "--datalake-formats": "hudi",
        },
        timeout=20 * 60,
        poke_interval=5 * 60,
        bucket_name=OUTPUT_BUCKET,
        bucket_key=(
            f"{OUTPUT_BUCKET}/{OUTPUT_DIRECTORY}/"
            + f"year={{{{ {BASE_DATE}.strftime('%Y') }}}}/"
            + f"month={{{{ {BASE_DATE}.strftime('%m') }}}}/*.parquet"
        ),
    )

    end_inference_pipeline = EmptyOperator(task_id="end_inference_pipeline")

    start_inference_pipeline >> validate_data_sources
    validate_data_sources >> create_model_input
    create_model_input >> model_predictions
    model_predictions >> create_model_output
    create_model_output >> end_inference_pipeline


workflow()
