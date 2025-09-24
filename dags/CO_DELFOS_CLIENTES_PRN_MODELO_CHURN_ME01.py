"""DAG to make the inference of the churn model of Nequi."""
from datetime import datetime
from airflow.decorators import dag
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.amazon.aws.operators.sagemaker import SageMakerTransformOperator
from airflow.providers.amazon.aws.sensors.sagemaker import SageMakerTransformSensor

DAG_ID = "CO_DELFOS_CLIENTES_PRN_MODELO_CHURN_ME01"
DAG_DESCRIPTION = (
    "DAG que calcula las inferencias del modelo de Churn de clientes de Nequi"
)
DAG_SCHEDULE = "59 23 2 * *"
EXECUTION_DATE = (
    "{{ (data_interval_end - macros.timedelta(days=2)).strftime('%Y-%m-%d') }}"
)
EXECUTION_DATETIME = (
    "{{ (data_interval_end - macros.timedelta(days=2)).strftime('%Y-%m-%d-%H-%M-%S') }}"
)
PARTITION = "{{ (data_interval_end - macros.timedelta(days=2)).strftime('%Y%m') }}"
ENV = Variable.get("env")
DOMAIN = "clientes"
SUBDOMAIN = "prn"
COUNTRY = "co"
BUCKET = "nequi-analytics" if (ENV == "pdn") else "nequi-analytics-qa"
PROJECT_DIRECTORY = "clients_churn_model"
OUTPUT_BUCKET = "nequi-data"
OUTPUT_DIRECTORY = "data_co/incremental/clientes_modelo_churn"
AWS_CONNECTION_ID = "apt0013-dataops-sherpa"
OPSGENIE_CONNECTION_ID = "opsgenie-dataops"
TAGS = [
    f"env.{ENV.upper()}",
    "teams.DELFOS",
    "dominio.CLIENTES",
    "subdominio.PRN",
    "pais.CO",
    "criticidad.P3",
]
INGESTION_GLUE_JOB = (
    f"{COUNTRY}-delfos-{DOMAIN}-{SUBDOMAIN}-ingesta-input_modelo_churn-{ENV}"
)
MODEL_OUTPUT_GLUE_JOB = (
    f"{COUNTRY}-delfos-{DOMAIN}-{SUBDOMAIN}-curado-output_modelo_churn-{ENV}"
)
PREPROCESS_SM_MODEL = (
    f"{COUNTRY}-delfos-{DOMAIN}-{SUBDOMAIN}-model-churn-processor-{ENV}"
)
ESTIMATOR_SM_MODEL = (
    f"{COUNTRY}-delfos-{DOMAIN}-{SUBDOMAIN}-model-churn-estimator-{ENV}"
)
PREPROCESS_BT_JOB = "churn-processor-bt-job-{0}".format(EXECUTION_DATETIME)
ESTIMATOR_BT_JOB = "churn-estimator-bt-job-{0}".format(EXECUTION_DATETIME)
SLACK_CHANNEL = f"airflow-data-notifications-clientes-{ENV}"

DEFAULT_ARGS = {
    "owner": "GERENCIA_INTELIGENCIA_DE_NEGOCIO",
    "start_date": datetime(2023, 4, 1),
}


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


def transform_config(
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
    """
    return {
        "Transform": {
            "TransformJobName": job_name,
            "ModelName": model_name,
            "MaxConcurrentTransforms": 4,
            "BatchStrategy": "MultiRecord",
            "TransformInput": {
                "DataSource": {
                    "S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": s3_input_path}
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


@dag(
    dag_id=DAG_ID,
    description=DAG_DESCRIPTION,
    default_args=DEFAULT_ARGS,
    schedule_interval=DAG_SCHEDULE,
    catchup=False,
    tags=TAGS,
    on_success_callback=orchestrate_error_message,
    on_failure_callback=orchestrate_error_message,
)
def workflow():
    """DAG for the Churn model of the Clients Domain of Nequi.

    Implement the Airflow operators of the workflow, and also manage the
    order of the execution for the different tasks to calculate the inferences
    of this machine learning model.
    """
    start_inference_pipeline = EmptyOperator(task_id="start_inference_pipeline")

    with TaskGroup("data_ingestion_tasks") as data_ingestion_tasks:
        submit_ingestion_glue_job = GlueJobOperator(
            task_id="submit_ingestion_glue_job",
            job_name=INGESTION_GLUE_JOB,
            script_args={
                "--execution_date": EXECUTION_DATE,
                "--ingestion_type": "inference",
                "--output_path": f"s3://{BUCKET}/{PROJECT_DIRECTORY}/data/input",
                "--threshold_inactivity": "90",
                "--horizon_future_days": "30",
            },
            wait_for_completion=False,
            aws_conn_id=AWS_CONNECTION_ID,
        )

        await_for_ingestion_glue_job_to_finish = GlueJobSensor(
            task_id="await_for_ingestion_glue_job_to_finish",
            run_id=submit_ingestion_glue_job.output,
            job_name=INGESTION_GLUE_JOB,
            aws_conn_id=AWS_CONNECTION_ID,
            timeout=20 * 60,
            mode="reschedule",
            poke_interval=5 * 60,
        )

        await_for_s3_input_presence = S3KeySensor(
            task_id="await_for_s3_input_presence",
            aws_conn_id=AWS_CONNECTION_ID,
            bucket_name=BUCKET,
            bucket_key=f"{PROJECT_DIRECTORY}/data/input/input.csv",
            wildcard_match=True,
            timeout=1 * 60,
            mode="poke",
            poke_interval=0.5 * 60,
        )

        submit_ingestion_glue_job >> await_for_ingestion_glue_job_to_finish
        await_for_ingestion_glue_job_to_finish >> await_for_s3_input_presence

    with TaskGroup("model_inference_tasks") as model_inference_tasks:
        submit_preprocess_batch_transform_job = SageMakerTransformOperator(
            task_id="submit_preprocess_batch_transform_job",
            config=transform_config(
                job_name=PREPROCESS_BT_JOB,
                model_name=PREPROCESS_SM_MODEL,
                s3_input_path=f"s3://{BUCKET}/{PROJECT_DIRECTORY}/data/input/input.csv",
                s3_output_path=f"s3://{BUCKET}/{PROJECT_DIRECTORY}/data/preprocess",
                input_filter="$[1:]",
                output_filter="$",
                content_type="text/csv",
            ),
            wait_for_completion=False,
            aws_conn_id=AWS_CONNECTION_ID,
        )

        await_preprocess_batch_transform_job_to_finish = SageMakerTransformSensor(
            task_id="await_preprocess_batch_transform_job_to_finish",
            job_name=PREPROCESS_BT_JOB,
            aws_conn_id=AWS_CONNECTION_ID,
            timeout=15 * 60,
            mode="reschedule",
            poke_interval=4 * 60,
        )

        await_for_s3_preprocess_input_presence = S3KeySensor(
            task_id="await_for_s3_preprocess_input_presence",
            aws_conn_id=AWS_CONNECTION_ID,
            bucket_name=BUCKET,
            bucket_key=f"{PROJECT_DIRECTORY}/data/preprocess/input.csv.out",
            wildcard_match=True,
            timeout=1 * 60,
            mode="poke",
            poke_interval=0.5 * 60,
        )

        submit_estimator_batch_transform_job = SageMakerTransformOperator(
            task_id="submit_estimator_batch_transform_job",
            config=transform_config(
                job_name=ESTIMATOR_BT_JOB,
                model_name=ESTIMATOR_SM_MODEL,
                s3_input_path=f"s3://{BUCKET}/{PROJECT_DIRECTORY}/data/preprocess/input.csv.out",
                s3_output_path=f"s3://{BUCKET}/{PROJECT_DIRECTORY}/data/inferences",
                input_filter="$[8:]",
                output_filter="$",
                content_type="text/csv",
            ),
            wait_for_completion=False,
            aws_conn_id=AWS_CONNECTION_ID,
        )

        await_estimator_batch_transform_job_to_finish = SageMakerTransformSensor(
            task_id="await_estimator_batch_transform_job_to_finish",
            job_name=ESTIMATOR_BT_JOB,
            aws_conn_id=AWS_CONNECTION_ID,
            timeout=15 * 60,
            mode="reschedule",
            poke_interval=4 * 60,
        )

        await_for_s3_estimator_input_presence = S3KeySensor(
            task_id="await_for_s3_estimator_input_presence",
            aws_conn_id=AWS_CONNECTION_ID,
            bucket_name=BUCKET,
            bucket_key=f"{PROJECT_DIRECTORY}/data/inferences/input.csv.out.out",
            wildcard_match=True,
            timeout=1 * 60,
            mode="poke",
            poke_interval=0.5 * 60,
        )

        (
            submit_preprocess_batch_transform_job
            >> await_preprocess_batch_transform_job_to_finish
        )
        (
            await_preprocess_batch_transform_job_to_finish
            >> await_for_s3_preprocess_input_presence
        )
        await_for_s3_preprocess_input_presence >> submit_estimator_batch_transform_job
        (
            submit_estimator_batch_transform_job
            >> await_estimator_batch_transform_job_to_finish
        )
        (
            await_estimator_batch_transform_job_to_finish
            >> await_for_s3_estimator_input_presence
        )

    with TaskGroup("curated_output_model_tasks") as curated_output_model_tasks:
        submit_curated_output_model_glue_job = GlueJobOperator(
            task_id="submit_curated_output_model_glue_job",
            job_name=MODEL_OUTPUT_GLUE_JOB,
            script_args={
                "--partition": PARTITION,
                "--threshold": "0.5",
                "--input_path": f"s3://{BUCKET}/{PROJECT_DIRECTORY}/data/inferences/input.csv.out.out",
                "--output_path": f"s3://{OUTPUT_BUCKET}/{OUTPUT_DIRECTORY}",
                "--datalake-formats": "hudi",
            },
            wait_for_completion=False,
            aws_conn_id=AWS_CONNECTION_ID,
        )

        await_for_ingestion_glue_job_to_finish = GlueJobSensor(
            task_id="await_for_ingestion_glue_job_to_finish",
            run_id=submit_curated_output_model_glue_job.output,
            job_name=MODEL_OUTPUT_GLUE_JOB,
            aws_conn_id=AWS_CONNECTION_ID,
            timeout=12 * 60,
            mode="reschedule",
            poke_interval=6 * 60,
        )

        submit_curated_output_model_glue_job >> await_for_ingestion_glue_job_to_finish

    end_inference_pipeline = EmptyOperator(task_id="end_inference_pipeline")

    start_inference_pipeline >> data_ingestion_tasks
    data_ingestion_tasks >> model_inference_tasks
    model_inference_tasks >> curated_output_model_tasks
    curated_output_model_tasks >> end_inference_pipeline


workflow()
