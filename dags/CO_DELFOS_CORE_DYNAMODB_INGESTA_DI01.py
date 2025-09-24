""" 
    DAG enfocado en el proceso de extracción de las tablas de DynamoDB de Nequi.

    equipo_de_trabajo: dataops

    version: 0.0.1
"""
import os
from datetime import datetime, timedelta
import json
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.opsgenie.operators.opsgenie import OpsgenieCreateAlertOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.amazon.aws.operators.s3 import (
    S3DeleteObjectsOperator,
)
from airflow.utils.task_group import TaskGroup


env: str = Variable.get("env")
bucket_name = "nequi-data" if (env == "pdn") else "nequi-data-qa"
DAG_ID = "CO_DELFOS_CORE_DYNAMODB_INGESTA_DI01"
DAG_DESCRIPTION = """It implements dynamodb extraction main DAG"""
DAG_SCHEDULE = "0 5 * * *"
OPSGENIE_CONNECTION_ID = "opsgenie-dataops"
slack_channel = f"airflow-data-notifications-clientes-{env}"
AWS_CONNECTION_ID = "apt0013-dataops-sherpa"
aws_lambda_function_name = f"general-export-dynamo-to-s3-by-event-{env}"
table_tratamineto_de_datos_personales = f"user-personal-data-treatment-{env}"


def send_error_message_on_slack(context):
    """
    Send a failure message over slack
    """
    error_message: str = f"""
        Ha ocurrio un error:
            dag: {context.get("task_instance").dag_id}
            tarea: {context.get("task_instance").task_id}
            url del log: {context.get('task_instance').log_url}
    """
    error_message_on_slack = SlackWebhookOperator(
        task_id="error_message_on_slack",
        slack_webhook_conn_id=f"slack-{slack_channel}",
        message=error_message,
    )
    return error_message_on_slack.execute(context=context)


def send_error_message_on_opsgenie(context):
    """
    send a failure message over slack
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


def send_error_message(context):
    send_error_message_on_opsgenie(context)
    send_error_message_on_slack(context)


def generate_general_export_dynamo_tasks(
    group_id: str,
    bucket_name: str,
    bucket_prefix: str,
    lambda_function_name: str,
    dynamodb_arn_table_name: str,
    aws_conn_id: str,
):
    """Generate a Group Task to extract a specific table.

    Args:
        group_id (str): Name of the Group Task.
        bucket_name (str): Name of the Bucket where the data is going to be stored.
        bucket_prefix (str): Prefix of the path.
        lambda_function_name (str): Name of the lambda to extract the data.
        dynamodb_arn_table_name (str): Name of the table to be extracted.
        aws_conn_id (str): AWS connection.
    """
    bucket_prefix_to_delete = os.path.join(bucket_prefix, "AWSDynamoDB")
    bucket_key_sensor = os.path.join(
        bucket_prefix,
        "AWSDynamoDB/*manifest-summary.json",
    )

    with TaskGroup(group_id=group_id) as task_group:
        summit_purge_s3_folder = S3DeleteObjectsOperator(
            task_id="summit_purge_s3_folder",
            bucket=bucket_name,
            prefix=bucket_prefix_to_delete,
            aws_conn_id=aws_conn_id,
            execution_timeout=timedelta(seconds=60),
        )

        lambda_payload = {
            "arnTable": dynamodb_arn_table_name,
            "bucketName": bucket_name,
            "additionalParams": {
                "S3Prefix": bucket_prefix,
            },
        }

        summit_task_invoke_lambda_dynamodb_export = LambdaInvokeFunctionOperator(
            task_id="summit_task_invoke_lambda_dynamodb_export",
            function_name=lambda_function_name,
            payload=json.dumps(lambda_payload),
            aws_conn_id=aws_conn_id,
            invocation_type="Event",
            qualifier="$LATEST",
            execution_timeout=timedelta(seconds=60),
        )

        await_for_s3_input_presence = S3KeySensor(
            task_id="await_for_s3_input_presence",
            bucket_name=bucket_name,
            bucket_key=bucket_key_sensor,
            wildcard_match=True,
            aws_conn_id=aws_conn_id,
            timeout=30 * 60,
            mode="reschedule",
            poke_interval=5 * 60,
        )

        summit_purge_s3_folder >> summit_task_invoke_lambda_dynamodb_export
        summit_task_invoke_lambda_dynamodb_export >> await_for_s3_input_presence

        summit_purge_s3_folder.doc_md = """
        **S3_BORRAR_EXPORT_VIEJO Operator S3DeleteObjectsOperator**

        Se borra el export viejo de la ruta S3.
        """

        summit_task_invoke_lambda_dynamodb_export.doc_md = f"""
        **LAMBDA_INVOCAR_EXPORT_DYNAMODB Operator LambdaInvokeFunctionOperator**

        Se Invoca la Lambda {lambda_function_name} para que se ejecute \
        el export sobre la tabla de DynamoDB.
        """

        await_for_s3_input_presence.doc_md = f"""
        **S3_KEY_SENSOR_ESPERA_EXPORT_DYNAMODB Operator S3KeySensor**

        Detecta si el export ha finalizado consultando la ruta \
        {bucket_key_sensor}.
        """

    return task_group


with DAG(
    dag_id=DAG_ID,
    description=DAG_DESCRIPTION,
    start_date=datetime(2023, 9, 11),
    catchup=False,
    schedule_interval=DAG_SCHEDULE,
    tags=[
        f"env:{env.upper()}",
        "team:DELFOS",
        "dominio:CORE",  # TODO: Use the right domain
        "subdominio:DYNAMODB",  # TODO: Use the right domain
        "pais:CO",
        "criticidad:P1",
    ],
    on_failure_callback=send_error_message,
) as dynamodb_extraction_dag:
    dynamodb_extraction_dag.doc_md = f"""
    Descripcion: Este DAG esta diseñado para la ejecucion de las extracciones de informacion de las tablas que pertenecen a la
        base de datos DYNAMODB. Atraves de una ejecuciones diarias se procede con la ejecucion de la extraccion de las tablas
        por medio de AWS GLUE. Al final la informacion es almacenada en el bucket:**nequi-data-{env}** asociado a la cuenta
        AWS: **177333342796(Sherpa)**
    Enlace:

    """
    # TODO: Create documentation of the DAG and put it right there.
    task_start = EmptyOperator(task_id="start_process")

    tb_user_personal_data_treatment = generate_general_export_dynamo_tasks(
        group_id="export_tb_user_personal_data_treatment",
        bucket_name=bucket_name,
        bucket_prefix=f"stage/dynamodb/user-personal-data-treatment-{env}",
        lambda_function_name=aws_lambda_function_name,
        dynamodb_arn_table_name=(
            f"arn:aws:dynamodb:us-east-1:177333342796:table/user-personal-data-treatment-{env}"
        ),
        aws_conn_id=AWS_CONNECTION_ID,
    )

    task_end = EmptyOperator(task_id="end_process")

    task_start >> tb_user_personal_data_treatment >> task_end


dynamodb_extraction_dag.doc_md = __doc__

tb_user_personal_data_treatment.tooltip = """
Grupo de tareas de la exportación de los datos de user-personal-data-treatment
"""
