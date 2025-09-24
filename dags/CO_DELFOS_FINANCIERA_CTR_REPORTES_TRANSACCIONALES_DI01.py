""" 
    DAG enfocado en el proceso de generación de los reportes: 
        - Transaccional Uso
        - Transaccional Cierre

    equipo_de_trabajo: dataops

    version: 0.0.1
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.opsgenie.operators.opsgenie import OpsgenieCreateAlertOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.amazon.aws.operators.s3 import (
    S3DeleteObjectsOperator,
)
from airflow.utils.task_group import TaskGroup


env: str = Variable.get("env")
BUCKET_NAME = "nequi-delfos-col-core-finacle-curated-177333342796"
PRE_STAGE_KEY = "core/finacle/pre_stage/reportes_transaccionales"
POST_STAGE_KEY = "core/finacle/post_stage/reportes_transaccionales"
used_transactional_report_job = f"glue-job-finacle-transaccional-uso-{env}"
closed_transactional_report_job = f"glue-job-finacle-transaccional-cierre-{env}"
DAG_ID = "CO_DELFOS_PRODUCTOS_COR_REPORTES_TRANSACCIONALES_DI01"
DAG_DESCRIPTION = """
This DAG generates the Used Transactional and Closed Transactional reports of Nequi.
"""
DAG_SCHEDULE = "0 5 * * *"  # TODO: Changed for the right cron expression
execution_date = "{{ execution_date.strftime('%Y-%m-%d') }}"
OPSGENIE_CONNECTION_ID = "opsgenie-dataops"
slack_channel = (
    f"airflow-data-notifications-clientes-{env}"  # TODO: Changed for the right value
)
AWS_CONNECTION_ID = "apt0013-dataops-sherpa"
DATA_SOURCES = {
    "accounts": "crmuser",
    "ach_entry_detail_table": "tbaadm",
    "c_cs_atm_hist_tran_table": "custom",
    "c_cs_bcol_payments_tbl": "custom",
    "c_cs_c24_addl_det": "custom",
    "c_cs_cif_linkage_table": "custom",
    "c_cs_merc_tran_chrg_det": "custom",
    "c_cs_pay_rec_det_table": "custom",
    "c_cs_pocket_tran_table": "custom",
    "c_cs_tran_info_table": "custom",
    "daily_tran_detail_table": "tbaadm",
    "general_acct_mast_table": "tbaadm",
    "user_addtl_det_table": "tbaadm",
}


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
    """Orchestrates the sending of error messages

    Handle the error messages over slack and Opsgenie.
    """
    send_error_message_on_opsgenie(context)
    send_error_message_on_slack(context)


with DAG(
    dag_id=DAG_ID,
    description=DAG_DESCRIPTION,
    start_date=datetime(2023, 1, 11),
    catchup=False,
    schedule_interval=DAG_SCHEDULE,
    tags=[
        f"env:{env.upper()}",
        "team:DELFOS",
        "dominio:PRODUCTOS",
        "subdominio:CTR",
        "pais:CO",
        "criticidad:P1",
    ],
    on_failure_callback=send_error_message,
) as dag:
    dag.doc_md = f"""
    Descripcion: Este DAG esta diseñado para realizar el proceso de generación de los reportes 
        transaccionales de Nequi (Transaccional Uso y Cierre).Al final la informacion es almacenada
        en el bucket:**nequi-data-{env}** asociado a la cuenta AWS: **177333342796(Sherpa)**
    Enlace:
        TODO: Add the URL of the DAG's documentation in the Wiki.
    """
    # TODO: Create documentation of the DAG and put it right there.
    task_start = EmptyOperator(task_id="start_process")

    with TaskGroup(group_id="purge_stage_data_sources") as purge_stage_data_sources:
        [
            S3DeleteObjectsOperator(
                task_id=f"summit_purge_{x}",
                bucket=f"{BUCKET_NAME}-{env}",
                prefix=f"{PRE_STAGE_KEY}/{x}",
                aws_conn_id=AWS_CONNECTION_ID,
                execution_timeout=timedelta(seconds=60),
            )
            for x in DATA_SOURCES
        ]

    with TaskGroup(group_id="cure_data_sources") as cure_data_sources:
        tasks_run_job_data_sources = [
            GlueJobOperator(
                task_id=f"submit_run_job_{x[0]}",
                job_name=f"nequi-delfos-col-core-finacle-table-catalog-{x[0]}".replace(
                    "_", "-"
                ),
                script_args={
                    "--ENV": env,
                    "--bd": f"co_delfos_core_finacle_raw_{env}",
                    "--bucket": BUCKET_NAME,
                    "--filter_date": execution_date,
                    "--key": PRE_STAGE_KEY,
                    "--table_catalog": f"{x[1]}_{x[0]}",
                    "--temp_table": x[0],
                },
                wait_for_completion=False,
                aws_conn_id=AWS_CONNECTION_ID,
            )
            for x in DATA_SOURCES.items()
        ]

        tasks_await_job_data_sources = [
            GlueJobSensor(
                task_id=f"await_for_job_{x[1]}",
                run_id=tasks_run_job_data_sources[x[0]].output,
                job_name=f"nequi-delfos-col-core-finacle-table-catalog-{x[1]}".replace(
                    "_", "-"
                ),
                aws_conn_id=AWS_CONNECTION_ID,
                timeout=90 * 60,
                mode="reschedule",
                poke_interval=20 * 60,
            )
            for x in enumerate(DATA_SOURCES)
        ]

        tasks_await_s3_files_data_sources = [
            S3KeySensor(
                task_id=f"await_for_s3_files_{x}",
                aws_conn_id=AWS_CONNECTION_ID,
                bucket_name=f"{BUCKET_NAME}-{env}",
                bucket_key=f"{PRE_STAGE_KEY}/{x}/*.parquet",
                wildcard_match=True,
                timeout=1 * 60,
                mode="poke",
                poke_interval=0.5 * 60,
            )
            for x in DATA_SOURCES
        ]

        [
            tasks_run_job_data_sources[x]
            >> tasks_await_job_data_sources[x]
            >> tasks_await_s3_files_data_sources[x]
            for x in range(len(DATA_SOURCES))
        ]

    with TaskGroup(
        "generate_used_transactional_report"
    ) as generate_used_transactional_report:
        submit_ingestion_glue_job = GlueJobOperator(
            task_id="submit_ingestion_glue_job",
            job_name=used_transactional_report_job,
            script_args={
                "--ENV": env,
                "--destination_bucket": BUCKET_NAME,
                "--destination_key": f"{POST_STAGE_KEY}/transaccional_uso",
                "--report_name": "FINACLE_TRANSACCIONALSBA",
                "--source_bucket": BUCKET_NAME,
                "--source_key": PRE_STAGE_KEY,
                "--filter_date": execution_date,
            },
            wait_for_completion=False,
            aws_conn_id=AWS_CONNECTION_ID,
        )

        await_for_ingestion_glue_job_to_finish = GlueJobSensor(
            task_id="await_for_ingestion_glue_job_to_finish",
            run_id=submit_ingestion_glue_job.output,
            job_name=used_transactional_report_job,
            aws_conn_id=AWS_CONNECTION_ID,
            timeout=90 * 60,
            mode="reschedule",
            poke_interval=20 * 60,
        )

        await_for_s3_input_presence = S3KeySensor(
            task_id="await_for_s3_input_presence",
            aws_conn_id=AWS_CONNECTION_ID,
            bucket_name=f"{BUCKET_NAME}-{env}",
            bucket_key=(
                f"{POST_STAGE_KEY}/transaccional_uso/FINACLE_TRANSACCIONALSBA*.parquet"
            ),
            wildcard_match=True,
            timeout=1 * 60,
            mode="poke",
            poke_interval=0.5 * 60,
        )

        submit_ingestion_glue_job >> await_for_ingestion_glue_job_to_finish
        await_for_ingestion_glue_job_to_finish >> await_for_s3_input_presence

    with TaskGroup(
        "generate_closed_transactional_report"
    ) as generate_closed_transactional_report:
        submit_ingestion_glue_job = GlueJobOperator(
            task_id="submit_ingestion_glue_job",
            job_name=closed_transactional_report_job,
            script_args={
                "--ENV": env,
                "--destination_bucket": BUCKET_NAME,
                "--destination_key": f"{POST_STAGE_KEY}/transaccional_cierre",
                "--report_name": "FINACLE_TRANS_CIERRE",
                "--source_bucket": BUCKET_NAME,
                "--source_key": PRE_STAGE_KEY,
                "--filter_date": execution_date,
            },
            wait_for_completion=False,
            aws_conn_id=AWS_CONNECTION_ID,
        )

        await_for_ingestion_glue_job_to_finish = GlueJobSensor(
            task_id="await_for_ingestion_glue_job_to_finish",
            run_id=submit_ingestion_glue_job.output,
            job_name=closed_transactional_report_job,
            aws_conn_id=AWS_CONNECTION_ID,
            timeout=90 * 60,
            mode="reschedule",
            poke_interval=20 * 60,
        )

        await_for_s3_input_presence = S3KeySensor(
            task_id="await_for_s3_input_presence",
            aws_conn_id=AWS_CONNECTION_ID,
            bucket_name=f"{BUCKET_NAME}-{env}",
            bucket_key=(
                f"{POST_STAGE_KEY}/transaccional_uso/FINACLE_TRANS_CIERRE*.parquet"
            ),
            wildcard_match=True,
            timeout=1 * 60,
            mode="poke",
            poke_interval=0.5 * 60,
        )

        submit_ingestion_glue_job >> await_for_ingestion_glue_job_to_finish
        await_for_ingestion_glue_job_to_finish >> await_for_s3_input_presence

    task_end = EmptyOperator(task_id="end_process")

    task_start >> purge_stage_data_sources >> cure_data_sources
    cure_data_sources >> generate_used_transactional_report
    cure_data_sources >> generate_closed_transactional_report
    [
        generate_used_transactional_report,
        generate_closed_transactional_report,
    ] >> task_end


dag.doc_md = __doc__
