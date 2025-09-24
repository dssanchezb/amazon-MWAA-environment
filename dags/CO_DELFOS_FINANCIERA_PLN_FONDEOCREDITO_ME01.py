"""DAG CO_DELFOS_FINANCIERA_PLN_FONDEOCREDITO_ME01"""
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.opsgenie.operators.opsgenie import OpsgenieCreateAlertOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from dbt_delfos_operator import DBTDelfosOperator
from datetime import datetime

DAG_ID = "CO_DELFOS_FINANCIERA_PLN_FONDEOCREDITO_ME01"
DAG_DESCRIPTION = "Este dag ejecuta el proceso del calculo de fondeo"
DAG_SCHEDULE = "59 23 8 * *" #Frecuencia de la actualización
BASE_DATE = "(data_interval_end - macros.timedelta(days=8))"
EXECUTION_DATE_YEAR = f"{{{{ {BASE_DATE}.strftime('%Y') }}}}" 
EXECUTION_DATE_MONTH = f"{{{{ {BASE_DATE}.strftime('%m') }}}}" 
EXECUTION_YEAR_MONTH = EXECUTION_DATE_YEAR + EXECUTION_DATE_MONTH
ENV = Variable.get("env")
DOMAIN = "financiera"
SUBDOMAIN = "pln"
COUNTRY = "co"
OPSGENIE_CONNECTION_ID = "opsgenie-dataops"
DBT_CONNECTION_ID = "ssh-delfos-datakitchen"

DEFAULT_ARGS = {
    "owner": "ANALISIS_PLANEACION_FINANCIERA",
    "start_date": datetime(2020,1,1),
}

if ENV == "dev":
    ACCOUNT_NUMBER = '668447462549 '
    ENV_MESH = "dev"
elif ENV == "qa":
    ACCOUNT_NUMBER = '823003472832'
    ENV_MESH = "qc"
elif ENV == "pdn":
    ACCOUNT_NUMBER = '435718222562'
    ENV_MESH = "pdn"

TAGS = [
    f"env.{ENV.upper()}",
    "teams.DELFOS",
    f"dominio.{DOMAIN.upper()}",
    f"subdominio.{SUBDOMAIN.upper()}",
    f"pais.{COUNTRY.upper()}",
    "criticidad.P3",
]

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

@dag(
    dag_id=DAG_ID,
    description=DAG_DESCRIPTION,
    schedule_interval=DAG_SCHEDULE,
    catchup=False,
    tags=TAGS,
    default_args=DEFAULT_ARGS,
)
def workflow():
    """Executes the workflow for the CO_DELFOS_CLIENTES_PRN_MODELO_BI_EMPRENDEDORES_ME01 DAG.

    This function defines the tasks and their dependencies for the DAG workflow. 
    It consists of three TaskGroups:
    - ingestion_tasks: Contains tasks related to data ingestion and processing.
    - prediction_tasks: Contains tasks related to prediction and model processing.
    - postprocessing_tasks: Contains tasks related to post processing steps.

    Task Dependencies:
    - The ingestion_tasks are executed sequentially, with each task depending on
    the successful completion of the previous task.
    - The prediction_tasks are executed after the ingestion_tasks have completed 
    successfully.
    - postprocessing_tasks are executed after the The prediction_tasks have completed 
    successfully.

    """
    task_start = EmptyOperator(task_id="start_process")

    dbt_athena_run_model = DBTDelfosOperator(
        task_id = "dbt_athena_fondeo",
        ssh_conn_id = DBT_CONNECTION_ID,
        domain = DOMAIN,
        subdomain = SUBDOMAIN,
        environment = ENV_MESH,
        account = ACCOUNT_NUMBER,
        repository = 'coDelfosFinanciera_pln_DBT',
        mode='run',
        args_mode='--select fondeo_credito',
        dbt_type='ATHENA',
        variables={
            "fecha_actualizacion": EXECUTION_YEAR_MONTH,
            "env": ENV_MESH
        },
    )

    task_end = EmptyOperator(task_id="end_process")

    task_start >> dbt_athena_run_model >> task_end

workflow()