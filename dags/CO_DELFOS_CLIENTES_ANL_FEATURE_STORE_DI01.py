"""DAG CO_DELFOS_CLIENTES_ANL_FEATURE_STORE_DI01"""
import calendar
from datetime import datetime
from dbt_delfos_operator import DBTDelfosOperator
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.opsgenie.operators.opsgenie import OpsgenieCreateAlertOperator

DAG_DESCRIPTION = """
    Este DAG tiene el proceso de orquestación de todas las features del 
    dominio de clientes de Nequi.
"""
DAG_SCHEDULE = "59 23 * * *"
EXECUTION_DATE = "{{execution_date.strftime('%Y-%m-%d')}}"
ENV = Variable.get("env")
DOMAIN = "clientes"
SUBDOMAIN = "anl"
COUNTRY = "co"
DAG_ID = f"{COUNTRY.upper()}_DELFOS_{DOMAIN.upper()}_{SUBDOMAIN.upper()}_FEATURE_STORE_DI01"
OPSGENIE_CONNECTION_ID = "opsgenie-dataops"
DBT_CONNECTION_ID = "ssh-delfos-datakitchen"

if ENV == "dev":
    ACCOUNT_NUMBER = '460311739291'
    ENV_MESH = "dev"
elif ENV == "qa":
    ACCOUNT_NUMBER = '465924778008'
    ENV_MESH = "qc"
elif ENV == "pdn":
    ACCOUNT_NUMBER = '158304509201'
    ENV_MESH = "pdn"

TAGS = [
    f"env.{ENV.upper()}",
    "teams.DELFOS",
    f"dominio.{DOMAIN.upper()}",
    f"subdominio.{SUBDOMAIN.upper()}",
    f"pais.{COUNTRY.upper()}",
    "criticidad.P1",
]
SLACK_CHANNEL = f"airflow-data-notifications-{ENV}"
DEFAULT_ARGS = {
    "owner": "INTELIGENCIA_DE_NEGOCIO",
    "start_date": datetime(2023, 5, 1),
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

FEATURE_CATALOG = [
    {
        "subdomain": "prn",
        "execution_groups": ["daily", "weekly", "monthly"]
    },
    {
        "subdomain": "prj",
        "execution_groups": ["daily", "weekly"]
    }
]

class DomainFeatureExecution:
    """Class that creates an Airflow DAG for the orchestration workflow of 
    the Feature Store.
    """
    def __init__(self, feature_catalog: list, execution_date: str):
        """Initiates an instance of the DomainFeatureExecution class.
        
        Args:
            feature_catalog (list): List that contains dictionary objects with the data of each
                subdomain that has features to be executed and orchestrated.
            execution_date (str): Jinja template for the execution date of the pipeline.

        """
        self.__feature_catalog = feature_catalog
        self.__execution_date = execution_date
        self.__validation_statements = {
            "daily": lambda x: True,
            "weekly": lambda x: calendar.day_name[x.weekday()] == "Monday",
            "monthly": lambda x: x.day == 1,
            "yearly": lambda x: (x.day == 1) and (x.month == 1)
        }

    def __generate_execution_group_workflow(
        self, subdomain_name: str, 
        group_name: str
    ) -> TaskGroup:
        """Generates a Task Group with the workflow implementation of a subdomain execution group.
        
        Args:
            subdomain_name (str): Name of the Subdomain.
            group_name (str): Name of the Execution group.
            
        Returns:
            group_workflow (TaskGroup): Execution Group workflow.

        """
        with TaskGroup(f"{group_name}_execution_features") as group_workflow:
            general_task_path = f"{subdomain_name}_workflow.{group_name}_execution_features."
            general_dbt_command = (
                f"--select path:models/feature_store,tag:{group_name},tag:{subdomain_name}"
            )

            @task(task_id="summit_dbt_run_state")
            def summit_dbt_run_state(**kwargs):
                """Validate the state of the previous dbt run task and returns the next command.
                
                Returns:
                    dbt_run_command (str): DBT command to be executed in the next run.

                """
                task_instance = kwargs['ti']
                previous_task_state = task_instance.xcom_pull(
                    task_ids=(
                        general_task_path +
                        f"dbt_athena_run_{group_name}_{subdomain_name}_features"
                    ),
                    key="ssh_exit"
                )

                if not previous_task_state:
                    return general_dbt_command
                return general_dbt_command + ",result:error --state target"

            @task(task_id="testing_task")
            def testing_task():
                print("This is a testing task")

            summit_dbt_features_executions = testing_task()

            # summit_dbt_features_executions = DBTDelfosOperator(
            #     task_id=f"dbt_athena_run_{group_name}_{subdomain_name}_features",
            #     ssh_conn_id=DBT_CONNECTION_ID,
            #     domain=DOMAIN,
            #     subdomain=subdomain_name,
            #     environment=ENV_MESH,
            #     account=ACCOUNT_NUMBER,
            #     repository=f"{COUNTRY}Delfos{DOMAIN.capitalize()}_{subdomain_name}_DBT",
            #     mode="run",
            #     args_mode=f"{{{{ ti.xcom_pull(task_ids='{general_task_path + 'summit_dbt_run_state'}', key='return_value') }}}}",
            #     dbt_type="ATHENA",
            #     variables={"execution_date": EXECUTION_DATE, "env": ENV_MESH}
            # )

            summit_dbt_run_state() >> summit_dbt_features_executions

        return group_workflow

    def __generate_branching_task(
        self,
        subdomain_name: str,
        execution_groups: list,
    ):
        """Generates the branching task base on a specific list of execution groups.
        
        Args:
            subdomain_name (str): Name of the Nequi's Subdomain.
            execution_groups (list): List of the Executions groups that are presented in the 
            subdomain.

        Returns:
            choose_execution_group (TaskDecorator): Function that generates a choosing task.

        """
        @task.branch(task_id=f"{subdomain_name}_choose_execution_group")
        def choose_execution_group(execution_date: str):
            """Choose the executions groups of the group.
            
            Args:
                execution_date (str): Execution date of the run.

            Returns:
                choosen_branches (list): List of the id's for the first task of the chosen 
                    branches.

            """
            choosen_branches = []
            execution_datetime = datetime.fromisoformat(execution_date)

            for x in execution_groups:
                if self.__validation_statements[x](x=execution_datetime):
                    choosen_branches.append(
                        f"{subdomain_name}_workflow.{x}_execution_features.summit_dbt_run_state"
                    )

            return choosen_branches

        return choose_execution_group

    def __generate_subdomain_workflow(
        self,
        subdomain_name: str,
        execution_groups: list
    ) -> TaskGroup:
        """Generate the Feature Store Execution Workflow of a Subdomain.

        Args:
            subdomain_name (str): Name of the Nequi's Subdomain.
            execution_groups (list): List of the Executions groups that are presented in the 
            subdomain.

        Returns:
            subdomain_workflow (TaskGroup): TaskGroup with the implementation of the subdomain.

        """
        with TaskGroup(f"{subdomain_name}_workflow") as subdomain_workflow:
            choose_execution_group = self.__generate_branching_task(
                subdomain_name, execution_groups
            )(execution_date=self.__execution_date)

            for x in execution_groups:
                summit_exec_group_workflow = self.__generate_execution_group_workflow(
                    subdomain_name=subdomain_name, group_name=x
                )

                choose_execution_group >> summit_exec_group_workflow

        return subdomain_workflow

    def build_workflow(self):
        """Built the Execution workflow."""
        task_start = EmptyOperator(task_id="start_process")
        task_end = EmptyOperator(
            task_id="end_process",
            trigger_rule="none_failed_min_one_success"
        )

        for x in self.__feature_catalog:
            subdomain_workflow = self.__generate_subdomain_workflow(
                subdomain_name=x["subdomain"],
                execution_groups=x["execution_groups"]
            )

            task_start >> subdomain_workflow >>task_end


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
    f"""Executes the workflow for the {DAG_ID} DAG."""
    domain_workflow = DomainFeatureExecution(
        feature_catalog=FEATURE_CATALOG, execution_date=EXECUTION_DATE
    )

    domain_workflow.build_workflow()

workflow()
