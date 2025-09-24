"""This module defines a custom Airflow Operator to run dbt models in Delfos."""

from __future__ import annotations
import json
from typing import Literal, Optional, Sequence
from airflow.providers.ssh.operators.ssh import SSHOperator


class DBTDelfosOperator(SSHOperator):
    """
    Custom Airflow Operator to run dbt models in Delfos.

    This operator extends the SSHOperator to specifically run dbt commands
    on Redshift, Athena and Glue by constructing a shell command that is executed
    on a remote server. It supports dynamic selection of dbt models to run or test
    models, and allows for the specification of additional environment variables.

    Attributes:
        domain (str): Domain of the AWS account where the model will be executed.
        subdomain (str): Domain of the AWS account where the model will be executed.
        account (str): AWS account number where the dbt model will be executed.
        environment (str): The environment (qc, pdn) where the dbt process will run.
        repository (str): GitHub repository name containing the dbt models.
        mode (Literal['run', 'test']): The dbt command to execute (run or test).
        args_mode (str): Additional arguments for the dbt command (e.g., model selection).
        dbt_type (Literal['REDSHIFT', 'ATHENA', 'GLUE']): AWS service for data consumption or transformation by dbt.
        variables (Optional[dict]): Additional variables to pass to the dbt command.
        conn_timeout (int): The timeout for the SSH connection, by default 60.
        command (str): Command to execute on the remote server.
    """

    # pylint: disable-next=S107
    template_fields: Sequence[str] = (
        'domain',
        'subdomain',
        'account',
        'environment',
        'repository',
        'mode',
        'args_mode',
        'dbt_type',
        'variables',
        'conn_timeout',
        'command',
    )

    def __init__(
        self,
        domain: str,
        subdomain: str,
        account: str,
        environment: str,
        repository: str,
        mode: Literal['run', 'test'],
        args_mode: str,
        dbt_type: Literal['REDSHIFT', 'ATHENA', 'GLUE'],
        variables: dict | None = None,
        conn_timeout: int = 60,
        do_xcom_push: bool = True,
        cmd_timeout: int = 20 * 60,
        **kwargs
    ) -> None:
        """
        Initialize a new instance of the DBTDelfosOperator.

        This constructor sets up the DBTDelfosOperator with all the necessary parameters to execute dbt commands on
        remote servers. It configures the connection details, dbt environment, and command to be executed.

        Args:
            domain (str): Domain of the AWS account where the model will be executed.
            subdomain (str): Subdomain of the AWS account where the model will be executed.
            account (str): AWS account number where the dbt model will be executed.
            environment (str): The environment (qc, pdn) where the dbt process will run.
            repository (str): GitHub repository name containing the dbt models.
            mode (Literal['run', 'test']): The dbt command to execute (run or test).
            args_mode (str): Additional arguments for the dbt command (e.g., model selection).
            dbt_type (Literal['REDSHIFT', 'ATHENA', 'GLUE']): AWS service for data consumption or transformation by dbt.
            variables (Optional[dict]): Additional variables to pass to the dbt command.
            conn_timeout (int): The timeout for the SSH connection, by default 60.
            command (str): Command to execute on the remote server.
            do_xcom_push (bool): Whether to push the command execution result to XCom.
            cmd_timeout (int): The timeout for the dbt command execution, by default 20 minutes.

        **kwargs: Additional keyword arguments passed to the SSHOperator.

        The constructor also prepares the shell command to be executed on the remote server, based on the provided
        parameters and the dbt environment configuration.
        """
        self.domain = domain.lower()
        self.subdomain = subdomain.lower()
        self.account = account
        self.environment = environment.lower()
        self.repository = repository
        self.mode = mode
        self.args_mode = args_mode
        self.dbt_type = dbt_type
        self.variables = variables

        if self.variables is None:
            self.variables = {}

        self.map_dbt_connectors = {
            "ATHENA": {
                "profile": "nequimesh_athena",
                "target": "athena"
            },
            "REDSHIFT": {
                "profile": "nequidw",
                "target": self.domain
            },
            "GLUE": {
                "profile": "nequimesh_glue",
                "target": "glue"
            }
        }

        command = f"""
        export PROGRAM="DBT_{self.dbt_type}" && 
        export MODE="{self.mode} {self.args_mode}" && 
        export PROFILE="{self.map_dbt_connectors[self.dbt_type]["profile"]}" && 
        export TARGET="{self.map_dbt_connectors[self.dbt_type]["target"]}" && 
        export DOMAIN="{self.domain}" && 
        export ACCOUNT="{self.account}" && 
        export MODEL_PATH="$DOMAIN/{self.repository}/model_{self.domain}_{self.subdomain}_{self.dbt_type.lower()}/" && 
        export ENVIRONMENT="{self.environment}" && 
        export VARS='{json.dumps(self.variables)}' && 
        cd ~/scripts/coDelfosAnalytics_DBTRunner_Lib && 
        bash ./DBTRunner.sh "$PROGRAM" "$MODE" "$PROFILE" "$TARGET" "$MODEL_PATH" "$DOMAIN" "$ACCOUNT" "$ENVIRONMENT" \
        "$VARS"
        """
        self.command = command
        kwargs['command'] = command
        kwargs['conn_timeout'] = conn_timeout
        kwargs['do_xcom_push'] = do_xcom_push
        kwargs['cmd_timeout'] = cmd_timeout
        self.ui_color = "#4887f1"
        self.ui_fgcolor = "#f3f3f3"

        super().__init__(**kwargs)