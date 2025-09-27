from datetime import datetime
from airflow import DAG
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

with DAG(
    dag_id="slack_probe", start_date=datetime(2025,5,1), schedule_interval=None, catchup=False):
    SlackWebhookOperator(
        task_id="send_test",
        slack_webhook_conn_id="slack_alerts",
        message=":wave: Airflow dice hola en *#nuevo-canal*",
    )
