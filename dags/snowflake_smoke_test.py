from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

with DAG(
    dag_id="snowflake_smoke_test",
    start_date=datetime(2025, 5, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test","snowflake"],
) as dag:

    t = SnowflakeOperator(
        task_id="ping_snowflake",
        snowflake_conn_id="snowflake_default",
        sql="""
            USE WAREHOUSE COMPUTE_WH;
            CREATE OR REPLACE TABLE MEETUP_DB.RAW._airflow_ping (ts TIMESTAMP_NTZ);
            INSERT INTO MEETUP_DB.RAW._airflow_ping SELECT CURRENT_TIMESTAMP();
            SELECT COUNT(*) AS rows_count, MAX(ts) AS last_ts FROM MEETUP_DB.RAW._airflow_ping;
        """,
    )
