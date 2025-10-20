from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models.baseoperator import chain
from pathlib import Path

SQL_BASE = "/opt/airflow/dags/sql"
default_args = {"owner": "data-eng", "retries": 0}

with DAG(
    dag_id="etl_raw_sessions",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",   # set to None if you want only manual runs
    catchup=False,
    default_args=default_args,
    description="Load raw.user_session_channel and raw.session_timestamp from stage",
    tags=["etl","snowflake","raw"],
) as dag:
    create_raw = SnowflakeOperator(
        task_id="create_raw_tables",
        snowflake_conn_id="snowflake_default",
        sql=Path(f"{SQL_BASE}/create_raw_tables.sql").read_text(),
    )
    copy_user_session_channel = SnowflakeOperator(
        task_id="copy_user_session_channel",
        snowflake_conn_id="snowflake_default",
        sql=Path(f"{SQL_BASE}/copy_raw_user_session_channel.sql").read_text(),
    )
    copy_session_timestamp = SnowflakeOperator(
        task_id="copy_session_timestamp",
        snowflake_conn_id="snowflake_default",
        sql=Path(f"{SQL_BASE}/copy_raw_session_timestamp.sql").read_text(),
    )
    chain(create_raw, [copy_user_session_channel, copy_session_timestamp])

