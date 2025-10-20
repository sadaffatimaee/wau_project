from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models.baseoperator import chain
from pathlib import Path

# Default settings for the DAG
default_args = {"owner": "data-eng", "retries": 0}

# Path to the SQL file that builds session_summary
SQL_PATH = "/opt/airflow/dags/sql/create_session_summary.sql"

# Define the DAG
with DAG(
    dag_id="elt_session_summary",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",      # You can change this to None for manual runs
    catchup=False,
    default_args=default_args,
    description="Join raw tables into analytics.session_summary and check for duplicates",
    tags=["elt", "snowflake", "analytics"],
) as dag:

    # Task 1: Build the session_summary table
    build_summary = SnowflakeOperator(
        task_id="build_session_summary",
        snowflake_conn_id="snowflake_default",
        sql=Path(SQL_PATH).read_text(),
    )

    # Task 2: Check for duplicate session_id values
    duplicate_check = SnowflakeOperator(
        task_id="duplicate_check",
        snowflake_conn_id="snowflake_default",
        sql="""
        -- Check for duplicate session_ids and return a simple message
        with d as (
          select session_id, count(*) as c
          from analytics.session_summary
          group by session_id
          having count(*) > 1
        )
        select
          case
            when count(*) = 0 then 'OK - no duplicates found'
            else concat('WARNING: ', count(*), ' duplicate session_id(s) found')
          end as status
        from d;
        """,
    )

    # Set task order
    chain(build_summary, duplicate_check)

