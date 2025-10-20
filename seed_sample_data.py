from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

SQL = r"""
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE OR REPLACE TABLE raw.user_session_channel (
  session_id      VARCHAR,
  user_id         VARCHAR,
  channel         VARCHAR,
  utm_source      VARCHAR,
  utm_campaign    VARCHAR
);

CREATE OR REPLACE TABLE raw.session_timestamp (
  session_id        VARCHAR,
  session_start_ts  TIMESTAMP_NTZ,
  session_end_ts    TIMESTAMP_NTZ
);

INSERT INTO raw.user_session_channel VALUES
  ('s001','u01','web','google','fall_sale'),
  ('s002','u02','web','facebook','fall_sale'),
  ('s003','u01','ios','email','onboarding'),
  ('s004','u03','android','google','retargeting'),
  ('s005','u04','web','twitter','promo'),
  ('s006','u02','ios','email','newsletter'),
  ('s007','u05','android','google','brand'),
  ('s008','u01','web','google','brand'),
  ('s009','u04','ios','email','onboarding'),
  ('s010','u06','web','facebook','promo');

INSERT INTO raw.session_timestamp VALUES
  ('s001','2025-07-29 10:05','2025-07-29 10:35'),
  ('s002','2025-08-03 14:10','2025-08-03 14:45'),
  ('s003','2025-08-10 09:01','2025-08-10 09:20'),
  ('s004','2025-08-18 18:30','2025-08-18 19:05'),
  ('s005','2025-08-24 12:00','2025-08-24 12:25'),
  ('s006','2025-09-02 08:12','2025-09-02 08:40'),
  ('s007','2025-09-07 16:50','2025-09-07 17:15'),
  ('s008','2025-09-15 11:05','2025-09-15 11:25'),
  ('s009','2025-09-23 20:00','2025-09-23 20:35'),
  ('s010','2025-10-01 07:45','2025-10-01 08:05');

CREATE OR REPLACE TABLE analytics.session_summary AS
SELECT
  st.session_id,
  usc.user_id,
  usc.channel,
  st.session_start_ts,
  st.session_end_ts,
  usc.utm_source,
  usc.utm_campaign
FROM raw.session_timestamp st
JOIN raw.user_session_channel usc
  ON usc.session_id = st.session_id;
"""

with DAG(
    dag_id="seed_sample_data",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "data-eng", "retries": 0},
    description="Seed raw tables with sample rows and build analytics.session_summary",
    tags=["seed","snowflake"],
) as dag:
    SnowflakeOperator(
        task_id="seed_all",
        snowflake_conn_id="snowflake_default",
        sql=SQL,
    )
