# WAU Project

This repository contains Airflow DAGs for loading and transforming user session data from Snowflake to build a Weekly Active Users (WAU) chart in Preset.

## DAGs
- **etl_raw_sessions.py**: Loads data into `raw.user_session_channel` and `raw.session_timestamp` tables.
- **elt_session_summary.py**: Joins raw tables into `analytics.session_summary` and includes a duplicate session check.
- **seed_sample_data.py**: Seeds test data into Snowflake for validation.

## BI / Preset
- **Dataset**: `analytics.session_summary`
- **Chart**: *Weekly Active Users (WAU)*  
  - Metric: `COUNT_DISTINCT(USER_ID)` labeled **WAU**  
  - Time Column: `SESSION_START_TS`  
  - Time Grain: `Week`

## How to Run
1. Start Airflow (`docker compose up -d`).
2. Trigger DAGs in order:
   - `etl_raw_sessions`
   - `elt_session_summary`
3. Open Preset → Create chart from `analytics.session_summary`.
