"""
weather_dag.py — Daily weather ETL pipeline.

Task flow:
  extract → transform → load_raw → dbt_run → quality_check

  extract    : fetch raw JSON from Open-Meteo
  transform  : parse JSON into DataFrames
  load_raw   : upsert raw_df into raw_weather (ON CONFLICT DO NOTHING)
  dbt_run    : build daily_summary from raw_weather via dbt
  quality_check : assert both tables have rows
"""

import logging
import sys

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

sys.path.insert(0, "/opt/airflow")

log = logging.getLogger(__name__)

DBT_CMD = (
    "dbt run "
    "--project-dir /opt/airflow/dbt "
    "--profiles-dir /opt/airflow/dbt"
)


# ── Task callables ─────────────────────────────────────────────────────────────

def run_extract(**context):
    from pipeline.extract import extract
    raw_data = extract()
    context["ti"].xcom_push(key="raw_data", value=raw_data)
    log.info("extract complete")


def run_transform(**context):
    from pipeline.transform import transform

    raw_data = context["ti"].xcom_pull(key="raw_data", task_ids="extract")
    raw_df, _ = transform(raw_data)  # daily_summary built by dbt

    context["ti"].xcom_push(
        key="raw_df",
        value=raw_df.to_json(orient="records", date_format="iso"),
    )
    log.info("transform complete — %d rows", len(raw_df))


def run_load_raw(**context):
    import pandas as pd
    from pipeline.load import load

    raw_json = context["ti"].xcom_pull(key="raw_df", task_ids="transform")
    raw_df = pd.read_json(raw_json, orient="records")
    raw_df["date"] = pd.to_datetime(raw_df["date"])

    load(raw_df)
    log.info("load_raw complete")


def run_quality_check(**context):
    import os
    from sqlalchemy import create_engine, text

    db_url = os.getenv(
        "WEATHER_DB_CONN",
        "postgresql+psycopg2://admin:password@weather_db:5432/weather",
    )
    engine = create_engine(db_url)

    with engine.connect() as conn:
        for table in ("raw_weather", "daily_summary"):
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            log.info("Quality check — %s: %d rows", table, count)
            if count == 0:
                raise ValueError(f"Quality gate FAILED: {table} has 0 rows")

    log.info("All quality checks passed")


# ── DAG definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="weather_etl",
    description="Daily ETL: Open-Meteo → PostgreSQL (raw via Python, summary via dbt)",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["weather", "etl", "dbt"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=run_extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=run_transform,
    )

    load_raw_task = PythonOperator(
        task_id="load_raw",
        python_callable=run_load_raw,
    )

    dbt_task = BashOperator(
        task_id="dbt_run",
        bash_command=DBT_CMD,
    )

    quality_task = PythonOperator(
        task_id="quality_check",
        python_callable=run_quality_check,
    )

    extract_task >> transform_task >> load_raw_task >> dbt_task >> quality_task
