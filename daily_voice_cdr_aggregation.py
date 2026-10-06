"""
DAG: daily_voice_cdr_aggregation
Purpose: Daily rollup of raw telecommunication CDRs into daily summary table in Trino Iceberg.
Schedule: Daily at 01:00 AM (Africa/Mogadishu EAT)
"""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.trino.hooks.trino import TrinoHook

# Set EAT local timezone
LOCAL_TZ = pendulum.timezone("Africa/Mogadishu")

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="daily_voice_cdr_aggregation",
    description="Summarize raw CDRs from voice_lab into daily aggregation table",
    default_args=default_args,
    schedule="0 1 * * *",  # Nightly at 01:00 AM EAT
    start_date=pendulum.datetime(2026, 9, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "cdrs", "aggregation"],
) as dag:

    def build_daily_summary(**context) -> None:
        """
        Executes aggregation query in Trino for the logical date (ds).
        """
        trino = TrinoHook(trino_conn_id="trino_lakehouse")
        ds = context.get("ds") or str(pendulum.now(LOCAL_TZ).subtract(days=1).date())
        
        sql = f"""
        INSERT INTO lakehouse.cdrs.voice_lab_daily_summary
        SELECT 
            el_record_date, 
            status, 
            COUNT(*) AS call_count,
            SUM(CAST(waitduration AS BIGINT)) AS total_wait,
            CURRENT_TIMESTAMP AS summary_generated_at
        FROM lakehouse.cdrs.voice_lab
        WHERE el_record_date = DATE '{ds}'
        GROUP BY el_record_date, status;
        """
        print(f"Executing aggregation query for date: {ds}")
        trino.run(sql)
        print(f"Aggregation complete for date: {ds}")

    summarize_task = PythonOperator(
        task_id="build_daily_summary",
        python_callable=build_daily_summary,
    )
