from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

DBT_PROJECT_DIR = os.environ.get(
    "DBT_PROJECT_DIR",
    "/opt/airflow/dbt/youtube_analytics"  # default fallback (update as needed)
)

DBT_CMD = f"dbt --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 2, 13),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "youtube_analytics_dbt",
    default_args=default_args,
    description="Run dbt models for YouTube analytics with data quality checks",
    schedule_interval="@daily",
    catchup=False,
) as dag:

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"{DBT_CMD} run --select staging",
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"{DBT_CMD} test --select staging",
    )

    dbt_run_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command=f"{DBT_CMD} run --select intermediate",
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"{DBT_CMD} run --select marts",
    )

    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=f"{DBT_CMD} test --select marts",
    )

    data_quality_check = BashOperator(
        task_id="data_quality_check",
        bash_command=f"{DBT_CMD} test --select data_quality_metrics",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    dbt_run_staging >> dbt_test_staging >> dbt_run_intermediate >> dbt_run_marts >> dbt_test_marts >> data_quality_check
