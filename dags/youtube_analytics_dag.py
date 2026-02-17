from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'youtube_analytics_dbt',
    default_args=default_args,
    description='Run dbt models for YouTube analytics with data quality checks',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    dbt_run_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command='cd /Users/nikita/Downloads/youtube-snowflake-dbt-pipeline/youtube_analytics && dbt run --select staging',
    )

    dbt_test_staging = BashOperator(
        task_id='dbt_test_staging',
        bash_command='cd /Users/nikita/Downloads/youtube-snowflake-dbt-pipeline/youtube_analytics && dbt test --select staging',
    )

    dbt_run_intermediate = BashOperator(
        task_id='dbt_run_intermediate',
        bash_command='cd /Users/nikita/Downloads/youtube-snowflake-dbt-pipeline/youtube_analytics && dbt run --select intermediate',
    )

    dbt_run_marts = BashOperator(
        task_id='dbt_run_marts',
        bash_command='cd /Users/nikita/Downloads/youtube-snowflake-dbt-pipeline/youtube_analytics && dbt run --select marts',
    )

    dbt_test_marts = BashOperator(
        task_id='dbt_test_marts',
        bash_command='cd /Users/nikita/Downloads/youtube-snowflake-dbt-pipeline/youtube_analytics && dbt test --select marts',
    )

    data_quality_check = BashOperator(
        task_id='data_quality_check',
        bash_command='cd /Users/nikita/Downloads/youtube-snowflake-dbt-pipeline/youtube_analytics && dbt test --select data_quality_metrics',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    dbt_run_staging >> dbt_test_staging >> dbt_run_intermediate >> dbt_run_marts >> dbt_test_marts >> data_quality_check
