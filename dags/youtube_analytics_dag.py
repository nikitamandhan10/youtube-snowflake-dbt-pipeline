from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

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
    description='Run dbt models for YouTube analytics',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /Users/nikita/Downloads/youtube-snowflake-dbt-pipeline/youtube_analytics && dbt run',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /Users/nikita/Downloads/youtube-snowflake-dbt-pipeline/youtube_analytics && dbt test',
    )

    dbt_run >> dbt_test
