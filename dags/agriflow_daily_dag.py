from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 1. Default settings for all tasks
default_args = {
    'owner': 'agriflow_admin',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# 2. Define the DAG
with DAG(
    'agriflow_daily_etl',
    default_args=default_args,
    description='End-to-End Daily ETL pipeline for AgriFlow Intelligence',
    schedule_interval='@daily',        # Runs once every night at midnight
    start_date=datetime(2026, 2, 19),  # Set to a past date so it triggers immediately on creation
    catchup=False,
    tags=['agriflow', 'etl'],
) as dag:

    # ---------------------------------------------------------
    # 3. Define the Tasks
    # Note: We use the BashOperator to execute the scripts we already wrote.
    # cwd='/opt/airflow' ensures it runs in the correct root directory inside Docker.
    # ---------------------------------------------------------

    # Task 1: Simulate Source System (Generate internal farm logs)
    generate_internal_data = BashOperator(
        task_id='generate_harvest_logs',
        bash_command='python scripts/generators/generate_harvests.py',
        cwd='/opt/airflow'
    )

    # Task 2: Simulate External API (Generate market prices)
    generate_external_data = BashOperator(
        task_id='fetch_market_prices',
        bash_command='python scripts/generators/mock_market_api.py',
        cwd='/opt/airflow'
    )

    # Task 3: Ingestion Layer (Move raw files to staging)
    ingest_data = BashOperator(
        task_id='ingest_to_staging',
        bash_command="sed -i 's/\\r$//' scripts/ingest.sh && bash scripts/ingest.sh ",
        cwd='/opt/airflow'
    )

    # Task 4: ETL Process (Extract, Transform, Load into Postgres)
    run_etl = BashOperator(
        task_id='run_etl_pipeline',
        bash_command='python scripts/etl/load.py',
        cwd='/opt/airflow'
    )

    # ---------------------------------------------------------
    # 4. Define the Execution Flow (Dependencies)
    # ---------------------------------------------------------
    # Both generators can run at the same time. Once BOTH are done, ingestion starts.
    [generate_internal_data, generate_external_data] >> ingest_data >> run_etl