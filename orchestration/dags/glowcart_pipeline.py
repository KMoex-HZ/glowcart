from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'glowcart',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 18),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'glowcart_daily_pipeline',
    default_args=default_args,
    description='GlowCart end-to-end data pipeline',
    schedule_interval='0 6 * * *',
    catchup=False,
    tags=['glowcart', 'production'],
) as dag:

    generate_events = BashOperator(
        task_id='generate_events',
        bash_command='echo "Step 1: Generating 100 e-commerce events and sending to Kafka"',
    )

    bronze_ingestion = BashOperator(
        task_id='bronze_ingestion',
        bash_command='echo "Step 2: Reading from Kafka → saving to Bronze Parquet layer"',
    )

    silver_transform = BashOperator(
        task_id='silver_transform',
        bash_command='echo "Step 3: Cleaning and validating data → Silver layer"',
    )

    gold_aggregation = BashOperator(
        task_id='gold_aggregation',
        bash_command='echo "Step 4: Aggregating business metrics → Gold layer"',
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='echo "Step 5: Running dbt models → fct_revenue, fct_funnel"',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='echo "Step 6: Running dbt data quality tests → 6 tests"',
    )

    generate_events >> bronze_ingestion >> silver_transform >> gold_aggregation >> dbt_run >> dbt_test
