from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# --- Pipeline Configurations ---
# Using standard production retries and delays
default_args = {
    'owner': 'glowcart',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 18),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'glowcart_daily_pipeline',
    default_args=default_args,
    description='End-to-end Data Pipeline for GlowCart E-Commerce Analytics',
    schedule='0 6 * * *',  # Updated to modern 'schedule' parameter
    catchup=False,
    tags=['glowcart', 'ingestion', 'transformation', 'dbt'],
) as dag:

    # Task 1: Data Ingestion Layer (Kafka Producer)
    # Note: Replace 'echo' with actual command like 'python3 /path/to/script.py'
    generate_events = BashOperator(
        task_id='generate_events',
        bash_command='echo "Generating e-commerce events and streaming to Kafka..."',
    )

    # Task 2: Bronze Layer (Raw Data Ingestion to Parquet)
    bronze_ingestion = BashOperator(
        task_id='bronze_ingestion',
        bash_command='echo "Ingesting data from Kafka to Bronze Parquet storage..."',
    )

    # Task 3: Silver Layer (Data Cleaning & Quality Validation)
    silver_transform = BashOperator(
        task_id='silver_transform',
        bash_command='echo "Running custom pandas validation and Silver layer transformation..."',
    )

    # Task 4: Gold Layer (Business Logic Aggregation)
    gold_aggregation = BashOperator(
        task_id='gold_aggregation',
        bash_command='echo "Aggregating metrics into Gold layer tables..."',
    )

    # Task 5: Analytics Engineering (dbt Models)
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='echo "Executing dbt models: fct_revenue, fct_funnel..."',
    )

    # Task 6: Data Quality Assurance (dbt Tests)
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='echo "Running dbt data quality assertions..."',
    )

    # Define linear dependency chain
    generate_events >> bronze_ingestion >> silver_transform >> gold_aggregation >> dbt_run >> dbt_test