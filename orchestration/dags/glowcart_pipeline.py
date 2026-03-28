from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    logging.error(f"SLA MISSED — DAG: {dag.dag_id} | Tasks: {[s.task_id for s in slas]}")

default_args = {
    'owner': 'glowcart',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 18),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=2),
}

with DAG(
    'glowcart_daily_pipeline',
    default_args=default_args,
    description='End-to-end Data Pipeline for GlowCart E-Commerce Analytics',
    schedule='0 6 * * *',
    catchup=False,
    sla_miss_callback=sla_miss_callback,
    tags=['glowcart', 'ingestion', 'transformation', 'dbt'],
) as dag:

    generate_events = BashOperator(
        task_id='generate_events',
        bash_command='cd /root/glowcart && source .venv/bin/activate && python3 ingestion/scripts/bulk_generate.py',
    )

    bronze_ingestion = BashOperator(
        task_id='bronze_ingestion',
        bash_command='cd /root/glowcart && source .venv/bin/activate && python3 -m storage.bronze.kafka_to_bronze',
    )

    silver_transform = BashOperator(
        task_id='silver_transform',
        bash_command='cd /root/glowcart && source .venv/bin/activate && python3 -m storage.silver.bronze_to_silver',
    )

    gold_aggregation = BashOperator(
        task_id='gold_aggregation',
        bash_command='cd /root/glowcart && source .venv/bin/activate && python3 -m storage.gold.silver_to_gold',
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /root/glowcart/transform/dbt && source /root/glowcart/.venv/bin/activate && dbt run',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /root/glowcart/transform/dbt && source /root/glowcart/.venv/bin/activate && dbt test',
    )

    generate_events >> bronze_ingestion >> silver_transform >> gold_aggregation >> dbt_run >> dbt_test
