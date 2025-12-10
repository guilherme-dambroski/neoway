from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pendulum import timezone

default_args = {
  'owner': 'airflow',
  'depends_on_past': False,
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
}

with DAG(
  'neoway_dbt_orchestration',
  default_args=default_args,
  description='Pipeline dbt-duckdb',
  schedule='0 6 * * *',
  start_date=datetime(2025, 1, 1, tzinfo=timezone('America/Sao_Paulo')),
  catchup=False,
  tags=['dbt','neoway'],
) as dag:

  dbt_debug = BashOperator(
    task_id='dbt_debug',
    bash_command='cd /opt/airflow/dbt && dbt debug --target prod',
  )

  dbt_seed = BashOperator(
    task_id='dbt_seed',
    bash_command='cd /opt/airflow/dbt && dbt seed --target prod',
  )

  dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /opt/airflow/dbt && dbt run --target prod',
  )

  dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='cd /opt/airflow/dbt && dbt test --target prod',
  )

  dbt_debug >> dbt_seed >> dbt_run >> dbt_test
