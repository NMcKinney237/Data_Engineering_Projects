
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dbt_transform',
    default_args=default_args,
    start_date = datetime(2023,5,17),
    schedule = timedelta(days=1),
    catchup = False,
    description='DAG to run dbt job',


)


dbt_run_task = BashOperator(
    task_id='dbt_run',
    bash_command='dbt run',
    dag=dag,
)


