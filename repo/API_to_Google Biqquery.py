from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta



# Constants
MY_NAME = "Nathan McKinney"
MY_NUMBER = 100

def multiply_by_23(number):
    '''Multiplies input by 23'''
    result = number * 23
    print(result)

default_args = {
    "owner": "Nathan McKinney",
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

with DAG(

    dag_id = "airflow_getting_started_p1",
    start_date = datetime(2023,5,17),
    schedule = timedelta(days=7),
    catchup = False,
    tags = ["tutorial"],
    default_args = default_args
) as dag:


    t1 = BashOperator(
        task_id = "say_my_name",
        bash_command=f"echo {MY_NAME}"
    )

    t2 = PythonOperator(
        task_id = "multiply_my_number",
        python_callable = multiply_by_23,
        op_kwargs={"number": MY_NUMBER}
    )

    t1 >> t2


