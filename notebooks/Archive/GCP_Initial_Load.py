
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.models.baseoperator import chain
import datetime



with DAG(
    "bigquery_load",
    start_date=datetime.datetime(2023, 6, 30),
    description="Example DAG showcasing loading and data quality checking with BigQuery.",
    schedule_interval= "@daily",
    catchup=False,
) as dag:


    check_table_exists = BigQueryTableExistenceSensor( 
    task_id="check_for_table",
    project_id="cloud-warehouse-389415",
    dataset_id=nmckinney_dev,
    table_id=polygon_stock_data,
    dag = dag
    )

    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")




    chain(
        begin,
        check_for_table,
        end,
    )