

# Create the DAG
dag = af.DAG(dag_id="parquet_to_bigquery", schedule_interval="@once")






# Define the tasks
parquet_to_bigquery_task = BigQueryInsertJobOperator(
    task_id="parquet_to_bigquery",
    bucket="test_bucket_nlm",
    source_objects="*.parquet",
    destination_table="polygon__daily_stock_data",
    schema="nmckinney_dev",
    write_disposition="WRITE_TRUNCATE",
)

bigquery_client = bigquery.Client()

# Add the tasks to the DAG
dag.add_task(parquet_to_bigquery_task)

# Run the DAG
af.start_dag(dag)