import airflow as af
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import pandas as pd
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator # example code: https://registry.astronomer.io/dags/simple_bigquery/versions/1.4.0



def get_aggregate_data(ticker, from_date, to_date, adjusted=True, limit=5000):

    rest_client = RESTClient(api_key="bt70lDEhuX6JBTu35vVU8Dbekd7pLWGt")
    get_aggregates = rest_client.get_aggs(
        ticker=ticker,
        multiplier=1,
        timespan="day",
        from_=from_date,
        to=to_date,
        adjusted=adjusted,
        sort="asc",
        limit=limit,
    )


    agg_data = []
    for agg in get_aggregates:
        agg_data.append(agg)

    data = {}
    for column in agg_data[0].__dict__.keys():
        data[column] = [getattr(agg, column) for agg in agg_data]

    df = pd.DataFrame(data)
    df["ticker"] = ticker


    # Add a column for the date
    df["date"] = df["timestamp"].apply(lambda x: datetime.fromtimestamp(x / 1000))

    return df


def load_data(df):
    df.to_parquet(f"gs://my-bucket/polygon_techstocks_data/{df['date'].dt.strftime('%Y-%m-%d')}.parquet")

    # Load the Parquet file to the warehouse
    # TODO: replace this with BigQueryInsertJobOperator
    BigQueryOperator(
        task_id="load_data",
        src=f"gs://my-bucket/polygon_techstocks_data/{df['date'].dt.strftime('%Y-%m-%d')}.parquet",
        destination="bigquery-public-data.polygon_techstocks_data",
        table="polygon_techstocks_data",
    )


with af.DAG(dag_id="initial_load", schedule_interval=None, start_date=days_ago(1)) as dag:
    get_data = PythonOperator(
        task_id="get_data",
        python_callable=get_aggregate_data,
        op_kwargs={
            "tickers": ["NVDA", "AAPL", "GOOG", "AMZN", "MSFT", "INTC", "IBM", "FB", "TSM", "TSLA"],
            "from_date": "2018-01-01",
            "to_date": datetime.now().strftime("%Y-%m-%d"),
        },
    )

    # TODO: remove this as this doesn't exist anymore
    # bigquery_client = bigquery.Client()

    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
        op_kwargs={
            "client": bigquery_client,
            "dataset_id": "cloud-warehouse-389415.jaffle_shop",
            "table_id": "polygon__daily_stock_prices",
        },
    )

    get_data >> load_data


with af.DAG(dag_id="incremental_load", schedule_interval="@daily", start_date=days_ago(1)) as dag:
    get_data = PythonOperator(
        task_id="get_data",
        python_callable=get_aggregate_data,
        op_kwargs={
            "tickers": ["NVDA", "AAPL", "GOOG", "AMZN", "MSFT", "INTC", "IBM", "FB", "TSM", "TSLA"],
            "from_date": datetime.datetime.now().strftime("%Y-%m-%d"),
        },
    )

    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
        op_kwargs={
        "client": bigquery_client,
        "dataset_id": "cloud-warehouse-389415.jaffle_shop",
        "table_id": "polygon_techstocks_data",})
    
# TODO: your tasks should generally look like this given the latest bigquery api changes. The above will need to be removed or updated to work with the latest bigquery api

    # """
    # #### BigQuery dataset creation
    # Create the dataset to store the sample data tables.
    # """
    # create_dataset = BigQueryCreateEmptyDatasetOperator(
    #     task_id="create_dataset", dataset_id=DATASET
    # )

    # """
    # #### BigQuery table creation
    # Create the table to store sample forest fire data.
    # """
    # create_table = BigQueryCreateEmptyTableOperator(
    #     task_id="create_table",
    #     dataset_id=DATASET,
    #     table_id=TABLE,
    #     schema_fields=[
    #         {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
    #         {"name": "y", "type": "INTEGER", "mode": "NULLABLE"},
    #         {"name": "month", "type": "STRING", "mode": "NULLABLE"},
    #         {"name": "day", "type": "STRING", "mode": "NULLABLE"},
    #         {"name": "ffmc", "type": "FLOAT", "mode": "NULLABLE"},
    #         {"name": "dmc", "type": "FLOAT", "mode": "NULLABLE"},
    #         {"name": "dc", "type": "FLOAT", "mode": "NULLABLE"},
    #         {"name": "isi", "type": "FLOAT", "mode": "NULLABLE"},
    #         {"name": "temp", "type": "FLOAT", "mode": "NULLABLE"},
    #         {"name": "rh", "type": "FLOAT", "mode": "NULLABLE"},
    #         {"name": "wind", "type": "FLOAT", "mode": "NULLABLE"},
    #         {"name": "rain", "type": "FLOAT", "mode": "NULLABLE"},
    #         {"name": "area", "type": "FLOAT", "mode": "NULLABLE"},
    #     ],
    # )

    # """
    # #### BigQuery table check
    # Ensure that the table was created in BigQuery before inserting data.
    # """
    # check_table_exists = BigQueryTableExistenceSensor(
    #     task_id="check_for_table",
    #     project_id="{{ var.value.gcp_project_id }}",
    #     dataset_id=DATASET,
    #     table_id=TABLE,
    # )

    # """
    # #### Insert data
    # Insert data into the BigQuery table using an existing SQL query (stored in
    # a file under dags/sql).
    # """
    # load_data = BigQueryInsertJobOperator(
    #     task_id="insert_query",
    #     configuration={
    #         "query": {
    #             "query": "{% include 'load_bigquery_forestfire_data.sql' %}",
    #             "useLegacySql": False,
    #         }
    #     },
    # )
