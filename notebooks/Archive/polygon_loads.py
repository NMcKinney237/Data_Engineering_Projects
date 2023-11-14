import airflow as af
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import pandas as pd
from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator



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

    bigquery_client = bigquery.Client()

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