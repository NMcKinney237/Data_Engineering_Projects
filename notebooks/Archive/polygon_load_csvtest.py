import airflow as af
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import pandas as pd

def write_to_csv(df, filename):
    df.to_csv(filename, index=False)


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


with af.DAG(dag_id="write_load", schedule_interval=None, start_date=days_ago(1)) as dag:
    get_data = PythonOperator(
        task_id="get_data",
        python_callable=get_aggregate_data,
        op_kwargs={
            "tickers": ["NVDA", "AAPL", "GOOG", "AMZN", "MSFT", "INTC", "IBM", "FB", "TSM", "TSLA"],
            "from_date": "2018-01-01",
            "to_date": datetime.now().strftime("%Y-%m-%d"),
        },
    )

    af.DAG(dag_id="write_to_csv", schedule_interval=None, start_date=days_ago(1)) as dag:

    ## Get the data
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})

    ## Write the data to CSV
    write_to_csv_task = PythonOperator(
        task_id="write_to_csv",
        python_callable=write_to_csv,
        op_kwargs={
            "df": df,
            "filename": "/Users/nathanmckinney/Desktop/Github/Data_Engineering_Projects/notebooks/my_csv_file.csv",
            
        },
    )

    get_data >> write_to_csv

