import airflow
from airflow.operators.python import PythonOperator
from datetime import datetime

def get_most_recent_day(ticker):

    rest_client = RESTClient(api_key="bt70lDEhuX6JBTu35vVU8Dbekd7pLWGt")
    get_aggregates = rest_client.get_aggs(
        ticker=ticker,
        multiplier=1,
        timespan="day",
        from_=datetime.now().strftime("%Y-%m-%d"),
        to=datetime.now().strftime("%Y-%m-%d"), 
        adjusted=True,
        sort="asc",
        limit=1,
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

dag = airflow.DAG(
    dag_id="pull_most_recent_day_data",
    description="Pulls the most recent day of data for all tickers",
    schedule_interval="@daily",
)

task_get_most_recent_day = PythonOperator(
    task_id="get_most_recent_day_data",
    python_callable=get_most_recent_day,
    op_args=["AAPL"],
)

dag.add_task(task_get_most_recent_day)


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