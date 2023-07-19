### Packages
import pandas as pd
import numpy as np
import requests
from polygon import RESTClient
from datetime import datetime
from airflow.utils.dates import days_ago
import pytz
import airflow as af
import pendulum
from airflow.operators.python import PythonOperator


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
    df["date"] = df["timestamp"].dt.date

    return df




dag = af.DAG(dag_id="initial_load", schedule=None, start_date=datetime.today())

get_data = PythonOperator(
    task_id="get_data",
    python_callable=get_aggregate_data,
    op_kwargs={
        "tickers": ["NVDA"],
        "from_date": "2018-01-01",
        "to_date": datetime.now().strftime("%Y-%m-%d"),
    },
)