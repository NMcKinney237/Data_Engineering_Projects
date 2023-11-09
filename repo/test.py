import pandas as pd
import numpy as np
import requests
from polygon import RESTClient
from datetime import datetime
import pytz
import time
import google.cloud.bigquery as bq

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


def main():
    """
    Main function.
    """

    # Get the top 10 tickers with strong AI ties
    top_10_tickers = ["NVDA", "AAPL", "GOOG", "AMZN", "MSFT", "INTC", "IBM", "FB", "TSM", "TSLA"]

    # Define the from_date variable
    from_date = "2018-01-01"

    # Define the to_date variable
    to_date = datetime.now().strftime("%Y-%m-%d")

    df = pd.DataFrame()
    for ticker in top_10_tickers:
        new_df = get_aggregate_data(ticker, from_date, to_date)
        df = pd.concat([df, new_df], ignore_index=True)

         # Wait 1 second before making the next request
        time.sleep(60)


    # Load the Pandas DataFrame to a GCP warehouse
    client = bq.Client()
    dataset_id = "cloud-warehouse-389415.jaffle_shop"
    table_id = "polygon_techstocks_data"
    df.to_gbq(client, dataset_id, table_id)

if __name__ == "__main__":
    main()