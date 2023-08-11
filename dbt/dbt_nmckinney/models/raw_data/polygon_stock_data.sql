SELECT 
    open,
    high,
    low,
    close,
    volume,
    vwap,
    timestamp,
    transactions,
    otc,
    ticker,
    date
FROM {{ source('dbt_dev', 'polygon_stock_data') }}