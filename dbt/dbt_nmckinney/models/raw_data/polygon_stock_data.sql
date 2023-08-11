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
FROM {{ source('stripe', 'polygon_stock_data') }}