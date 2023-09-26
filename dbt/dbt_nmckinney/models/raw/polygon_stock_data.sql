    {{
        config(
            materialized='table'
        )
    }}

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
FROM {{ source('dbt_nmckinney', 'polygon_stock_data') }}