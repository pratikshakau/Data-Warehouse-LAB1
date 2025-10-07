# Data-Warehouse-LAB1
End-to-end stock price pipeline using Airflow + Snowflake: extract Alpha Vantage daily prices, transform with pandas, and full-refresh load into RAW.STOCK_DATA, scheduled @daily. Includes idempotency + optional Snowflake ML forecast.
