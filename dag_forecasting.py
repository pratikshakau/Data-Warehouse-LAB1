from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

def return_snowflake_conn():
    """Return a Snowflake cursor object."""
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def train_model(cur, stock_table, train_view, forecast_function):
    """Train the forecasting model on historical stock prices"""
    
    create_view_sql = f"""CREATE OR REPLACE VIEW {train_view} AS 
        SELECT date, close, symbol 
        FROM {stock_table};"""

    create_model_sql = f"""CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function} (
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
        SERIES_COLNAME => 'SYMBOL',
        TIMESTAMP_COLNAME => 'DATE',
        TARGET_COLNAME => 'CLOSE',
        CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
    );"""

    try:
        cur.execute(create_view_sql)
        cur.execute(create_model_sql)
        cur.execute(f"CALL {forecast_function}!SHOW_EVALUATION_METRICS();")
    except Exception as e:
        print("Error in training model:", e)
        raise

@task
def generate_forecast(cur, forecast_function, stock_table, forecast_table, final_table):
    """Predict stock prices and merge with historical data"""
    
    forecast_sql = f"""BEGIN
        CALL {forecast_function}!FORECAST(
            FORECASTING_PERIODS => 7,
            CONFIG_OBJECT => {{'prediction_interval': 0.95}}
        );
        LET result := SQLID;
        CREATE OR REPLACE TABLE {forecast_table} AS SELECT * FROM TABLE(RESULT_SCAN(:result));
    END;"""

    merge_forecast_sql = f"""CREATE OR REPLACE TABLE {final_table} AS
        SELECT symbol, date, close AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
        FROM {stock_table}
        UNION ALL
        SELECT replace(series, '"', '') as symbol, ts as date, NULL AS actual, forecast, lower_bound, upper_bound
        FROM {forecast_table};"""

    try:
        cur.execute(forecast_sql)
        cur.execute(merge_forecast_sql)
    except Exception as e:
        print("Error in forecasting:", e)
        raise

with DAG(
    dag_id='Dag_Test2',
    start_date=datetime(2025, 10, 4),
    catchup=False,
    schedule_interval='0 8 * * *',
    tags=['ML', 'Forecasting'],
    default_args={'retries': 3, 'retry_delay': timedelta(minutes=5)}
) as dag_2:

    stock_table = "USER_DB_MACKEREL.RAW.stock_price"   # Using the same schema from ETL
    train_view = "USER_DB_MACKEREL.RAW.stock_price_view"
    forecast_table = "USER_DB_MACKEREL.RAW.stock_forecast"
    forecast_function = "USER_DB_MACKEREL.RAW.predict_stock_price"
    final_table = "USER_DB_MACKEREL.RAW.stock_final"
    
    cur = return_snowflake_conn()
    
    trained_model = train_model(cur, stock_table, train_view, forecast_function)
    forecasted_data = generate_forecast(cur, forecast_function, stock_table, forecast_table, final_table)

    trained_model >> forecasted_data


