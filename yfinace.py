import yfinance as yf
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

def return_snowflake_conn():
    """Return a Snowflake cursor object."""
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def extract(ticker_symbols):
    """Extract stock data from yfinance"""
    data = {}
    for symbol in ticker_symbols:
        ticker = yf.Ticker(symbol)
        stock_data = ticker.history(period='180d')
        data[symbol] = stock_data
    return data

@task
def transform(stock_data):
    """Transform extracted stock data"""
    records = []
    for symbol, data in stock_data.items():
        for date, row in data.iterrows():
            records.append((symbol, date.date(), row["Open"], row["High"], row["Low"], row["Close"], row["Volume"]))
    return records

@task
def load(records, target_table):
    """Load transformed stock data into Snowflake"""
    con = return_snowflake_conn()
    try:
        con.execute("BEGIN;")
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                symbol VARCHAR,
                date DATE,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume FLOAT,
                PRIMARY KEY (symbol, date)
            );
        """)
        con.execute(f"DELETE FROM {target_table}")
        
        for r in records:
            sql = f"""
                INSERT INTO {target_table} 
                (symbol, date, open, high, low, close, volume) 
                VALUES ('{r[0]}', '{r[1]}', '{r[2]}', '{r[3]}', '{r[4]}', '{r[5]}', '{r[6]}')
            """
            con.execute(sql)
        
        con.execute("COMMIT;")
    except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise e


with DAG(
    dag_id='Dag_Test1',
    start_date=datetime(2025, 10, 4),
    catchup=False,
    tags=['ETL'],
    schedule='0 7 * * *',
    default_args={'retries': 3, 'retry_delay': timedelta(minutes=5)}
) as dag_1:

  # Handle missing Airflow Variable
    ticker_symbols = Variable.get("stock_symbol").split(",")
    target_table = "USER_DB_MACKEREL.RAW.STOCK_PRICE"

    extracted_data = extract(ticker_symbols)
    transformed_records = transform(extracted_data)
    loaded_data = load(transformed_records, target_table)

    trigger_forecast = TriggerDagRunOperator(
        task_id='trigger_forecast_dag',
        trigger_dag_id='Dag_Test2',
        conf={},
        dag=dag_1
    )

    extracted_data >> transformed_records >> loaded_data >> trigger_forecast