from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import snowflake.connector
import yfinance as yf  # <-- Using yfinance
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def get_scalar(val):
    """Extract scalar value if val is a one-element Series; otherwise, return val as is."""
    return val.item() if hasattr(val, "item") else val

def fetch_and_load_data(**context):
    """
    Daily job:
      - For each symbol:
        1) Get latest date already in Snowflake.
        2) Fetch daily data from yfinance (last 180 days or from the day after the latest date).
        3) Insert only rows newer than the latest date in DB.
    """
    # 1) Snowflake connection
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse="COMPUTE_WH",
        database="FINANCE_DB",
        schema="ANALYTICS"
    )
    cursor = conn.cursor()

    symbols = ["TSLA", "SPY"]  # Add more symbols if needed

    for symbol in symbols:
        # 2) Find the latest date in Snowflake for this symbol
        cursor.execute("""
            SELECT MAX(date)
            FROM FINANCE_DB.ANALYTICS.stock_prices
            WHERE stock_symbol = %s
        """, (symbol,))
        result = cursor.fetchone()
        latest_date_in_db = result[0]  # None if no rows yet

        # Determine the start_date for yfinance
        if latest_date_in_db is None:
            # If table empty, fetch last 180 days
            start_date = datetime.today() - timedelta(days=180)
        else:
            # Fetch from day after the latest date in DB
            start_date = latest_date_in_db + timedelta(days=1)
        end_date = datetime.today()

        # 3) Fetch daily data from yfinance.
        #    yfinance returns a DataFrame with columns: Open, High, Low, Close, Adj Close, Volume.
        #    We'll rename them to match your Snowflake schema: open, close, min, max, volume.
        df = yf.download(symbol, start=start_date, end=end_date, progress=False)

        if df.empty:
            print(f"✅ {symbol}: No new data from yfinance (df is empty).")
            continue

        df = df.rename(columns={
            "Open": "open",
            "Close": "close",
            "Low": "min",
            "High": "max",
            "Volume": "volume"
        })

        # 4) Build a list of only the “new” rows
        rows_to_insert = []
        for idx, row in df.iterrows():
            date_obj = idx.date()  # idx is a Timestamp; convert to date
            if (latest_date_in_db is None) or (date_obj > latest_date_in_db):
                open_val = float(get_scalar(row["open"]))
                close_val = float(get_scalar(row["close"]))
                min_val = float(get_scalar(row["min"]))
                max_val = float(get_scalar(row["max"]))
                volume_raw = get_scalar(row["volume"])
                volume_val = int(volume_raw) if not pd.isna(volume_raw) else 0

                rows_to_insert.append((
                    symbol,
                    date_obj.isoformat(),  # Format date as string
                    open_val,
                    close_val,
                    min_val,
                    max_val,
                    volume_val
                ))

        if not rows_to_insert:
            print(f"✅ {symbol}: Already up to date. No new rows to insert.")
            continue

        # Sort rows by date ascending (oldest first)
        rows_to_insert.sort(key=lambda x: x[1])

        # 5) Insert new rows
        insert_sql = """
            INSERT INTO FINANCE_DB.ANALYTICS.stock_prices
                (stock_symbol, date, open, close, min, max, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        try:
            cursor.execute("BEGIN;")
            cursor.executemany(insert_sql, rows_to_insert)
            cursor.execute("COMMIT;")
            print(f"✅ Inserted {len(rows_to_insert)} new rows for {symbol}.")
        except Exception as e:
            cursor.execute("ROLLBACK;")
            print(f"❌ Failed to insert new rows for {symbol}. Error: {e}")

    cursor.close()
    conn.close()

def cleanup_old_data(**context):
    """
    Keep only the last 180 trading days (per symbol) using a window function.
    Anything older than that gets deleted.
    """
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse="COMPUTE_WH",
        database="FINANCE_DB",
        schema="ANALYTICS"
    )
    cursor = conn.cursor()

    cleanup_sql = """
        DELETE FROM FINANCE_DB.ANALYTICS.stock_prices
        WHERE (stock_symbol, date) IN (
            SELECT stock_symbol, date
            FROM (
                SELECT
                    stock_symbol,
                    date,
                    ROW_NUMBER() OVER (
                        PARTITION BY stock_symbol
                        ORDER BY date DESC
                    ) AS rn
                FROM FINANCE_DB.ANALYTICS.stock_prices
            ) sub
            WHERE sub.rn > 180
        );
    """

    try:
        cursor.execute("BEGIN;")
        cursor.execute(cleanup_sql)
        cursor.execute("COMMIT;")
        print("✅ Cleaned up rows so that only the last 180 trading days remain for each symbol.")
    except Exception as e:
        cursor.execute("ROLLBACK;")
        print(f"❌ Failed to clean up old rows. Error: {e}")
    finally:
        cursor.close()
        conn.close()

# Define the DAG
with DAG(
    dag_id='stock_price_daily_load',
    default_args=default_args,
    description='Fetch daily stock data from yfinance and keep exactly 180 trading days in Snowflake.',
    schedule_interval='@daily',
    catchup=False
) as dag:

    load_task = PythonOperator(
        task_id='fetch_and_load_data',
        python_callable=fetch_and_load_data,
        provide_context=True
    )

    cleanup_task = PythonOperator(
        task_id='cleanup_old_data',
        python_callable=cleanup_old_data,
        provide_context=True
    )

    load_task >> cleanup_task
