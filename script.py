from datetime import datetime
import requests
import os
from dotenv import load_dotenv
import time
import csv
import snowflake.connector
load_dotenv()

POLYGON_API_KEY = os.getenv("polygon_API_key")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

DS = '2025-10-28'

def run_stock_job():
    DS = datetime.now().strftime('%Y-%m-%d')
    print("Running stock job...")
    url = f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit=1000&sort=ticker&apiKey={POLYGON_API_KEY}'
    response = requests.get(url)
    tickers = [] 

    data= response.json()
    for ticker in data['results']:
        ticker['ds'] = DS
        tickers.append(ticker) 

        
    while 'next_url' in data:
        response = requests.get(data['next_url']+ f'&apikey={POLYGON_API_KEY}')
        data =response.json()
        for ticker in data['results']:
            ticker['ds'] = DS
            tickers.append(ticker)
            
        time.sleep(13)
        
    example_ticker = {'ticker': 'ZVOL',
    'name': 'Volatility Premium Plus ETF',
    'market': 'stocks',
    'locale': 'us',
    'primary_exchange': 'BATS',
    'type': 'ETF',
    'active': True,
    'currency_name': 'usd',
    'composite_figi': 'BBG01G6K5K57',
    'share_class_figi': 'BBG01G6K5L19',
    'last_updated_utc': '2025-10-28T06:06:30.038104559Z',
    'ds': '2025-10-28'}

    fieldnames = list(example_ticker.keys())
    
    # prepare rows for insert
    rows = []
    for t in tickers:
        row = [t.get(k, None) for k in fieldnames]
        rows.append(tuple(row))

    # connect to Snowflake
    conn_kwargs = {
        "user": SNOWFLAKE_USER,
        "password": SNOWFLAKE_PASSWORD,
        "account": SNOWFLAKE_ACCOUNT,
    }
    if SNOWFLAKE_WAREHOUSE:
        conn_kwargs["warehouse"] = SNOWFLAKE_WAREHOUSE
    if SNOWFLAKE_DATABASE:
        conn_kwargs["database"] = SNOWFLAKE_DATABASE
    if SNOWFLAKE_SCHEMA:
        conn_kwargs["schema"] = SNOWFLAKE_SCHEMA

    ctx = snowflake.connector.connect(**conn_kwargs)
    cs = ctx.cursor()
    try:
        table_name = "TICKERS"
        # create table (simple types; last_updated_utc kept as VARCHAR)
        create_cols = [
            "ticker VARCHAR",
            "name VARCHAR",
            "market VARCHAR",
            "locale VARCHAR",
            "primary_exchange VARCHAR",
            "\"type\" VARCHAR",
            "active BOOLEAN",
            "currency_name VARCHAR",
            "composite_figi VARCHAR",
            "share_class_figi VARCHAR",
            "last_updated_utc TIMESTAMP",
            "ds DATE"
        ]
        create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(create_cols)});"
        cs.execute(create_sql)

        # insert rows using parameterized statement
        placeholders = ", ".join(["%s"] * len(fieldnames))
        sql_fieldnames = ['"{}"'.format(f) if f.lower() == 'type' else f for f in fieldnames]
        insert_sql = f"INSERT INTO {table_name} ({', '.join(sql_fieldnames)}) VALUES ({placeholders})"
        # executemany in chunks to avoid huge single batch
        chunk_size = 1000
        inserted = 0
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i+chunk_size]
            cs.executemany(insert_sql, chunk)
            inserted += len(chunk)

        ctx.commit()
        print(f"Inserted {inserted} rows into Snowflake table {table_name}")
    finally:
        cs.close()
        ctx.close()


if __name__ == "__main__":
    run_stock_job()