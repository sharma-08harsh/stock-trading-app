import requests
import os
from dotenv import load_dotenv
import time
import csv
load_dotenv()

POLYGON_API_KEY = os.getenv("polygon_API_key")


url = f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit=1000&sort=ticker&apiKey={POLYGON_API_KEY}'
response = requests.get(url)
tickers = []

data= response.json()
for ticker in data['results']:
    tickers.append(ticker) 

    
while 'next_url' in data:
    response = requests.get(data['next_url']+ f'&apikey={POLYGON_API_KEY}')
    data =response.json()
    for ticker in data['results']:
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
 'last_updated_utc': '2025-10-28T06:06:30.038104559Z'}

fieldnames = list(example_ticker.keys())
output_file = "tickers.csv"

with open(output_file, mode="w", newline='', encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    for t in tickers:
        # ensure keys exist and keep schema consistent
        row = {k: t.get(k, "") for k in fieldnames}
        writer.writerow(row)

print(f"Saved {len(tickers)} tickers to {output_file}")
