import schedule
import time
from script import run_stock_job
from datetime import datetime 

def basic_job():
    print("Basic job executed at", datetime.now())
    
schedule.every().minutes.do(basic_job)

schedule.every().day.at("09:30").do(run_stock_job)

schedule.every().minutes.do(run_stock_job)

while True:
    schedule.run_pending()
    time.sleep(1)