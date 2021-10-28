import requests
import pandas as pd
import os
from datetime import datetime as dt
from pathlib import Path
import schedule
import time
from time import sleep as sleep

# Create a directory to hold the csv records
def createDirectory():
    if not os.path.isdir(report_directory): os.mkdir(report_directory)

# Download a report for today from the API
def getTodaysReport():
    today = dt.utcnow().strftime('%Y%m%d')
    file_location = f'{report_directory}/{today}_test_cases.csv'
    url = f'https://www.opendata.nhs.scot/dataset/b318bddf-a4dc-4262-971f-0ba329e09b87/resource/8da654cd-293b-4286-96a4-b3ece86225f0/download/test_hb_{today}.csv'  
    response = requests.get(url)
    if response.status_code == 200:
        with open(file_location, 'w') as f:
            f.write(response.text)

    # In the event of an unsuccessful request wait x mins and try again
    else:
        wait = 10
        sleep(wait * 60)

# Concatentate all the reports together in one csv
def concatenateAllReports():
    reports = [str(p) for p in Path(report_directory).rglob('*csv')]
    dfs = [pd.read_csv(open(path, 'r', encoding='utf-8'), sep=',') for path in reports]
    df_concat = pd.concat(dfs)
    df_concat.to_csv(f'{cwd}/master_test_report.csv')

def run(t):
    createDirectory()
    getTodaysReport()
    concatenateAllReports()
    print(t)

# Set directories
cwd = os.getcwd()
report_directory = f'{cwd}/test_reports'

# Set update time
update_time = '11:28'

# Run sheduled tasks
schedule.every().day.at(update_time).do(run, f'>>> Running {update_time} Update')
while True:
    schedule.run_pending()
    time.sleep(60) # wait one minute

    
