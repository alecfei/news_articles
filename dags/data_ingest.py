import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import requests
import os
from dotenv import load_dotenv
import json

import pandas as pd
import sqlalchemy

default_args = {
    'owner': 'alecfei',
    'start_date': dt.datetime(2024, 10, 26),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

def extractData():
    # read api key from local environment
    # for safety reason
    load_dotenv()
    apikey = os.getenv("NEWS_API_KEY")
    
    # request data from api
    url = 'https://eventregistry.org/api/v1/article/getArticles?'
    para = {
        'apiKey' : apikey,
        'dataStart' : '2023-01-01', # start date 
        'lang' : 'eng' # only English articles
    }

    response = requests.get(url, params=para)

    if response.status_code == 200:
        try:
            data = response.json()
            print(f"Success: {response.status_code} - {response.text}")
        except ValueError as e:
            print("Error parsing JSON:", e)
        else:
            print(f"Error: {response.status_code} - {response.text}")


def transformData():


def loadData():
    engine = sqlalchemy.create_engine("postgresql://postgres:fzj%400829@localhost:5432/dataengineering")
    df=pd.read_sql("select name, city from data", engine)
    df.to_csv('postgresqldata.csv')
    print("-------Data Saved-------")



with DAG('DataPipeTest',
         default_args=default_args,
         schedule_interval=timedelta(minutes=5),
                # '0 * * * *',
         ) as dag:
    
    getData = PythonOperator(task_id='QueryPostgreSQL',
        python_callable=queryPostgresql)
    
    insertData = PythonOperator(task_id='InsertDataElasticsearch',
        python_callable=insertElasticsearch)
    
getData >> insertData