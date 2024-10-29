import datetime as dt
from datetime import timedelta
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import requests
import os
from dotenv import load_dotenv
import json

import pandas as pd
import sqlalchemy

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'alecfei',
    'start_date': dt.datetime(2024, 10, 28),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

def extractData(**kwargs):
    load_dotenv()
    apikey = os.getenv("NEWS_API_KEY")
    
    url = 'https://eventregistry.org/api/v1/article/getArticles?'
    para = {
        'apiKey' : apikey,
        'dataType' : ["news", "pr", "blog"],
        'dataStart' : "2023-01-01",
        'articlesSortByAsc' : True,
        'includeArticleSocialScore' : True,
        'includeArticleCategories' : True,
        'lang' : "eng",
    }

    response = requests.get(url, params=para)

    if response.status_code == 200:
        try:
            data = response.json()
            logger.info(f"Success: {response.status_code} - {response.text}", len(data))
            return data
        except ValueError as e:
            logger.error("Error parsing JSON: %s", e)
            raise
    else:
        logger.error("Error: %d - %s", response.status_code, response.text)
        raise Exception(f"API request failed with status code {response.status_code}")

# Function to get first, second, and third-level keywords
def extract_levels(label):
    parts = label.split('/')
    first_level = parts[1] if len(parts) > 1 else None
    second_level = parts[2] if len(parts) > 2 else None
    third_level = parts[3] if len(parts) > 3 else None
    return first_level, second_level, third_level

def transformData(**kwargs):
    # Pull data from XCom
    data = kwargs['ti'].xcom_pull(task_ids='extract_data')
    
    if not data:
        logger.warning("No data to transform")
        return

   # Initialise empty lists to store data
    articles = []
    sources = []
    authors = []
    categories = []

    # Loop through each item in the result
    for item in data:
        # Append article data using relevant keys
        articles.append({
            'id': item.get('uri'),
            'is_duplicate': item.get('isDuplicate'),
            'datetime_found': item.get('dateTime'),
            'datetime_published': item.get('dateTimePub'),
            'article_type': item.get('dataType'),
            'sim': item.get('sim'), # cosine similarity of the article to the centroid of the story
            'url': item.get('url'),
            'title': item.get('title'),
            'body': item.get('body'),
            'image': item.get('image'),
            'sentiment': item.get('sentiment'),
            #'wgt': item.get('wgt'), # parameter used internally for sorting purposes (DO NOT USE THE VALUE)
            'relevance': item.get('relevance') # represents how well does the article match the query
                                            # the higher the value, the better the match
        })

        # Append source information
        source_list = item.get('source')
        sources.append({
                #'source_id': str(uuid.uuid4()),
                'article_id': item.get('uri'),
                'source_name': source_list.get('title'),
                'source_link': source_list.get('uri'),
        })

        # Append author information
        author_list = item.get('authors', [])
        if not author_list:
            authors.append({
                #'author_id': str(uuid.uuid4()),
                'article_id': item.get('uri'),
                'author_name': None,
                'author_email': None,
                'author_type': None,
                'is_agency': None
            })
        else:
            # Loop through each author if multiple authors are present
            for author in author_list:
                if isinstance(author, dict):
                    authors.append({
                        #'author_id': str(uuid.uuid4()),
                        'article_id': item.get('uri'),
                        'author_name': author.get('name', None),
                        'author_email': author.get('uri', None),
                        'author_type': author.get('type', None),
                        'is_agency': author.get('isAgency', None)
                    })
                else:
                    # Handle cases where author data isn't a dict
                    authors.append({
                        #'author_id': str(uuid.uuid4()),
                        'article_id': item.get('uri'),
                        'author_name': None,
                        'author_email': None,
                        'author_type': None,
                        'is_agency': None
                    })
        # Append category information
        category_list = item.get('categories', [])
        for category in category_list:  # Iterate directly over the list
            first_level, second_level, third_level = extract_levels(category.get('label', None))
            categories.append({
                    'article_id': item.get('uri'),
                    #'uri': category.get('uri', None),
                    'label': category.get('label', None),
                    'keyword_1': first_level,
                    'keyword_2': second_level,
                    'keyword_3': third_level
                })
            
        articles_df = pd.DataFrame(articles)
        sources_df = pd.DataFrame(sources)
        authors_df = pd.DataFrame(authors)
        categories_df = pd.DataFrame(categories)

        loadData(articles_df, sources_df, authors_df, categories_df)

def loadData(articles_df, sources_df, authors_df, categories_df):
    try:
        engine = sqlalchemy.create_engine(os.getenv("DATABASE_URL"))
        articles_df.to_sql('articles', con=engine, if_exists='append', index=False)
        sources_df.to_sql('sources', con=engine, if_exists='append', index=False)
        authors_df.to_sql('authors', con=engine, if_exists='append', index=False)

        logger.info("-------Data loaded-------")
    
    except Exception as e:
        logger.error("Error loading data: %s", e)
        raise

with DAG('ArticleDataIngest',
         default_args=default_args,
         schedule_interval=timedelta(minutes=5),
         ) as dag:
    
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extractData,
        provide_context=True
    )
    
    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transformData,
        provide_context=True
    )
    
    load_data = PythonOperator(
        task_id='load_data',
        python_callable=loadData,
        provide_context=True
    )
    
extract_data >> transform_data >> load_data