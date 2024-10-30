import datetime as dt
import logging
import os

import requests
import pandas as pd
import sqlalchemy
from sqlalchemy import text
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

default_args = {
    'owner': 'alecfei',
    'start_date': dt.datetime(2024, 10, 30),
    #'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

def extractData():
    apikey = os.getenv("NEWS_API_KEY")
    
    url = 'https://eventregistry.org/api/v1/article/getArticles?'
    params = {
            'apiKey': apikey,
            'dataType': ["news", "pr", "blog"],
            'articlesSortByAsc': True,
            'includeArticleSocialScore': True,
            'includeArticleCategories': True,
            'lang': "eng",
            'forceMaxDataTimeWindow': 7,
    }

    response = requests.get(url, params=params)

    if response.status_code == 200:
        try:
            data = response.json()
            logger.info(f"Success: {response.status_code} - Retrieved {len(data.get('articles', []))} articles")
            result = data['articles']['results']
            return result
        except ValueError as e:
            logger.error("Error parsing JSON: %s", e)
            raise
    else:
        logger.error("API request failed with status code %d: %s", response.status_code, response.text)
        raise Exception(f"API request failed with status code {response.status_code}")

# Function to get first, second, and third-level keywords
def extract_levels(label):
    parts = label.split('/')
    first_level = parts[1] if len(parts) > 1 else None
    second_level = parts[2] if len(parts) > 2 else None
    third_level = parts[3] if len(parts) > 3 else None
    return first_level, second_level, third_level

def transformData(ti):
    data = ti.xcom_pull(task_ids='extract_data')

    if not data:
        logger.warning("No data to transform")
        return

    articles, sources, authors, categories, facebook_shares = [], [], [], [], []

    for item in data:
        if isinstance(item, dict):  # Ensure item is a dictionary
            articles.append({
                'id': item.get('uri'),
                'is_duplicate': item.get('isDuplicate'),
                'datetime_found': item.get('dateTime'),
                'datetime_published': item.get('dateTimePub'),
                'article_type': item.get('dataType'),
                'sim': item.get('sim'),
                'url': item.get('url'),
                'title': item.get('title'),
                'body': item.get('body'),
                'image': item.get('image'),
                'sentiment': item.get('sentiment'),
                'relevance': item.get('relevance')
            })

            source_list = item.get('source')
            sources.append({
                'article_id': item.get('uri'),
                'source_name': source_list.get('title'),
                'source_link': source_list.get('uri'),
            })

            author_list = item.get('authors', [])
            if not author_list:
                authors.append({
                    'article_id': item.get('uri'),
                    'author_name': None,
                    'author_email': None,
                    'author_type': None,
                    'is_agency': None
                })
            else:
                for author in author_list:
                    if isinstance(author, dict):
                        authors.append({
                            'article_id': item.get('uri'),
                            'author_name': author.get('name', None),
                            'author_email': author.get('uri', None),
                            'author_type': author.get('type', None),
                            'is_agency': author.get('isAgency', None)
                        })
                    else:
                        authors.append({
                            'article_id': item.get('uri'),
                            'author_name': None,
                            'author_email': None,
                            'author_type': None,
                            'is_agency': None
                        })
            category_list = item.get('categories', [])
            for category in category_list:
                first_level, second_level, third_level = extract_levels(category.get('label', None))
                categories.append({
                        'article_id': item.get('uri'),
                        'label': category.get('label', None),
                        'keyword_1': first_level,
                        'keyword_2': second_level,
                        'keyword_3': third_level
                    })

            share_list = item.get('shares')
            facebook_shares.append({
                    'article_id': item.get('uri'),
                    'facebook_share': share_list.get('facebook'),
            })
    
    ti.xcom_push(key='articles_df', value=pd.DataFrame(articles))
    ti.xcom_push(key='sources_df', value=pd.DataFrame(sources))
    ti.xcom_push(key='authors_df', value=pd.DataFrame(authors))
    ti.xcom_push(key='categories_df', value=pd.DataFrame(categories))
    ti.xcom_push(key='shares_df', value=pd.DataFrame(facebook_shares))

def loadData(ti):
    articles_df = ti.xcom_pull(key='articles_df', task_ids='transform_data')
    sources_df = ti.xcom_pull(key='sources_df', task_ids='transform_data')
    authors_df = ti.xcom_pull(key='authors_df', task_ids='transform_data')
    categories_df = ti.xcom_pull(key='categories_df', task_ids='transform_data')
    shares_df = ti.xcom_pull(key='shares_df', task_ids='transform_data')

    if articles_df.empty and sources_df.empty and authors_df.empty and categories_df.empty and shares_df.empty:
        logger.warning("No data to load into the database")
        return

    try:
        engine = sqlalchemy.create_engine(os.getenv("DATABASE_URL"))

        articles_df.to_sql('articles', con=engine, if_exists='append', index=False)
        sources_df.to_sql('sources', con=engine, if_exists='append', index=False)
        authors_df.to_sql('authors', con=engine, if_exists='append', index=False)
        categories_df.to_sql('categories', con=engine, if_exists='append', index=False)
        shares_df.to_sql('facebookshares', con=engine, if_exists='append', index=False)
    
        logger.info("Data loaded successfully")
    
    except Exception as e:
        logger.error("Error loading data: %s", e)
        raise

with DAG('ETLTestPipeline',
         default_args=default_args,
         schedule=dt.timedelta(days=1),
         #max_active_runs=1,
         catchup=False) as dag:
    
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extractData,
    )
    
    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transformData,
    )
    
    load_data = PythonOperator(
        task_id='load_data',
        python_callable=loadData,
    )

extract_data >> transform_data >> load_data