import datetime as dt
import logging
import os

import requests
import pandas as pd

import sqlalchemy
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables once
load_dotenv()

default_args = {
    'owner': 'alecfei',
    'start_date': dt.datetime(2024, 10, 28),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

def extractData():
    apikey = os.getenv("NEWS_API_KEY")
    
    url = 'https://eventregistry.org/api/v1/article/getArticles?'
    params = {
        'apiKey': apikey,
        'dataType': ["news", "pr", "blog"],  # Ensure API supports this format
        'dataStart': "2023-01-01",
        'articlesSortByAsc': True,
        'includeArticleSocialScore': True,
        'includeArticleCategories': True,
        'lang': "eng",
    }

    response = requests.get(url, params=params)

    if response.status_code == 200:
        try:
            data = response.json()
            logger.info(f"Success: {response.status_code} - Retrieved {len(data.get('articles', []))} articles")
            logger.info(f"Response Data: {data}")  # Log the entire response

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

    articles, sources, authors, categories = [], [], [], []

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
    if articles_df.empty and sources_df.empty and authors_df.empty and categories_df.empty:
        logger.warning("No data to load into the database")
        return

    try:
        engine = sqlalchemy.create_engine(os.getenv("DATABASE_URL"))
        articles_df.to_sql('articles', con=engine, if_exists='append', index=False)
        sources_df.to_sql('sources', con=engine, if_exists='append', index=False)
        authors_df.to_sql('authors', con=engine, if_exists='append', index=False)
        categories_df.to_sql('categories', con=engine, if_exists='append', index=False)
        
        logger.info("Data loaded successfully")
    
    except Exception as e:
        logger.error("Error loading data: %s", e)
        raise

with DAG('ArticleETLPipeline',
         default_args=default_args,
         schedule_interval=dt.timedelta(minutes=5),
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
        op_kwargs={
            'articles_df': pd.DataFrame(),
            'sources_df': pd.DataFrame(),
            'authors_df': pd.DataFrame(),
            'categories_df': pd.DataFrame()
        }
    )

extract_data >> transform_data >> load_data