import datetime as dt
from datetime import datetime, timedelta
import logging

from dotenv import load_dotenv
import os

import requests

import pandas as pd
import sqlalchemy
from sqlalchemy import text

from airflow import DAG
from airflow.operators.python import PythonOperator

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

default_args = {
    'owner': 'alecfei',
    'start_date': datetime(2024, 10, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create tables in database in PostgreSQL
def createTables():
    try:
        engine = sqlalchemy.create_engine(os.getenv("DATABASE_URL"))
        with engine.begin() as connection:
            # Create Articles Table
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS articles (
                    id TEXT PRIMARY KEY,
                    is_duplicate BOOLEAN,
                    datetime_found TIMESTAMP,
                    datetime_published TIMESTAMP,
                    article_type TEXT,
                    sim FLOAT,
                    url TEXT,
                    title TEXT,
                    body TEXT,
                    image TEXT,
                    sentiment TEXT,
                    relevance FLOAT
                );
            """))

            # Create Sources Table
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS sources (
                    article_id TEXT,
                    source_name TEXT,
                    source_link TEXT,
                    UNIQUE (article_id, source_name)
                );
            """))

            # Create Authors Table
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS authors (
                    article_id TEXT,
                    author_name TEXT,
                    author_email TEXT,
                    author_type TEXT,
                    is_agency BOOLEAN,
                    UNIQUE (article_id, author_name, author_email)
                );
            """))

            # Create Categories Table
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS categories (
                    article_id TEXT,
                    label TEXT,
                    keyword_1 TEXT,
                    keyword_2 TEXT,
                    keyword_3 TEXT,
                    UNIQUE (article_id)
                );
            """))

            # Create Facebook Shares Table
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS facebookshares (
                    article_id TEXT,
                    shares FLOAT,
                    UNIQUE (article_id)
                );
            """))

        logger.info("Tables created or already exist.")

    except Exception as e:
        logger.error("Error creating tables: %s", e)
        raise

# Extract articles from NewsAPI
def extractData():
    apikey = os.getenv("NEWS_API_KEY")
    
    url = 'https://eventregistry.org/api/v1/article/getArticles?'
    params = {
            'apiKey': apikey,
            'dataType': ["news", "pr", "blog"],
            'dateStart' : (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d'),
            'dateEnd' : (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
            'articlesSortByAsc': True,
            'includeArticleSocialScore': True,
            'includeArticleCategories': True,
            'lang': "eng",
            #'forceMaxDataTimeWindow': 31,
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

# Define function to get first, second, and third-level keywords in category lists
def extract_levels(label):
    parts = label.split('/')
    first_level = parts[1] if len(parts) > 1 else None
    second_level = parts[2] if len(parts) > 2 else None
    third_level = parts[3] if len(parts) > 3 else None
    return first_level, second_level, third_level

# Transform data into the form for loading into database, i.e. data modeling
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
                    'shares': share_list.get('facebook'),
            })
    
    # Create pandas DataFrames
    articles_df = pd.DataFrame(articles)
    sources_df = pd.DataFrame(sources)
    authors_df = pd.DataFrame(authors)
    categories_df = pd.DataFrame(categories)
    shares_df = pd.DataFrame(facebook_shares)

    # Convert datetime columns to datetime type
    if not articles_df.empty:
        articles_df['datetime_found'] = pd.to_datetime(articles_df['datetime_found'], errors='coerce')
        articles_df['datetime_published'] = pd.to_datetime(articles_df['datetime_published'], errors='coerce')

    # Push DataFrames to XCom for later use
    ti.xcom_push(key='articles_df', value=articles_df)
    ti.xcom_push(key='sources_df', value=sources_df)
    ti.xcom_push(key='authors_df', value=authors_df)
    ti.xcom_push(key='categories_df', value=categories_df)
    ti.xcom_push(key='shares_df', value=shares_df)

def loadData(ti):
    # Pull transformed DataFrames
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

        # Load each DataFrame into its respective table
        with engine.begin() as connection:
            # Articles
            for _, row in articles_df.iterrows():
                connection.execute(
                    text("""
                        INSERT INTO articles (id, is_duplicate, datetime_found, datetime_published, article_type, sim, url, title, body, image, sentiment, relevance)
                        VALUES (:id, :is_duplicate, :datetime_found, :datetime_published, :article_type, :sim, :url, :title, :body, :image, :sentiment, :relevance)
                        ON CONFLICT (id) DO NOTHING;
                    """), **row
                )

            # Sources
            for _, row in sources_df.iterrows():
                connection.execute(
                    text("""
                        INSERT INTO sources (article_id, source_name, source_link)
                        VALUES (:article_id, :source_name, :source_link)
                        ON CONFLICT (article_id, source_name) DO NOTHING;
                    """), **row
                )

            # Authors
            for _, row in authors_df.iterrows():
                connection.execute(
                    text("""
                        INSERT INTO authors (article_id, author_name, author_email, author_type, is_agency)
                        VALUES (:article_id, :author_name, :author_email, :author_type, :is_agency)
                        ON CONFLICT (article_id, author_name, author_email) DO NOTHING;
                    """), **row
                )

            # Categories
            for _, row in categories_df.iterrows():
                connection.execute(
                    text("""
                        INSERT INTO categories (article_id, label, keyword_1, keyword_2, keyword_3)
                        VALUES (:article_id, :label, :keyword_1, :keyword_2, :keyword_3)
                        ON CONFLICT (article_id) DO NOTHING;
                    """), **row
                )

            # Facebook Shares
            for _, row in shares_df.iterrows():
                connection.execute(
                    text("""
                        INSERT INTO facebookshares (article_id, shares)
                        VALUES (:article_id, :shares)
                        ON CONFLICT (article_id) DO NOTHING;
                    """), **row
                )

        logger.info("Data loaded successfully without duplicates")

    except Exception as e:
        logger.error("Error loading data: %s", e)
        raise


with DAG('ETLPipelineForArticles',
         default_args=default_args,
         schedule=timedelta(minutes=60),
         catchup=False
         ) as dag:
    
    create_tables = PythonOperator(
        task_id='create_tables',
        python_callable=createTables,
    )

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

create_tables >> extract_data >> transform_data >> load_data