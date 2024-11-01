* [Data engineering project](#data-engineering-project)
    * [Architecture](#architecture)
    * [Data pipeline](#setup)
        * [API info](#api-intro)
        * [Articles Batch Processing using Apache Airflow](#airflow-streaming)
    * [Data analysis using SQL](#sql)
    * [Data visualiastion using Metabase ???](#metabase)

<!-- small space -->

# Data engineering project
In this project, we have developed a data pipeline using several technologies, including [NewsAPI](https://newsapi.ai/), **Apache Airflow** and **PostgreSQL**. The data, which are articles about recent news, press releases and blogs, are extracted from a free trail version of the NewsAPI. The structures of data include information about the article's general content, author(s), source, label(s) and Facebook shares. The whole data pipeline is orchestrated by Aiflow, which is scheduled to batch process (100 records for every batch) the past 10 days' data ***every one hour***. Potential duplicates are managed throughout whole process.

<!-- small space -->

## Architecture