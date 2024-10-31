* [Data engineering project](#data-engineering-project)
    * [Architecture](#architecture)
    * [API info](#api-intro)
    * [Data streaming](#setup)
        * [Option 1: Articles Batch Streaming using Apache Airflow](#airflow-streaming)
        * [Option 2: Events Batch Streaming using Apache Nifi ???](#nifi-streaming)
    * [Data visualiastion using Metabase ???](#metabase)
    * [Machine learning ???](#ml)

<!-- small space -->

# Data engineering project
In this project, we have developed a data pipeline using several technologies, including [NewsAPI](https://newsapi.ai/), **Apache Airflow** and **PostgreSQL**. The data, which are articles about recent news, press releases and blogs, are extracted from a free trail version of the NewsAPI. The structures of data include information about the article's general content, author(s), source, label(s) and Facebook shares. The whole data pipeline is orchestrated by Aiflow, which is scheduled to batch stream (100 records for every batch) the past 10 days' data ***every one hour***. Potential duplicates are managed throughout whole process.

<!-- small space -->

## Architecture