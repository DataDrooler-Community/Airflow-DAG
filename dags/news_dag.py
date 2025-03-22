from datetime import datetime, timedelta
from airflow import DAG
import requests
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

NEWS_API_KEY = "INSERT API KEY"
NEWS_SOURCES = "bbc-news,cnn,the-verge"
NEWS_API_URL = f"https://newsapi.org/v2/everything?q=artificial+intelligence&sources={NEWS_SOURCES}&sortBy=publishedAt&apiKey={NEWS_API_KEY}"

def fetch_ai_news(ti):
    response = requests.get(NEWS_API_URL)
    news_data = response.json()
    articles = news_data.get("articles", [])
    
    if not articles:
        logging.info("No news data found")
    else:
        # Create DataFrame from all sources combined
        df = pd.DataFrame(articles, columns=["title", "description", "url", "publishedAt", "source"])
        
        # Ensure source field contains only the name of the source (extracted from the source dictionary)
        df["source"] = df["source"].apply(lambda x: x["name"] if isinstance(x, dict) else x)
    
    ti.xcom_push(key="news_data", value=df.to_dict("records"))


def clean_data(ti):
    news_data = ti.xcom_pull(key="news_data", task_ids="fetch_news_data")
    if not news_data:
        raise ValueError("No news data found")
    
    df = pd.DataFrame(news_data)
    df = df.dropna(subset=["title", "description"])  # Remove rows with missing values
    df["publishedAt"] = pd.to_datetime(df["publishedAt"], errors="coerce").dt.strftime('%Y-%m-%d %H:%M:%S')  # Convert to string format
    df["title"] = df["title"].str.strip().str.replace(r"\s+", " ", regex=True).str.lower()
    df["description"] = df["description"].str.strip().str.replace(r"\s+", " ", regex=True).str.lower()
    
    ti.xcom_push(key="cleaned_news_data", value=df.to_dict("records"))

def insert_news_data_into_postgres(ti):
    news_data = ti.xcom_pull(key="cleaned_news_data", task_ids="clean_news_data")
    if not news_data:
        raise ValueError("No cleaned news data found")
    
    postgres_hook = PostgresHook(postgres_conn_id="my_db_connection")
    insert_query = """
    INSERT INTO ai_news_data (title, description, url, published_at, source, inserted_at)
    VALUES (%s, %s, %s, %s, %s, NOW())  -- Use NOW() for the inserted_at timestamp
    """
    for article in news_data:
        postgres_hook.run(insert_query, parameters=(article["title"], article["description"], article["url"], article["publishedAt"], article["source"]))

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 15),
}

dag = DAG(
    "fetch_and_store_ai_news",
    default_args=default_args,
    description="A DAG to fetch AI news and store it in Postgres",
    schedule_interval=timedelta(days=1),
    catchup=False,
)

fetch_news_data_task = PythonOperator(
    task_id="fetch_news_data",
    python_callable=fetch_ai_news,
    dag=dag,
)

clean_news_data_task = PythonOperator(
    task_id="clean_news_data",
    python_callable=clean_data,
    dag=dag,
)

create_table_task = SQLExecuteQueryOperator(
    task_id="create_table",
    conn_id="my_db_connection",
    sql="""
    CREATE TABLE IF NOT EXISTS ai_news_data (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        description TEXT,
        url TEXT,
        published_at TIMESTAMP,
        source TEXT,
        inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    dag=dag,
)

insert_news_data_task = PythonOperator(
    task_id="insert_news_data",
    python_callable=insert_news_data_into_postgres,
    dag=dag,
)

fetch_news_data_task >> clean_news_data_task >> create_table_task >> insert_news_data_task
