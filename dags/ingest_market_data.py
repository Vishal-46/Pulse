import json
import time
import os
import pandas as pd
import s3fs
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from kafka import KafkaConsumer
from dotenv import load_dotenv

# Load env variables (assuming they are set in the Airflow environment)
load_dotenv()

def consume_and_save():
    TOPIC = 'market_data'
    # Use internal service names for docker-compose networking
    BOOTSTRAP_SERVERS = 'redpanda:9092' 
    MINIO_URL = 'http://minio:9000'
    BUCKET = 'bronze'
    
    # Credentials from environment variables
    AWS_ACCESS_KEY = os.getenv('MINIO_ROOT_USER', 'admin')
    AWS_SECRET_KEY = os.getenv('MINIO_ROOT_PASSWORD', 'password')

    # Connect to MinIO
    fs = s3fs.S3FileSystem(
        client_kwargs={'endpoint_url': MINIO_URL}, 
        key=AWS_ACCESS_KEY, 
        secret=AWS_SECRET_KEY, 
        use_ssl=False
    )

    # Initialize Consumer
    # We set consumer_timeout_ms so it stops if no more messages arrive
    consumer = KafkaConsumer(
        TOPIC, 
        bootstrap_servers=[BOOTSTRAP_SERVERS], 
        auto_offset_reset='earliest',
        consumer_timeout_ms=10000,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    batch = []
    for message in consumer:
        batch.append(message.value)
        if len(batch) >= 10:
            break
    
    consumer.close()

    if batch:
        df = pd.DataFrame(batch)
        filename = f"{BUCKET}/data_{int(time.time())}.parquet"
        with fs.open(filename, 'wb') as f:
            df.to_parquet(f)
        print(f"Successfully saved {len(batch)} messages to {filename}")
    else:
        print("No messages found in topic.")

with DAG(
    dag_id='ingest_market_data_batch',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@hourly',
    catchup=False
) as dag:
    
    task_ingest = PythonOperator(
        task_id='ingest_data_from_redpanda',
        python_callable=consume_and_save
    )
