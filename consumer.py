import json
import pandas as pd
from kafka import KafkaConsumer
import s3fs
import time
import os
from dotenv import load_dotenv

# Load variables from .env
load_dotenv()

# 1. Configuration
# Redpanda topic to listen to
TOPIC = 'market_data'
# MinIO settings
MINIO_URL = 'http://localhost:9000'
BUCKET = 'bronze'
# MinIO credentials loaded from environment
AWS_ACCESS_KEY = os.getenv('MINIO_ROOT_USER')
AWS_SECRET_KEY = os.getenv('MINIO_ROOT_PASSWORD')

# 2. Setup S3 File System
fs = s3fs.S3FileSystem(
    client_kwargs={'endpoint_url': MINIO_URL},
    key=AWS_ACCESS_KEY,
    secret=AWS_SECRET_KEY
)

# 3. Setup Consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[os.getenv('BOOTSTRAP_SERVERS')],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# 4. Batch Processing Loop
batch = []
BATCH_SIZE = 10  # Save to MinIO every 10 messages

print("Consumer started. Waiting for messages...")

for message in consumer:
    data = message.value
    batch.append(data)
    print(f"Received message {len(batch)}/{BATCH_SIZE}")

    if len(batch) >= BATCH_SIZE:
        # Convert batch to DataFrame
        df = pd.DataFrame(batch)
        
        # Define a unique filename using a timestamp
        filename = f"{BUCKET}/market_data_{int(time.time())}.parquet"
        
        # Save as Parquet to MinIO
        with fs.open(filename, 'wb') as f:
            df.to_parquet(f)
            
        print(f"Successfully saved {BATCH_SIZE} records to {filename}")
        
        # Clear the batch
        batch = []
