import json
import pandas as pd
from kafka import KafkaConsumer
import s3fs
import time
import os
from dotenv import load_dotenv

load_dotenv()

TOPIC = 'market_data'
MINIO_URL = 'http://localhost:9000'
BUCKET = 'bronze'
AWS_ACCESS_KEY = os.getenv('MINIO_ROOT_USER')
AWS_SECRET_KEY = os.getenv('MINIO_ROOT_PASSWORD')

fs = s3fs.S3FileSystem(client_kwargs={'endpoint_url': MINIO_URL}, key=AWS_ACCESS_KEY, secret=AWS_SECRET_KEY, use_ssl=False)

consumer = KafkaConsumer(TOPIC, bootstrap_servers=[os.getenv('BOOTSTRAP_SERVERS')], auto_offset_reset='earliest')

batch = []
BATCH_SIZE = 5

print("Consumer started...")
for message in consumer:
    try:
        data = json.loads(message.value.decode('utf-8'))
        batch.append(data)
        print(f"Added: {len(batch)}/{BATCH_SIZE}")
        if len(batch) >= BATCH_SIZE:
            df = pd.DataFrame(batch)
            filename = f"{BUCKET}/data_{int(time.time())}.parquet"
            with fs.open(filename, 'wb') as f:
                df.to_parquet(f)
            print(f"Saved: {filename}")
            batch = []
    except Exception as e:
        print(f"Error: {e}")
