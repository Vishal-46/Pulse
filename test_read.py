import json
from kafka import KafkaConsumer
import os
from dotenv import load_dotenv

load_dotenv()
consumer = KafkaConsumer('market_data', bootstrap_servers=[os.getenv('BOOTSTRAP_SERVERS')], auto_offset_reset='earliest')
for msg in consumer:
    print(f"Message: {msg.value}")
    break
