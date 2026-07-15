from kafka import KafkaProducer
import json
import time

# 1. Connect to the Redpanda broker
# We use localhost:19092 because that's the port we mapped in docker-compose.
producer = KafkaProducer(
    bootstrap_servers=['localhost:19092'], #location of redpanda running
    value_serializer=lambda v: json.dumps(v).encode('utf-8') # like a translator  used to serialize the message to JSON format before sending it to Kafka
    # here v is our raw message and dumps turn em to JSON string and encode turns it to bytes
)

# 2. Send a "Hello World" message
topic = 'market_data' #like a folder
message = {'hello': 'world', 'timestamp': time.time()}

print(f"Sending: {message} to topic: {topic}")
producer.send(topic, value=message)

# 3. Ensure the message is actually sent
producer.flush()
print("Message sent successfully!")
