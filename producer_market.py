import yfinance as yf
from kafka import KafkaProducer
import json
import time

# 1. Connect to the Redpanda broker
# We use localhost:19092 because that's the port we mapped in docker-compose.
producer = KafkaProducer(
    bootstrap_servers=['localhost:19092'],
    # Automatically convert Python dictionaries to JSON and then to bytes
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 2. Configuration for our ingestion
TICKER = "^GSPC"  # S&P 500 index
TOPIC = "market_data"

def fetch_and_send():
    # Fetch the latest data point for the index
    ticker_data = yf.Ticker(TICKER)
    # 'fast_info' is a quick way to get the current price without downloading a huge history
    current_price = ticker_data.fast_info['last_price']
    
    # Create our message structure
    message = {
        'ticker': TICKER,
        'price': current_price,
        'timestamp': time.time()
    }
    
    # Send the data to the broker
    print(f"Sending: {message}")
    producer.send(TOPIC, value=message)
    # Ensure the message hits the broker
    producer.flush()

# 3. The Polling Loop
# This runs forever until you stop it (Ctrl+C)
print(f"Starting producer for {TICKER}...")
while True:
    try:
        fetch_and_send()
        # Wait for 60 seconds before fetching again
        time.sleep(60) 
    except Exception as e:
        print(f"Error fetching data: {e}")
        # If there's an error, wait a bit before trying again
        time.sleep(10)
