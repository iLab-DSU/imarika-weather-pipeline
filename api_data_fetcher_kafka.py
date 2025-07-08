import requests
import pandas as pd
import time
import json
from confluent_kafka import Producer
from dotenv import load_dotenv
import os
load_dotenv()

# # Your actual credentials
# EMAIL = "derrick.lubanga@strathmore.edu"
# PASSWORD = "Expendables"
EMAIL = os.getenv("EMAIL")
PASSWORD = os.getenv("PASSWORD")


# API endpoints
LOGIN_URL = "https://api.wirelessplanet.co.ke/api/v1/auth/login"
READINGS_URL = "https://api.wirelessplanet.co.ke/api/v1/readings"

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'kafka-broker-1:19092'  # Replace with your Kafka broker address
KAFKA_TOPIC = 'weather-readings-raw'

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}')

def login_and_get_token(email, password):
    """Login to API and get a fresh access token."""
    response = requests.post(LOGIN_URL, json={"email": email, "password": password})
    
    if response.status_code == 201:
        tokens = response.json()
        access_token = tokens["data"]["accessToken"]
        print("✅ Access token obtained successfully.")
        return access_token
    else:
        print(f"❌ Login failed. Status code: {response.status_code}")
        print(response.text)
        return None

def get_readings(access_token, producer):
    """Get readings data using the access token and send to Kafka."""
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(READINGS_URL, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data)
        print("✅ Data retrieved successfully.")
        print(df.head())  # Show first few rows
        
        # Send data to Kafka
        for index, row in df.iterrows():
            record = row.to_dict()
            try:
                producer.produce(KAFKA_TOPIC, key=str(index), value=json.dumps(record).encode('utf-8'), callback=delivery_report)
                producer.poll(0)  # Serve delivery callback queue
            except Exception as e:
                print(f"Failed to produce message: {e}")
        producer.flush() # Wait for all messages to be delivered
        print(f"✅ Data sent to Kafka topic: {KAFKA_TOPIC}\n")

    else:
        print(f"❌ Failed to retrieve data. Status code: {response.status_code}")
        print(response.text)

def main_loop():
    # Initialize Kafka Producer
    conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
    producer = Producer(conf)

    while True:
        # Step 1: Login to get access token
        token = login_and_get_token(EMAIL, PASSWORD)
        
        if token:
            # Step 2: Get readings and send to Kafka
            get_readings(token, producer)
        else:
            print("Skipping data retrieval due to login failure.")
        
        # Step 3: Wait for 3 hours (3*60*60 seconds)
        print("⏳ Sleeping for 3 hours...\n")
        time.sleep(3 * 60 * 60)

if __name__ == "__main__":
    main_loop()

