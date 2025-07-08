
#!/usr/bin/env python3
"""
IMARIKA Weather Data Kafka Producer
This script fetches weather data from the Wireless Planet API and streams it to a Kafka topic.
"""

import json
import logging
import os
import sys
import time
import traceback
from datetime import datetime
from typing import Dict, Any, Optional

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('weather_kafka_producer.log')
    ]
)
logger = logging.getLogger('weather_kafka_producer')

# Configuration
class Config:
    """Configuration for the Kafka producer and API client"""
    # API settings
    API_URL = os.environ.get('API_URL', "https://api.wirelessplanet.co.ke/api/v1/readings")
    API_HEADERS = {"x-auth-token": os.environ.get('API_TOKEN', "strathmore")}
    API_TIMEOUT = int(os.environ.get('API_TIMEOUT', 30))  # seconds
    
    # Kafka settings
    KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka-broker-1:19092')
    KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'weather-readings-raw')
    
    # Application settings
    POLLING_INTERVAL = int(os.environ.get('POLLING_INTERVAL', 60))  # seconds
    MAX_RETRIES = int(os.environ.get('MAX_RETRIES', 5))
    RETRY_BACKOFF_FACTOR = int(os.environ.get('RETRY_BACKOFF_FACTOR', 2))  # seconds


class APIClient:
    """Client for interacting with the Wireless Planet API"""
    
    def __init__(self, url: str, headers: Dict[str, str], timeout: int):
        self.url = url
        self.headers = headers
        self.timeout = timeout
        logger.info(f"Initialized API client for {url}")
    
    def fetch_readings(self) -> Optional[Dict[str, Any]]:
        """
        Fetch weather readings from the API
        
        Returns:
            Dict containing the API response or None if the request failed
        """
        retry_count = 0
        max_retries = Config.MAX_RETRIES
        
        while retry_count <= max_retries:
            try:
                logger.info(f"Fetching data from {self.url} (attempt {retry_count + 1}/{max_retries + 1})")
                response = requests.get(
                    self.url,
                    headers=self.headers,
                    timeout=self.timeout
                )
                
                # Check if the request was successful
                response.raise_for_status()
                
                # Parse the JSON response
                data = response.json()
                data_size = len(str(data))
                logger.info(f"Successfully fetched data: {data_size} bytes")
                
                # Validate data structure
                if not self._validate_data_structure(data):
                    logger.warning("Data structure validation failed, retrying...")
                    retry_count += 1
                    time.sleep(Config.RETRY_BACKOFF_FACTOR * retry_count)
                    continue
                
                return data
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching data from API: {str(e)}")
                retry_count += 1
                
                if retry_count <= max_retries:
                    wait_time = Config.RETRY_BACKOFF_FACTOR * retry_count
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Max retries ({max_retries}) exceeded. Giving up.")
                    return None
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing JSON response: {str(e)}")
                retry_count += 1
                
                if retry_count <= max_retries:
                    wait_time = Config.RETRY_BACKOFF_FACTOR * retry_count
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Max retries ({max_retries}) exceeded. Giving up.")
                    return None
            except Exception as e:
                logger.error(f"Unexpected error: {str(e)}")
                logger.error(traceback.format_exc())
                retry_count += 1
                
                if retry_count <= max_retries:
                    wait_time = Config.RETRY_BACKOFF_FACTOR * retry_count
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Max retries ({max_retries}) exceeded. Giving up.")
                    return None
    
    def _validate_data_structure(self, data: Dict[str, Any]) -> bool:
        """
        Validate the structure of the API response
        
        Args:
            data: The data to validate
            
        Returns:
            bool: True if the data structure is valid, False otherwise
        """
        try:
            # Check if data is a dictionary
            if not isinstance(data, dict):
                logger.warning(f"Expected dict, got {type(data)}")
                return False
            
            # Check for required fields (adjust based on your API structure)
            required_fields = ["reading_id", "device_id", "reading"]
            for field in required_fields:
                if field not in data:
                    logger.warning(f"Missing required field: {field}")
                    return False
            
            # Check reading structure
            reading = data.get("reading", {})
            if not isinstance(reading, dict):
                logger.warning(f"Expected reading to be dict, got {type(reading)}")
                return False
            
            # Check for required reading fields
            required_reading_fields = ["valid", "air_temperature", "air_humidity"]
            for field in required_reading_fields:
                if field not in reading:
                    logger.warning(f"Missing required reading field: {field}")
                    return False
            
            return True
        except Exception as e:
            logger.error(f"Error validating data structure: {str(e)}")
            return False


class KafkaClient:
    """Client for producing messages to Kafka"""
    
    def __init__(self, bootstrap_servers: str, topic: str):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = self._create_producer()
        logger.info(f"Initialized Kafka producer for {bootstrap_servers}, topic: {topic}")
    
    def _create_producer(self) -> KafkaProducer:
        """Create and return a Kafka producer instance"""
        retry_count = 0
        max_retries = Config.MAX_RETRIES
        
        while retry_count <= max_retries:
            try:
                return KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',  # Wait for all replicas to acknowledge
                    retries=5,   # Retry sending on failure
                    linger_ms=5, # Small delay to allow batching
                    # Add error handling and reconnection logic
                    reconnect_backoff_ms=1000,
                    reconnect_backoff_max_ms=10000,
                    request_timeout_ms=30000
                )
            except KafkaError as e:
                logger.error(f"Failed to create Kafka producer: {str(e)}")
                retry_count += 1
                
                if retry_count <= max_retries:
                    wait_time = Config.RETRY_BACKOFF_FACTOR * retry_count
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Max retries ({max_retries}) exceeded. Giving up.")
                    raise
    
    def send_message(self, key: str, value: Dict[str, Any], headers: Dict[str, str] = None) -> bool:
        """
        Send a message to the Kafka topic
        
        Args:
            key: Message key
            value: Message value (will be serialized to JSON)
            headers: Optional message headers
            
        Returns:
            bool: True if message was sent successfully, False otherwise
        """
        try:
            # Convert headers to bytes if provided
            kafka_headers = None
            if headers:
                kafka_headers = [(k, v.encode('utf-8')) for k, v in headers.items()]
            
            # Send the message
            future = self.producer.send(
                self.topic,
                key=key.encode('utf-8') if key else None,
                value=value,
                headers=kafka_headers
            )
            
            # Block until the message is sent (or fails)
            record_metadata = future.get(timeout=10)
            
            logger.info(
                f"Message sent to topic {record_metadata.topic}, "
                f"partition {record_metadata.partition}, "
                f"offset {record_metadata.offset}"
            )
            return True
            
        except Exception as e:
            logger.error(f"Error sending message to Kafka: {str(e)}")
            logger.error(traceback.format_exc())
            return False
    
    def close(self):
        """Close the Kafka producer"""
        if self.producer:
            try:
                self.producer.flush()
                self.producer.close()
                logger.info("Kafka producer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {str(e)}")


class WeatherDataStreamer:
    """Main class for streaming weather data from API to Kafka"""
    
    def __init__(self, config: Config):
        self.config = config
        self.api_client = APIClient(
            config.API_URL,
            config.API_HEADERS,
            config.API_TIMEOUT
        )
        self.kafka_client = KafkaClient(
            config.KAFKA_BOOTSTRAP_SERVERS,
            config.KAFKA_TOPIC
        )
        self.last_timestamp = None
        self.health_check_interval = 10  # Check health every 10 iterations
        self.iteration_count = 0
        logger.info("Weather data streamer initialized")
    
    def process_data(self, data: Dict[str, Any]) -> None:
        """
        Process the data received from the API and send to Kafka
        
        Args:
            data: The data received from the API
        """
        if not data:
            logger.warning("No data to process")
            return
        
        # Add metadata
        timestamp = datetime.utcnow().isoformat()
        
        # Prepare headers
        headers = {
            'source': 'wireless-planet-api',
            'timestamp': timestamp,
            'version': '1.0'
        }
        
        # Generate a key based on timestamp or station ID if available
        key = str(int(time.time()))
        if 'device_id' in data:
            key = str(data['device_id'])
        
        # Add collection timestamp to the data
        enriched_data = {
            'reading_id': data.get('reading_id', f"auto-{int(time.time())}"),
            'device_id': data.get('device_id', 'unknown'),
            'reading': data.get('reading', {}),
            'metadata': {
                'collected_at': timestamp,
                'source': 'wireless-planet-api'
            }
        }
        
        # Validate data before sending
        if not self._validate_enriched_data(enriched_data):
            logger.warning("Enriched data validation failed, skipping message")
            return
        
        # Send to Kafka
        success = self.kafka_client.send_message(key, enriched_data, headers)
        if success:
            self.last_timestamp = timestamp
    
    def _validate_enriched_data(self, data: Dict[str, Any]) -> bool:
        """
        Validate the enriched data before sending to Kafka
        
        Args:
            data: The enriched data to validate
            
        Returns:
            bool: True if the data is valid, False otherwise
        """
        try:
            # Check for required fields
            required_fields = ['reading_id', 'device_id', 'reading', 'metadata']
            for field in required_fields:
                if field not in data:
                    logger.warning(f"Missing required field in enriched data: {field}")
                    return False
            
            # Check reading structure
            reading = data.get('reading', {})
            if not isinstance(reading, dict):
                logger.warning(f"Expected reading to be dict, got {type(reading)}")
                return False
            
            return True
        except Exception as e:
            logger.error(f"Error validating enriched data: {str(e)}")
            return False
    
    def check_health(self) -> bool:
        """
        Check the health of the API and Kafka connections
        
        Returns:
            bool: True if both connections are healthy, False otherwise
        """
        try:
            # Check API connection
            api_healthy = self.api_client.fetch_readings() is not None
            
            # Check Kafka connection
            kafka_healthy = self.kafka_client.send_message(
                "health_check",
                {"status": "ok", "timestamp": datetime.utcnow().isoformat()},
                {"type": "health_check"}
            )
            
            logger.info(f"Health check: API={api_healthy}, Kafka={kafka_healthy}")
            return api_healthy and kafka_healthy
        except Exception as e:
            logger.error(f"Health check failed: {str(e)}")
            return False
    
    def run(self) -> None:
        """Run the data streaming process"""
        logger.info("Starting weather data streaming process")
        
        try:
            while True:
                # Perform health check periodically
                self.iteration_count += 1
                if self.iteration_count % self.health_check_interval == 0:
                    logger.info("Performing periodic health check")
                    if not self.check_health():
                        logger.warning("Health check failed, but continuing operation")
                
                # Fetch data from API
                data = self.api_client.fetch_readings()
                
                # Process and send to Kafka
                if data:
                    self.process_data(data)
                
                # Wait for the next polling interval
                logger.info(f"Waiting {self.config.POLLING_INTERVAL} seconds until next poll")
                time.sleep(self.config.POLLING_INTERVAL)
                
        except KeyboardInterrupt:
            logger.info("Process interrupted by user")
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            logger.error(traceback.format_exc())
        finally:
            self.kafka_client.close()
            logger.info("Weather data streaming process stopped")


def main():
    """Main entry point"""
    try:
        # Initialize configuration
        config = Config()
        
        # Log configuration
        logger.info(f"API URL: {config.API_URL}")
        logger.info(f"Kafka Bootstrap Servers: {config.KAFKA_BOOTSTRAP_SERVERS}")
        logger.info(f"Kafka Topic: {config.KAFKA_TOPIC}")
        logger.info(f"Polling Interval: {config.POLLING_INTERVAL} seconds")
        
        # Create and run the streamer
        streamer = WeatherDataStreamer(config)
        streamer.run()
        
    except Exception as e:
        logger.error(f"Failed to start streaming process: {str(e)}")
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
# This code is designed to run as a standalone script.