#!/bin/bash
# Pipeline Validation Script
# This script helps validate the IMARIKA Weather Data Pipeline

# Set colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}IMARIKA Weather Data Pipeline Validation Script${NC}"
echo "========================================================"
echo

# Function to check if a service is running
check_service() {
  local service_name=$1
  echo -e "\n${YELLOW}Checking $service_name...${NC}"
  
  if docker ps | grep -q "$service_name"; then
    echo -e "${GREEN}✓ $service_name is running${NC}"
    return 0
  else
    echo -e "${RED}✗ $service_name is not running${NC}"
    return 1
  fi
}

# Function to check Kafka topics
check_kafka_topics() {
  echo -e "\n${YELLOW}Checking Kafka topics...${NC}"
  
  if docker exec imarika-kafka-broker kafka-topics --bootstrap-server localhost:9092 --list | grep -q "weather-readings-raw"; then
    echo -e "${GREEN}✓ Topic 'weather-readings-raw' exists${NC}"
    
    # Check if topic has messages
    MSG_COUNT=$(docker exec imarika-kafka-broker kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic weather-readings-raw --time -1 | awk -F ":" '{sum += $3} END {print sum}')
    
    if [ "$MSG_COUNT" -gt 0 ]; then
      echo -e "${GREEN}✓ Topic has $MSG_COUNT messages${NC}"
    else
      echo -e "${RED}✗ Topic has no messages${NC}"
    fi
    
    return 0
  else
    echo -e "${RED}✗ Topic 'weather-readings-raw' does not exist${NC}"
    return 1
  fi
}

# Function to check PostgreSQL tables
check_postgres_tables() {
  echo -e "\n${YELLOW}Checking PostgreSQL tables...${NC}"
  
  # Check weather_raw table
  RAW_COUNT=$(docker exec imarika-postgres psql -U postgres -d imarika -t -c "SELECT COUNT(*) FROM weather_raw;")
  if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Table 'weather_raw' exists${NC}"
    echo -e "  - Contains ${GREEN}$RAW_COUNT${NC} records"
  else
    echo -e "${RED}✗ Table 'weather_raw' does not exist${NC}"
  fi
  
  # Check weather_clean table
  CLEAN_COUNT=$(docker exec imarika-postgres psql -U postgres -d imarika -t -c "SELECT COUNT(*) FROM weather_clean;")
  if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Table 'weather_clean' exists${NC}"
    echo -e "  - Contains ${GREEN}$CLEAN_COUNT${NC} records"
  else
    echo -e "${RED}✗ Table 'weather_clean' does not exist${NC}"
  fi
}

# Function to check Spark logs
check_spark_logs() {
  echo -e "\n${YELLOW}Checking Spark logs for errors...${NC}"
  
  ERROR_COUNT=$(docker logs imarika-spark-submit 2>&1 | grep -c "ERROR")
  WARNING_COUNT=$(docker logs imarika-spark-submit 2>&1 | grep -c "WARN")
  
  echo -e "Found ${RED}$ERROR_COUNT${NC} errors and ${YELLOW}$WARNING_COUNT${NC} warnings in Spark logs"
  
  if [ "$ERROR_COUNT" -gt 0 ]; then
    echo -e "\n${YELLOW}Last 5 errors:${NC}"
    docker logs imarika-spark-submit 2>&1 | grep "ERROR" | tail -5
  fi
}

# Function to check Kafka consumer groups
check_consumer_groups() {
  echo -e "\n${YELLOW}Checking Kafka consumer groups...${NC}"
  
  CONSUMER_GROUPS=$(docker exec imarika-kafka-broker kafka-consumer-groups --bootstrap-server localhost:9092 --list)
  
  if [ -z "$CONSUMER_GROUPS" ]; then
    echo -e "${RED}✗ No consumer groups found${NC}"
  else
    echo -e "${GREEN}✓ Consumer groups found:${NC}"
    echo "$CONSUMER_GROUPS"
    
    # Check for Spark consumer group
    if echo "$CONSUMER_GROUPS" | grep -q "spark-kafka"; then
      echo -e "\n${YELLOW}Checking Spark consumer group details:${NC}"
      docker exec imarika-kafka-broker kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group spark-kafka-source
    fi
  fi
}

# Function to test end-to-end data flow
test_data_flow() {
  echo -e "\n${YELLOW}Testing end-to-end data flow...${NC}"
  
  # Check if data is flowing from Kafka to PostgreSQL
  BEFORE_COUNT=$(docker exec imarika-postgres psql -U postgres -d imarika -t -c "SELECT COUNT(*) FROM weather_raw;" | tr -d '[:space:]')
  
  echo "Current record count in weather_raw: $BEFORE_COUNT"
  echo "Waiting 2 minutes to check for new data..."
  sleep 120
  
  AFTER_COUNT=$(docker exec imarika-postgres psql -U postgres -d imarika -t -c "SELECT COUNT(*) FROM weather_raw;" | tr -d '[:space:]')
  echo "New record count in weather_raw: $AFTER_COUNT"
  
  if [ "$AFTER_COUNT" -gt "$BEFORE_COUNT" ]; then
    echo -e "${GREEN}✓ New data is flowing into PostgreSQL${NC}"
  else
    echo -e "${RED}✗ No new data detected in PostgreSQL${NC}"
  fi
}

# Main validation process
echo "Starting validation checks..."

# Check if all services are running
check_service "imarika-zookeeper"
check_service "imarika-kafka-broker"
check_service "imarika-postgres"
check_service "imarika-spark-master"
check_service "imarika-spark-worker"
check_service "imarika-spark-submit"
check_service "imarika-kafka-producer"

# Check Kafka topics
check_kafka_topics

# Check PostgreSQL tables
check_postgres_tables

# Check Spark logs
check_spark_logs

# Check Kafka consumer groups
check_consumer_groups

# Ask if user wants to test end-to-end data flow
echo -e "\n${YELLOW}Do you want to test end-to-end data flow? (y/n)${NC}"
read -r response
if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]; then
  test_data_flow
fi

echo -e "\n${YELLOW}Validation complete!${NC}"
echo "For more detailed diagnostics, check the logs of each service:"
echo "  - Kafka Producer: docker logs imarika-kafka-producer"
echo "  - Spark Submit: docker logs imarika-spark-submit"
echo "  - PostgreSQL: docker logs imarika-postgres"
