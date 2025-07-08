#!/bin/bash
# Save as check-kafka-spark.sh

# Get container IDs
KAFKA_CONTAINER=$(docker ps | grep imarika-kafka-broker | awk '{print $1}')
SPARK_CONTAINER=$(docker ps | grep imarika-spark-submit | awk '{print $1}')

echo "Checking Kafka topics..."
docker exec $KAFKA_CONTAINER kafka-topics --bootstrap-server localhost:9092 --list

echo "Checking if producer is sending data..."
docker logs $KAFKA_CONTAINER | grep -i "weather-readings-raw"

echo "Checking Spark can reach Kafka..."
docker exec $SPARK_CONTAINER ping -c 2 $(docker inspect $KAFKA_CONTAINER | grep IPAddress | tail -1 | awk -F'"' '{print $4}')

echo "Checking Kafka logs for connection attempts..."
docker logs $KAFKA_CONTAINER | grep -i "connection"
# Use a more reliable method to check if Spark job is running
echo "Checking Spark job status..."
docker exec $SPARK_CONTAINER jps | grep -q "SparkSubmit" && echo "Spark job is running" || echo "No Spark job running"

# Instead of directly trying to cat files that might not exist
echo "Checking Spark job output..."
docker exec $SPARK_CONTAINER bash -c "if [ -f /tmp/spark-output.txt ]; then cat /tmp/spark-output.txt; else echo 'Output file not found'; fi"

echo "Checking Spark job errors..."
docker exec $SPARK_CONTAINER bash -c "if [ -f /tmp/spark-errors.txt ]; then cat /tmp/spark-errors.txt; else echo 'Error file not found'; fi"

echo "Checking Spark job metrics..."
docker exec $SPARK_CONTAINER bash -c "if [ -f /tmp/spark-metrics.txt ]; then cat /tmp/spark-metrics.txt; else echo 'Metrics file not found'; fi"