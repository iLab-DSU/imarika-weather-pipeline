#!/bin/bash

MODE=$1

# Default to dev if no argument is provided
if [ -z "$MODE" ]; then
  MODE="dev"
fi

# Generate a unique ID based on timestamp
UNIQUE_ID=$(date +%Y%m%d%H%M%S)

# Clean up any existing containers and networks
echo "Stopping and removing any existing containers..."
docker-compose down -v

# Export the unique ID as an environment variable
export UNIQUE_ID

echo "Starting services with unique ID: $UNIQUE_ID"
echo "This will ensure no container naming conflicts occur"

# Run docker-compose with the unique ID
docker-compose -f docker-compose.yml up --build -d

# Choose flags based on mode
if [ "$MODE" == "deployment" ]; then
  docker-compose -f docker-compose.yml up --build -d
  echo "Services started in detached mode."
else
  docker-compose -f docker-compose.yml up --build
  echo "Services started in foreground (development mode)."
fi