#!/bin/bash

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
docker-compose -f docker-compose.yml up --build

# Note: Press Ctrl+C to stop all services when done
