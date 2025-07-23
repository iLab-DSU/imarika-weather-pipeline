# IMARIKA Weather Data Processing Pipeline

A real-time weather data processing pipeline built with Apache Spark, Kafka, and PostgreSQL. This system fetches weather data from an external API, processes it through a streaming pipeline with data cleaning, imputation, and anomaly detection, then stores both raw and processed data in PostgreSQL.

## 🏗️ Architecture

```
Weather API → Kafka Producer → Kafka Topic → Spark Streaming → PostgreSQL
                                    ↓
                              [Data Processing Pipeline]
                              • Data Cleaning
                              • ML Imputation
                              • Anomaly Detection
                              • Daily Aggregation

         +-------------------+
         |   Weather API     |
         +--------+----------+
                  |
                  v
         +--------+----------+
         |   Kafka Producer  |
         | (api_data_fetcher)|   [Pushes data to Kafka Topic]
         +--------+----------+
                  |
                  v
         +-------------------+
         |   Kafka Topic     |
         | (weather-readings)|
         +--------+----------+
                  |
                  v
         +-----------------------------+
         |       Spark Streaming       |
         |   - Data Cleaning           |
         |   - ML Imputation           |
         |   - Anomaly Detection       |
         |   - Aggregation             |
         +--------+----------+--------+
                  |                   |
         +--------v--+        +-------v--------+
         | Raw Table |        | Processed Table|
         | PostgreSQL|        | PostgreSQL     |
         +--------+--+        +-------+--------+
                  |                   |
                  |         +---------v----------+
                  +-------->+ Visualization (BI) |
                            |   - Power BI       |
                            +--------------------+

                            +---------------------+
                            | Forecasting Layer   |
                            |   - Neural GCM      |
                            +---------------------+
```

## 📊 Features

- **Real-time Data Ingestion**: Fetches weather data from external API every 3 hours
- **Stream Processing**: Apache Spark Structured Streaming for real-time data processing
- **Data Quality**: Comprehensive data cleaning and validation
- **ML Pipeline**: Mean-based imputation and Z-score anomaly detection
- **Data Aggregation**: Daily weather summaries and statistics
- **Monitoring**: Kafka UI (Kafdrop) for stream monitoring
- **Containerized**: Fully dockerized environment for easy deployment

## 🛠️ Tech Stack

- **Stream Processing**: Apache Spark 3.3.0
- **Message Broker**: Apache Kafka with Zookeeper
- **Database**: PostgreSQL 14
- **Container Orchestration**: Docker Compose
- **Programming Language**: Python 3.x
- **ML Libraries**: PySpark MLlib

## 📁 Project Structure

```
imarika/
├── spark_streaming/                    # Main streaming application
│   ├── postgres-init/
│   │   └── 01-init.sql                # PostgreSQL table initialization
│   ├── api_data_fetcher_kafka.py      # Kafka producer - fetches API data
│   ├── weather_spark_processor.py     # Main Spark streaming application
│   ├── kafka_consumer.py              # Kafka consumer for testing
│   ├── docker-compose.yml             # Container orchestration
│   ├── start-pipeline.sh              # Pipeline startup script
│   ├── Dockerfile.consumer            # Consumer container
│   ├── Dockerfile.producer            # Producer container
│   ├── Dockerfile.spark               # Spark container
│   ├── requirements.txt               # Python dependencies
│   └── .env                          # Environment variables
└── README.md                         # This file
```

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- 8GB+ RAM recommended
- Weather API account (see setup below)

### 1. Create Weather API Account

Before running the pipeline, you need to create an account with the weather API:

#### Step 1: Sign Up
Use Postman or any HTTP client to create an account:

```bash
POST https://api.wirelessplanet.co.ke/api/v1/auth/signup
Content-Type: application/json

{
  "email": "your_email@example.com",
  "password": "yourSecurePassword"
}
```

**Response**: `201 Created` on success

#### Step 2: Verify Email
Check your email inbox and click the verification link sent to your email address.

#### Step 3: Test Login (Optional)
Verify your credentials work:

```bash
POST https://api.wirelessplanet.co.ke/api/v1/auth/login
Content-Type: application/json

{
  "email": "your_email@example.com",
  "password": "yourSecurePassword"
}
```

**Response Example**:
```json
{
  "access_token": "eyJhbGciOi...",
  "refresh_token": "eyJhbGciOi..."
}
```

> **Note**: The pipeline automatically handles token refresh by logging in every 3 hours, so you don't need to manage tokens manually.

### 2. Clone and navigate to project
```bash
git clone https://github.com/iLab-DSU/imarika-weather-pipeline.git
cd imarika-weather-pipeline
```
# Set up Python environment
```bash
python -m venv venv
venv\Scripts\activate  # Windows
source venv/Scripts/activate # Git 
# or
source venv/bin/activate  # macOS/Linux

# Install dependencies
pip install -r requirements.txt
```
### 3. Environment Setup

Create a `.env` file with your **verified** credentials:

```bash
# Weather API Credentials (use the same credentials from Step 1)
EMAIL=your_email@example.com
PASSWORD=yourSecurePassword

# Database Configuration
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=imarika
```


### 4. Run the Project

That's it! Just run:

```bash
sh start-pipeline.sh
```

**Note**: The script automatically handles:
- Making itself executable
- Stopping any existing containers
- Building and starting all services
- Setting up the complete pipeline

### 5. Verify Setup
-  Zookeeper (port 2181)
-  Kafka Broker (ports 9092, 19092)
-  Schema Registry (port 8081)
-  Kafdrop UI (port 9000)
-  Spark Master (port 8080)
-  Spark Worker
-  PostgreSQL (port 5433)
-  API Data Producer
-  Spark Streaming Processor

### 6. Monitor the Pipeline

- **Spark UI**: http://localhost:8080 - Monitor Spark jobs
- **Kafdrop**: http://localhost:9000 - Monitor Kafka topics and messages
- **PostgreSQL**: localhost:5433 - Access database directly

<!-- ## 🔧 Configuration

### Environment Variables(This can be put to the .env file)

| Variable | Description | Default |
|----------|-------------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address | `kafka-broker-1:19092` |
| `KAFKA_TOPIC_RAW` | Raw data topic | `weather-readings-raw` |
| `POSTGRES_URL` | PostgreSQL JDBC URL | `jdbc:postgresql://imarika-postgres:5432/imarika` |
| `MAX_OFFSETS_PER_TRIGGER` | Spark batch size | `1000` |
| `CHECKPOINT_LOCATION` | Spark checkpoint location | `/tmp/imarika/checkpoints` | -->

### Kafka Configuration

- **Topic**: `weather-readings-raw`
- **Partitions**: 1 (configurable)
- **Replication Factor**: 1

## Data Pipeline

### 1. Data Ingestion (`api_data_fetcher_kafka.py`)
- Authenticates with weather API using email/password
- Automatically refreshes access tokens every 3 hours
- Fetches readings and publishes to Kafka topic
- **API Endpoints Used**:
  - Login: `POST https://api.wirelessplanet.co.ke/api/v1/auth/login`
  - Readings: `GET https://api.wirelessplanet.co.ke/api/v1/readings`

### 2. Stream Processing (`weather_spark_processor.py`)

#### Data Schema
```json
{
  "message": "Request successful",
  "success": true,
  "data": {
    "reading_id": "uuid",
    "device_id": "string",
    "reading": {
      "valid": boolean,
      "uv_index": integer,
      "rain_gauge": integer,
      "wind_speed": integer,
      "air_humidity": integer,
      "peak_wind_gust": double,
      "air_temperature": double,
      "light_intensity": integer,
      "rain_accumulation": double,
      "barometric_pressure": integer,
      "wind_direction_sensor": integer
    },
    "created_at": "ISO timestamp"
  }
}
```

#### Processing Steps

1. **Data Cleaning**
   - Filter invalid readings
   - Validate numeric ranges
   - Handle missing values

2. **ML Imputation**
   - Mean-based imputation for numeric columns
   - Preserve data integrity

3. **Anomaly Detection**
   - Z-score based statistical anomaly detection
   - Configurable threshold (default: 10.0)

4. **Daily Aggregation**
   - Group by device and date
   - Calculate min/max/avg temperatures
   - Compute wind speeds, precipitation
   - Weather condition predictions

### 3. Data Storage

#### Raw Data Table (`weather_raw`)
```sql
CREATE TABLE weather_raw (
    id SERIAL PRIMARY KEY,
    data TEXT NOT NULL,
    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### Processed Data Table (`weather_clean`)
```sql
CREATE TABLE weather_clean (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(255) NOT NULL,
    date VARCHAR(10) NOT NULL,
    date_epoch INTEGER NOT NULL,
    maxtemp_c DOUBLE PRECISION,
    mintemp_c DOUBLE PRECISION,
    avgtemp_c DOUBLE PRECISION,
    maxwind_kph DOUBLE PRECISION,
    totalprecip_mm DOUBLE PRECISION,
    avghumidity INTEGER,
    daily_will_it_rain INTEGER,
    daily_chance_of_rain INTEGER,
    processing_timestamp TIMESTAMP,
    anomaly_score DOUBLE PRECISION,
    is_anomaly BOOLEAN
);
```

## 📈 Performance Metrics

- **Throughput**: ~40-50 records/second
- **Latency**: <30 seconds end-to-end
- **Data Compression**: 1000 raw readings → ~35 daily summaries
- **Memory Usage**: 1GB driver + 1GB executor

## 🔍 Monitoring & Troubleshooting

### View Logs
```bash
# View all logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f spark-submit
docker-compose logs -f producer
```

### Check Data Quality
```sql
-- Connect to PostgreSQL
psql -h localhost -p 5433 -U postgres -d imarika

-- Check raw data
SELECT COUNT(*) FROM weather_raw;

-- Check processed data
SELECT device_id, date, maxtemp_c, mintemp_c, is_anomaly 
FROM weather_clean 
ORDER BY date DESC 
LIMIT 10;
```

### Common Issues

1. **Out of Memory**: Increase Docker memory allocation to 8GB+
2. **Kafka Connection**: Check if all services are healthy: `docker-compose ps`
3. **API Authentication**: Verify credentials in `.env` file
4. **Database Connection**: Ensure PostgreSQL is accessible on port 5433

## 🛡️ Data Quality Validation

The pipeline includes comprehensive data validation:

-  **Schema Validation**: Ensures data matches expected structure
-  **Range Validation**: Temperature (-50°C to 60°C), Humidity (0-100%), etc.
-  **Null Handling**: Imputation for missing values
-  **Anomaly Detection**: Statistical outlier detection
-  **Integrity Checks**: Foreign key and constraint validation

## 🔧 Development

### Local Development Setup

**Quick Start (Recommended)**
```bash
# Run the complete pipeline locally
sh start-pipeline.sh
```

**For Component Development**

1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Run Individual Components**
   ```bash
   # Start infrastructure only (Kafka, PostgreSQL, Spark)
   docker-compose up zookeeper kafka-broker-1 postgres spark-master spark-worker
   
   # Run producer locally for testing
   python api_data_fetcher_kafka.py
   
   # Run Spark job locally
   spark-submit weather_spark_processor.py
   ```

### Testing

```bash
# Test Kafka consumer
python kafka_consumer.py

# Test database connection
python -c "import psycopg2; print('Connection successful')"
```

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📧 Contact

For questions or support, please contact the development team.

---

**Built with ❤️ for real-time weather data processing**

