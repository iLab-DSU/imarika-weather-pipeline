
-- PostgreSQL initialization script for IMARIKA Weather Data Pipeline
-- Creates necessary tables for raw and clean weather data

-- Create raw weather data table
CREATE TABLE IF NOT EXISTS weather_raw (
    id SERIAL PRIMARY KEY,
    data TEXT NOT NULL,
    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create clean weather data table with proper schema
-- CREATE TABLE IF NOT EXISTS weather_clean (
--     id SERIAL PRIMARY KEY,
--     device_id VARCHAR(255) NOT NULL,
--     date VARCHAR(10) NOT NULL,
--     date_epoch INTEGER NOT NULL,
--     day TEXT NOT NULL,
--     is_anomaly BOOLEAN,
--     anomaly_score DOUBLE PRECISION,
--     processing_timestamp TIMESTAMP
-- );


CREATE TABLE IF NOT EXISTS weather_clean (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(255) NOT NULL,
    date VARCHAR(10) NOT NULL,
    date_epoch INTEGER NOT NULL,
    day TEXT NOT NULL,
    is_anomaly BOOLEAN,
    anomaly_score DOUBLE PRECISION,
    processing_timestamp TIMESTAMP
);


-- Optional test connection table (for JDBC or system health checks)
CREATE TABLE IF NOT EXISTS test_connection (
    id SERIAL PRIMARY KEY,
    status TEXT DEFAULT 'ok'
);
INSERT INTO test_connection (status) VALUES ('ready') ON CONFLICT DO NOTHING;


-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_weather_raw_received_at ON weather_raw(received_at);
CREATE INDEX IF NOT EXISTS idx_weather_clean_device_id ON weather_clean(device_id);
CREATE INDEX IF NOT EXISTS idx_weather_clean_date ON weather_clean(date);
CREATE INDEX IF NOT EXISTS idx_weather_clean_is_anomaly ON weather_clean(is_anomaly);

-- Grant necessary permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
