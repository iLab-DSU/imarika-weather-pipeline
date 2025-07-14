
#!/usr/bin/env python3
# """
# IMARIKA Weather Data Processing Pipeline - Structured Streaming with foreachBatch
# ----------------------------------------
# This Spark Streaming application processes weather data from Kafka,
# performs data cleaning, mean-based imputation, anomaly detection,
# and writes both raw and cleaned daily aggregates to PostgreSQL.
# """

import os
import logging
import time
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, lit, current_timestamp, date_format, unix_timestamp, when, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, IntegerType, TimestampType
from pyspark.ml.feature import Imputer, VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.sql.types import *

import pyspark.sql.functions as F


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("weather_spark_processor")

# Environment variables with defaults
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker-1:19092")
KAFKA_TOPIC_RAW = os.environ.get("KAFKA_TOPIC_RAW", "weather-readings-raw")
POSTGRES_URL = os.environ.get("POSTGRES_URL", "jdbc:postgresql://imarika-postgres:5432/imarika")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")
POSTGRES_DRIVER = "org.postgresql.Driver"
POSTGRES_TABLE_RAW = os.environ.get("POSTGRES_TABLE_RAW", "weather_raw")
POSTGRES_TABLE_CLEAN = os.environ.get("POSTGRES_TABLE_CLEAN", "weather_clean")
CHECKPOINT_LOCATION = os.environ.get("CHECKPOINT_LOCATION", "/tmp/imarika/checkpoints")
MAX_OFFSETS_PER_TRIGGER = int(os.environ.get("MAX_OFFSETS_PER_TRIGGER", "1000"))


def create_spark_session():
    """Create and configure Spark session"""
    return (SparkSession.builder
            .appName("IMARIKA Weather Data Processor")
            .config("spark.jars.packages", \
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.5.1")
            .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION)
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
            .config("spark.executor.memory", "1g")
            .config("spark.driver.memory", "1g")
            .getOrCreate())


def define_raw_schema():
    """Schema for raw weather data from Kafka with schema evolution support"""
    return StructType([
        StructField("reading_id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("reading", StructType([
            StructField("valid", BooleanType(), True),
            StructField("uv_index", IntegerType(), True),  # Changed from DoubleType to IntegerType
            StructField("rain_gauge", IntegerType(), True),  # Changed from DoubleType to IntegerType  
            StructField("wind_speed", IntegerType(), True),  # Changed from DoubleType to IntegerType
            StructField("air_humidity", IntegerType(), True),
            StructField("peak_wind_gust", DoubleType(), True),
            StructField("air_temperature", DoubleType(), True),
            StructField("light_intensity", IntegerType(), True),
            StructField("rain_accumulation", DoubleType(), True),
            StructField("barometric_pressure", IntegerType(), True),
            StructField("wind_direction_sensor", IntegerType(), True)
        ]), True),
        StructField("created_at", StringType(), True)  # Changed from TimestampType to StringType
    ])

def define_outer_schema():
    """Schema for the complete JSON structure from Kafka"""
    return StructType([
        StructField("message", StringType(), True),
        StructField("success", BooleanType(), True),
        StructField("data", define_raw_schema(), True)
    ])

def basic_data_cleaning(df):
    """Filter invalid readings and apply data quality rules"""
    try:
        logger.info("Starting basic data cleaning")
        # Log input schema for debugging
        logger.info(f"Input DataFrame schema: {df.schema.simpleString()}")
        logger.info(f"Input DataFrame columns: {df.columns}")

        # Show sample data for debugging
        logger.info("Sample input data:")
        df.show(5, truncate=False)

        # Check if 'valid' column exists and filter
        if "valid" in df.columns:
            df_valid = df.filter(col("valid") == True).cache()
            valid_count = df_valid.count()
            invalid_count = df.count() - valid_count
            logger.info(f"Valid readings: {valid_count} out of {df.count()}, filtered out {invalid_count} invalid readings")
        else:
            logger.warning("No 'valid' column found; proceeding without filtering")
            df_valid = df.cache()
            valid_count = df_valid.count()
            logger.info(f"Total readings (no valid filter): {valid_count}")

        # DataFrame is already flattened, so select required columns
        df_flat = df_valid.select(
            col("reading_id"),
            col("device_id"),
            col("uv_index"),
            col("rain_gauge"),
            col("wind_speed"),
            col("air_humidity"),
            col("peak_wind_gust"),
            col("air_temperature"),
            col("light_intensity"),
            col("rain_accumulation"),
            col("barometric_pressure"),
            col("wind_direction_sensor"),
            col("processing_timestamp")
        )

        # Clean invalid numeric ranges based on realistic values
        cleaned_df = df_flat.withColumn(
            "air_temperature", when(col("air_temperature").between(-50, 60), col("air_temperature")).otherwise(None)
        ).withColumn(
            "air_humidity", when(col("air_humidity").between(0, 100), col("air_humidity")).otherwise(None)
        ).withColumn(
            "wind_speed", when(col("wind_speed") >= 0, col("wind_speed")).otherwise(None)
        ).withColumn(
            "uv_index", when(col("uv_index").between(0, 15), col("uv_index")).otherwise(None)  # UV can go higher than 12
        ).withColumn(
            "barometric_pressure",
            when(col("barometric_pressure").between(87000, 108000), col("barometric_pressure")).otherwise(None)
        ).withColumn(
            "rain_gauge", when(col("rain_gauge") >= 0, col("rain_gauge")).otherwise(None)  # Rain gauge should be non-negative
        ).withColumn(
            "rain_accumulation", when(col("rain_accumulation") >= 0, col("rain_accumulation")).otherwise(None)  # Rain accumulation should be non-negative
        ).withColumn(
            "peak_wind_gust", when(col("peak_wind_gust") >= 0, col("peak_wind_gust")).otherwise(None)  # Wind gust should be non-negative
        ).withColumn(
            "light_intensity", when(col("light_intensity") >= 0, col("light_intensity")).otherwise(None)  # Light intensity should be non-negative
        ).withColumn(
            "wind_direction_sensor", when(col("wind_direction_sensor").between(0, 360), col("wind_direction_sensor")).otherwise(None)  # Wind direction 0-360 degrees
        ).withColumn(
            "date",
            date_format(col("processing_timestamp"), "yyyy-MM-dd")
        )

        # Log cleaning statistics
        numeric_cols = ["air_temperature", "air_humidity", "wind_speed", "uv_index", "barometric_pressure", 
                       "rain_gauge", "rain_accumulation", "peak_wind_gust", "light_intensity", "wind_direction_sensor"]
        null_counts = {col_name: cleaned_df.filter(cleaned_df[col_name].isNull()).count() for col_name in numeric_cols}
        logger.info(f"Null counts after cleaning: {null_counts}")

        # Show sample cleaned data
        logger.info("Sample cleaned data:")
        cleaned_df.show(5, truncate=False)

        return cleaned_df
        
    except Exception as e:
        logger.error(f"Error in basic_data_cleaning: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def apply_ml_imputation(df):
    """Mean-based imputation on numeric columns"""
    try:
        logger.info("Starting ML imputation")

        numerical_cols = [
            "air_temperature", "air_humidity", "wind_speed",
            "uv_index", "barometric_pressure", "peak_wind_gust",
            "light_intensity", "rain_accumulation"
        ]

        # Log null counts before imputation
        null_counts_before = {col_name: df.filter(df[col_name].isNull()).count() for col_name in numerical_cols}
        logger.info(f"Null counts before imputation: {null_counts_before}")

        # Check if all rows have null in all numerical columns
        non_null_df = df.select(numerical_cols).dropna(how="all")
        if non_null_df.count() == 0:
            logger.warning("All numerical columns are null for all rows — skipping imputation.")
            return df

        # Proceed with imputation
        assembler = VectorAssembler(
            inputCols=numerical_cols,
            outputCol="features",
            handleInvalid="keep"
        )

        imputer = Imputer(
            inputCols=numerical_cols,
            outputCols=[f"{c}_imputed" for c in numerical_cols],
            strategy="mean"
        )

        df_assembled = assembler.transform(df)
        df_imputed = imputer.fit(df_assembled).transform(df_assembled)

        # Replace null values with imputed values
        for c in numerical_cols:
            df_imputed = df_imputed.withColumn(
                c,
                when(col(c).isNull(), col(f"{c}_imputed")).otherwise(col(c))
            )

        result_df = df_imputed.drop(*[f"{c}_imputed" for c in numerical_cols], "features")

        # Log null counts after imputation
        null_counts_after = {col_name: result_df.filter(result_df[col_name].isNull()).count() for col_name in numerical_cols}
        logger.info(f"Null counts after imputation: {null_counts_after}")

        return result_df

    except Exception as e:
        logger.error(f"Error in apply_ml_imputation: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def detect_anomalies_simple(df):
    """Simple statistical anomaly detection using Z-score"""
    try:
        logger.info("Starting simple anomaly detection")
        feature_cols = ["air_temperature", "air_humidity", "wind_speed", "uv_index", "barometric_pressure"]

        # Check if there is any usable data
        non_null_df = df.select(feature_cols).dropna(how="all")
        if non_null_df.count() == 0:
            logger.warning("All feature values are null — skipping anomaly detection")
            return df.withColumn("anomaly_score", lit(0.0)).withColumn("is_anomaly", lit(False))

        # Calculate statistics for each feature
        stats = df.select([
            F.mean(col).alias(f"{col}_mean") for col in feature_cols
        ] + [
            F.stddev(col).alias(f"{col}_stddev") for col in feature_cols
        ]).collect()[0]

        # Calculate anomaly score based on how many standard deviations away from mean
        anomaly_expr = lit(0.0)
        for col_name in feature_cols:
            mean_val = stats[f"{col_name}_mean"]
            stddev_val = stats[f"{col_name}_stddev"]
            
            if mean_val is not None and stddev_val is not None and stddev_val > 0:
                z_score = F.abs((col(col_name) - lit(mean_val)) / lit(stddev_val))
                anomaly_expr = anomaly_expr + z_score

        result_df = df.withColumn("anomaly_score", anomaly_expr) \
                     .withColumn("is_anomaly", col("anomaly_score") > lit(10.0))  # Threshold for anomaly

        # Log stats
        anomaly_count = result_df.filter(result_df["is_anomaly"]).count()
        total_count = result_df.count()
        logger.info(f"Detected {anomaly_count} anomalies out of {total_count} records using simple Z-score method")

        return result_df

    except Exception as e:
        logger.error(f"Error in detect_anomalies_simple: {str(e)}")
        logger.error(traceback.format_exc())
        # Return DataFrame with default values if anomaly detection fails
        return df.withColumn("anomaly_score", lit(0.0)).withColumn("is_anomaly", lit(False))

def aggregate_daily_data(df):
    """Aggregate cleaned & imputed data to daily metrics"""
    try:
        logger.info("Starting daily data aggregation")
        
        # Log input DataFrame schema and columns for debugging
        logger.info(f"Input DataFrame columns: {df.columns}")
        logger.info(f"Input DataFrame schema: {df.schema.simpleString()}")
        
        # Check if 'date' column exists, if not create it
        if "date" not in df.columns:
            logger.info("Date column not found, creating from processing_timestamp")
            df = df.withColumn("date", date_format(col("processing_timestamp"), "yyyy-MM-dd"))
        
        # Verify required columns exist
        required_cols = ["device_id", "date", "air_temperature", "wind_speed", "rain_gauge", 
                        "air_humidity", "barometric_pressure"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            logger.error(f"Missing required columns for aggregation: {missing_cols}")
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        # Check if anomaly columns exist, add defaults if not
        if "anomaly_score" not in df.columns:
            logger.info("Adding default anomaly_score column")
            df = df.withColumn("anomaly_score", lit(0.0))
        
        if "is_anomaly" not in df.columns:
            logger.info("Adding default is_anomaly column")
            df = df.withColumn("is_anomaly", lit(False))
        
        # Perform aggregation with error handling for each metric
        df_daily = df.groupBy("device_id", "date").agg(
            F.max("air_temperature").alias("maxtemp_c"),
            F.min("air_temperature").alias("mintemp_c"),
            F.avg("air_temperature").alias("avgtemp_c"),
            F.max(col("wind_speed") * 3.6).alias("maxwind_kph"),  # Convert m/s to km/h
            F.sum("rain_gauge").alias("totalprecip_mm"),
            F.avg("air_humidity").cast(IntegerType()).alias("avghumidity"),
            when(F.sum("rain_gauge") > 0, lit(1)).otherwise(lit(0)).alias("daily_will_it_rain"),
            when((F.avg("air_humidity") > 70) & (F.avg("barometric_pressure") < 100000), lit(70)).otherwise(lit(0)).alias("daily_chance_of_rain"),
            F.max("processing_timestamp").alias("processing_timestamp"),
            unix_timestamp(col("date"), "yyyy-MM-dd").cast(IntegerType()).alias("date_epoch"),
            F.max("anomaly_score").alias("anomaly_score"),
            F.max(when(col("is_anomaly") == True, 1).otherwise(0)).cast("boolean").alias("is_anomaly")
        )
        
        # Log aggregation results
        aggregated_count = df_daily.count()
        original_count = df.count()
        logger.info(f"Aggregated {original_count} records into {aggregated_count} daily summaries")
        
        return df_daily

    except Exception as e:
        logger.error(f"Error in aggregate_daily_data: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def validate_data_quality(df, table_name):
    """Validate data quality before writing to database"""
    try:
        logger.info(f"Validating data quality for {table_name}")
        
        # Check if DataFrame is empty
        if df.isEmpty():
            logger.warning(f"DataFrame for {table_name} is empty")
            return False
            
        # Check for nulls in critical columns
        if table_name == POSTGRES_TABLE_CLEAN:
            null_counts = {
                "device_id": df.filter(df["device_id"].isNull()).count(),
                "date": df.filter(df["date"].isNull()).count()
            }
            logger.info(f"Null counts in critical columns: {null_counts}")
            
            if any(count > 0 for count in null_counts.values()):
                logger.warning(f"Critical columns contain NULL values: {null_counts}")
                return False
            
            # Check for reasonable temperature ranges
            stats = df.select(
                F.min("mintemp_c").alias("min_temp"),
                F.max("maxtemp_c").alias("max_temp"),
                F.avg("avghumidity").alias("avg_humidity")
            ).collect()[0]
            
            logger.info(f"Data stats: min_temp={stats['min_temp']}, max_temp={stats['max_temp']}, avg_humidity={stats['avg_humidity']}")
            
            # Return True if data passes quality checks
            quality_check = all([
                stats['min_temp'] is not None,
                stats['max_temp'] is not None,
                stats['min_temp'] <= stats['max_temp'],
                stats['avg_humidity'] is not None and 0 <= stats['avg_humidity'] <= 100
            ])
            
            if not quality_check:
                logger.warning("Data quality check failed for aggregated values")
                return False
                
        return True
    except Exception as e:
        logger.error(f"Error in validate_data_quality: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def write_batch_to_postgres(batch_df, table, batch_id):
    """Write batch data to PostgreSQL with enhanced error handling"""
    try:
        logger.info(f"Writing batch {batch_id} to {table}")
        
        # Check if DataFrame is empty
        if batch_df.isEmpty():
            logger.warning(f"Batch {batch_id} is empty, skipping write to {table}")
            return False
        
        # Log the actual schema being written
        logger.info(f"DataFrame schema for {table}: {batch_df.schema}")
        logger.info(f"DataFrame columns: {batch_df.columns}")
        
        # Show sample data for debugging
        logger.info(f"Sample data for {table}:")
        batch_df.show(5, truncate=False)
        
        # Validate data quality
        if not validate_data_quality(batch_df, table):
            logger.warning(f"Data quality validation failed for batch {batch_id}, skipping write to {table}")
            return False
            
        # Prepare data for raw table
        if table == POSTGRES_TABLE_RAW:
            batch_df = batch_df.withColumn("data", to_json(struct("*"))).select("data")
        
        # Write to PostgreSQL with detailed error reporting
        (batch_df.write
             .format("jdbc")
             .option("url", POSTGRES_URL)
             .option("dbtable", table)
             .option("user", POSTGRES_USER)
             .option("password", POSTGRES_PASSWORD)
             .option("driver", POSTGRES_DRIVER)
             .mode("append")
             .save())
        
        logger.info(f"Successfully wrote {batch_df.count()} records from batch {batch_id} to {table}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to write batch {batch_id} to {table}: {str(e)}")
        logger.error(f"Exception type: {type(e).__name__}")
        logger.error(f"Exception args: {e.args}")
        logger.error(traceback.format_exc())
        
        # Try to get more specific JDBC error information
        if "java.sql" in str(e) or "JDBC" in str(e):
            logger.error("This appears to be a JDBC/SQL error. Check:")
            logger.error("1. Table schema matches DataFrame schema")
            logger.error("2. PostgreSQL connection is working")
            logger.error("3. Table exists and has correct permissions")
            
        return False

def process_batch(batch_df, batch_id):
    """Process a batch of data with metrics collection and error handling"""
    try:
        start_time = time.time()
        
        # Log batch size
        batch_count = batch_df.count()
        logger.info(f"Processing batch {batch_id} with {batch_count} records")
        
        if batch_count == 0:
            logger.warning(f"Empty batch {batch_id}, skipping processing")
            return
        
        # Raw write
        raw_write_success = write_batch_to_postgres(batch_df, POSTGRES_TABLE_RAW, batch_id)
        
        # Only proceed with processing if raw write was successful
        if raw_write_success:
            # Cleaning
            cleaned = basic_data_cleaning(batch_df)
            cleaned_count = cleaned.count()
            logger.info(f"After cleaning: {cleaned_count} records")
            
            # Imputation
            imputed = apply_ml_imputation(cleaned)
            imputed_count = imputed.count()
            logger.info(f"After imputation: {imputed_count} records")
            
            # Anomaly detection
            flagged = detect_anomalies_simple(imputed)
            flagged_count = flagged.count()
            logger.info(f"After anomaly detection: {flagged_count} records")
            
            # Aggregation
            aggregated = aggregate_daily_data(flagged)
            aggregated_count = aggregated.count()
            logger.info(f"After aggregation: {aggregated_count} records")
            
            # Clean write
            clean_write_success = write_batch_to_postgres(aggregated, POSTGRES_TABLE_CLEAN, batch_id)
            
            # Record metrics
            processing_time = time.time() - start_time
            logger.info(f"Batch {batch_id} processing metrics: count={batch_count}, time={processing_time:.2f}s, " +
                        f"rate={batch_count/processing_time:.2f} records/s, " +
                        f"raw_write={raw_write_success}, clean_write={clean_write_success}")
        else:
            logger.warning(f"Skipping further processing for batch {batch_id} due to raw write failure")
    except Exception as e:
        logger.error(f"Error processing batch {batch_id}: {str(e)}")
        logger.error(traceback.format_exc())

def verify_postgres_tables():
    """Verify that PostgreSQL tables exist with correct schema"""
    try:
        logger.info("Verifying PostgreSQL tables")
        spark = SparkSession.builder.getOrCreate()
        
        # Create a JDBC connection to PostgreSQL
        jdbc_url = f"{POSTGRES_URL}?user={POSTGRES_USER}&password={POSTGRES_PASSWORD}"
        
        # Check raw table
        try:
            raw_df = spark.read.format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", f"(SELECT * FROM {POSTGRES_TABLE_RAW} LIMIT 0) as tmp") \
                .load()
            logger.info(f"Raw table schema: {raw_df.schema.simpleString()}")
        except Exception as e:
            logger.warning(f"Failed to verify raw table: {str(e)}")
            logger.warning("Will attempt to create raw table if it doesn't exist")

        # Check clean table
        try:
            clean_df = spark.read.format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", f"(SELECT * FROM {POSTGRES_TABLE_CLEAN} LIMIT 0) as tmp") \
                .load()
            logger.info(f"Clean table schema: {clean_df.schema.simpleString()}")
        except Exception as e:
            logger.warning(f"Failed to verify clean table: {str(e)}")
            logger.warning("Will attempt to create clean table if it doesn't exist")
            
        return True
    except Exception as e:
        logger.error(f"Failed to verify PostgreSQL tables: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def test_postgres_connection():
    """Test direct connection to PostgreSQL"""
    try:
        logger.info("Testing PostgreSQL connection")
        spark = SparkSession.builder.getOrCreate()
        
        # Create a test DataFrame
        test_df = spark.createDataFrame([("test_connection",)], ["col1"])
        
        # Try to write to PostgreSQL
        test_df.write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", "test_connection") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", POSTGRES_DRIVER) \
            .mode("overwrite") \
            .save()
            
        logger.info("PostgreSQL connection test successful")
        return True
    except Exception as e:
        logger.error(f"PostgreSQL connection test failed: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def main():
    """Main entry point for the Spark Streaming application"""
    try:
        logger.info("Starting IMARIKA Weather Data Processing Pipeline with foreachBatch")

        # Create Spark session
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")

        # Verify PostgreSQL tables
        tables_ok = verify_postgres_tables()
        if not tables_ok:
            logger.warning("PostgreSQL tables verification failed, but continuing anyway")

        # Test PostgreSQL connection
        connection_ok = test_postgres_connection()
        if not connection_ok:
            logger.error("PostgreSQL connection test failed, check your connection settings")
            return

        # Define schema for raw data
        outer_schema = define_outer_schema()

        # Create streaming DataFrame from Kafka
        df_kafka_raw = (spark.readStream
                        .format("kafka")
                        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                        .option("subscribe", KAFKA_TOPIC_RAW)
                        .option("startingOffsets", "earliest")
                        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
                        .load())

        # Parse JSON data with outer schema
        df_parsed = df_kafka_raw.select(
            from_json(col("value").cast("string"), outer_schema).alias("parsed")
        ).select("parsed.data.*")

        # Flatten the 'reading' struct and convert types properly
        df_flat = df_parsed.select(
            col("reading_id"),
            col("device_id"),
            col("reading.valid").alias("valid"),
            col("reading.uv_index").cast(DoubleType()).alias("uv_index"),  # Convert to double for consistency
            col("reading.rain_gauge").cast(DoubleType()).alias("rain_gauge"),  # Convert to double
            col("reading.wind_speed").cast(DoubleType()).alias("wind_speed"),  # Convert to double
            col("reading.air_humidity").alias("air_humidity"),  # Keep as integer
            col("reading.peak_wind_gust").alias("peak_wind_gust"),  # Already double
            col("reading.air_temperature").alias("air_temperature"),  # Already double
            col("reading.light_intensity").alias("light_intensity"),  # Keep as integer
            col("reading.rain_accumulation").alias("rain_accumulation"),  # Already double
            col("reading.barometric_pressure").alias("barometric_pressure"),  # Keep as integer
            col("reading.wind_direction_sensor").alias("wind_direction_sensor"),  # Keep as integer
            # Convert ISO timestamp string to proper timestamp
            to_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("processing_timestamp")
        )

        # Log schema for debugging
        logger.info("Final flattened DataFrame schema:")
        df_flat.printSchema()

        # Use foreachBatch for micro-batch processing
        query = (df_flat
                 .writeStream
                 .foreachBatch(process_batch)
                 .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/weather_pipeline")
                 .outputMode("append")
                 .start())

        logger.info("Streaming query started, waiting for termination")
        query.awaitTermination()

    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main()