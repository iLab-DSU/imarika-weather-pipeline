

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
from pyspark.sql.functions import col, from_json, to_json, struct, lit, current_timestamp, date_format, unix_timestamp, when
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
            StructField("uv_index", DoubleType(), True),
            StructField("rain_gauge", DoubleType(), True),
            StructField("wind_speed", DoubleType(), True),
            StructField("air_humidity", IntegerType(), True),
            StructField("peak_wind_gust", DoubleType(), True),
            StructField("air_temperature", DoubleType(), True),
            StructField("light_intensity", IntegerType(), True),
            StructField("rain_accumulation", DoubleType(), True),
            StructField("barometric_pressure", IntegerType(), True),
            StructField("wind_direction_sensor", IntegerType(), True)
        ]), True),
        # Add a catch-all field for future schema evolution
        StructField("created_at", TimestampType(), True),
        StructField("additional_fields", StringType(), True)

    ])

def define_outer_schema():
    return StructType([
        StructField("message", StringType()),
        StructField("success", BooleanType()),
        StructField("data", define_raw_schema())  # `define_raw_schema()` stays the same
    ])

def basic_data_cleaning(df):
    """Filter invalid readings and flatten nested JSON"""
    try:
        logger.info("Starting basic data cleaning")
        df_valid = df.filter(col("reading.valid") == True).cache()
        valid_count = df_valid.count()
        logger.info(f"Valid readings: {valid_count} out of {df.count()}")
        
        df_flat = df_valid.select(
            col("reading_id"), col("device_id"),
            col("reading.uv_index").alias("uv_index"),
            col("reading.rain_gauge").alias("rain_gauge"),
            col("reading.wind_speed").alias("wind_speed"),
            col("reading.air_humidity").alias("air_humidity"),
            col("reading.peak_wind_gust").alias("peak_wind_gust"),
            col("reading.air_temperature").alias("air_temperature"),
            col("reading.light_intensity").alias("light_intensity"),
            col("reading.rain_accumulation").alias("rain_accumulation"),
            col("reading.barometric_pressure").alias("barometric_pressure"),
            col("reading.wind_direction_sensor").alias("wind_direction_sensor"),
            current_timestamp().alias("processing_timestamp")
        )
        
        # Clean invalid numeric ranges
        cleaned_df = df_flat.withColumn(
            "air_temperature", when(col("air_temperature").between(-50,60), col("air_temperature")).otherwise(None)
        ).withColumn(
            "air_humidity",    when(col("air_humidity").between(0,100), col("air_humidity")).otherwise(None)
        ).withColumn(
            "wind_speed",      when(col("wind_speed") >= 0, col("wind_speed")).otherwise(None)
        ).withColumn(
            "uv_index",        when(col("uv_index").between(0,12), col("uv_index")).otherwise(None)
        ).withColumn(
            "barometric_pressure",
            when(col("barometric_pressure").between(87000,108000), col("barometric_pressure")).otherwise(None)
        ).withColumn(
            "date",
            date_format(col("processing_timestamp"), "yyyy-MM-dd")
        )
        
        # Log cleaning statistics
        null_counts = {col_name: cleaned_df.filter(cleaned_df[col_name].isNull()).count() 
                      for col_name in ["air_temperature", "air_humidity", "wind_speed", "uv_index"]}
        logger.info(f"Null counts after cleaning: {null_counts}")
        
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



def detect_anomalies(df):
    """Unsupervised anomaly detection via KMeans distance"""
    try:
        logger.info("Starting anomaly detection")
        feature_cols = ["air_temperature","air_humidity","wind_speed","uv_index","barometric_pressure"]

        # Check if there is any usable data
        non_null_df = df.select(feature_cols).dropna(how="all")
        if non_null_df.count() == 0:
            logger.warning("All feature values are null — skipping anomaly detection")
            return df.withColumn("anomaly_score", lit(None)).withColumn("is_anomaly", lit(False))

        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="keep")
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)
        kmeans = KMeans(k=2, featuresCol="scaled_features", predictionCol="cluster")

        df_feat = assembler.transform(df)
        df_scaled = scaler.fit(df_feat).transform(df_feat)
        df_clust = kmeans.fit(df_scaled).transform(df_scaled)

        df_score = df_clust.withColumn(
            "anomaly_score", F.sqrt(F.expr("aggregate(scaled_features, 0D, (acc, x) -> acc + x*x)"))
        )

        result_df = df_score.withColumn("is_anomaly", col("anomaly_score") > lit(3.0)) \
                            .drop("features", "scaled_features", "cluster")

        # Log stats
        anomaly_count = result_df.filter(result_df["is_anomaly"]).count()
        total_count = result_df.count()
        logger.info(f"Detected {anomaly_count} anomalies out of {total_count} records")

        return result_df

    except Exception as e:
        logger.error(f"Error in detect_anomalies: {str(e)}")
        logger.error(traceback.format_exc())
        return df



def aggregate_daily_data(df):
    """Aggregate cleaned & imputed data to daily metrics"""
    try:
        logger.info("Starting daily data aggregation")
        df_daily = df.groupBy("device_id","date").agg(
            F.max("air_temperature").alias("maxtemp_c"),
            F.min("air_temperature").alias("mintemp_c"),
            F.avg("air_temperature").alias("avgtemp_c"),
            F.max(col("wind_speed")*3.6).alias("maxwind_kph"),  # Convert m/s to km/h
            F.sum("rain_gauge").alias("totalprecip_mm"),
            F.avg("air_humidity").cast(IntegerType()).alias("avghumidity"),
            when(F.sum("rain_gauge")>0, lit(1)).otherwise(lit(0)).alias("daily_will_it_rain"),
            when((F.avg("air_humidity")>70)&(F.avg("barometric_pressure")<100000), lit(70)).otherwise(lit(0)).alias("daily_chance_of_rain"),
            F.max("processing_timestamp").alias("processing_timestamp"),
            unix_timestamp(col("date"),"yyyy-MM-dd").cast(IntegerType()).alias("date_epoch"),
            F.max("anomaly_score").alias("anomaly_score"),
            F.max(when(col("is_anomaly") == True, 1).otherwise(0)).cast("boolean").alias("is_anomaly")

        )
        
        # # Condition and nested struct
        # df_cond = df_daily.withColumn(
        #     "condition_text",
        #     when(col("daily_will_it_rain")==1,"Rainy")
        #     .when(col("avghumidity")>80,"Cloudy")
        #     .when(col("maxtemp_c")>30,"Hot")
        #     .when(col("maxtemp_c")<15,"Cold")
        #     .otherwise("Sunny")
        # )
        
        # df_cond = df_cond.withColumn(
        #     "day",
        #     struct(
        #         col("maxtemp_c"),col("mintemp_c"),col("avgtemp_c"),col("maxwind_kph"),
        #         col("totalprecip_mm"),col("avghumidity"),col("daily_will_it_rain"),col("daily_chance_of_rain"),
        #         struct(
        #             col("condition_text").alias("text"),
        #             lit("//cdn.weatherapi.com/weather/64x64/day/113.png").alias("icon"),
        #             lit(1000).alias("code")
        #         ).alias("condition")
        #     )
        # )
        
        # # result_df = df_cond.select("device_id","date","date_epoch","day","is_anomaly","anomaly_score","processing_timestamp")
        # result_df = df_cond.select("device_id", "date", "date_epoch",to_json(col("day")).alias("day"),  
        #                            "is_anomaly", "anomaly_score", "processing_timestamp")
        # Log aggregation statistics
        # logger.info(f"Aggregated {df.count()} records into {result_df.count()} daily summaries")
        
        # return result_df
        return df_daily

    except Exception as e:
        logger.error(f"Error in aggregate_daily_data: {str(e)}")
        logger.error(traceback.format_exc())
        raise


# def validate_data_quality(df, table_name):
#     """Validate data quality before writing to database"""
#     try:
#         logger.info(f"Validating data quality for {table_name}")
        
#         # Check if DataFrame is empty
#         if df.isEmpty():
#             logger.warning(f"DataFrame for {table_name} is empty")
#             return False
            
#         # Check for nulls in critical columns
#         if table_name == POSTGRES_TABLE_CLEAN:
#             # null_counts = {col: df.filter(df[col].isNull()).count() for col in ["device_id", "date", "day"]}
#             null_counts = {"device_id": df.filter(df["device_id"].isNull()).count(),
#                            "date": df.filter(df["date"].isNull()).count(),
#                            "day": df.filter(df["day"].isNull() | (df["day"] == "")).count()  # also check empty string
#                            }
#             logger.info(f"Null counts in critical columns: {null_counts}")
            
#             if any(count > 0 for count in null_counts.values()):
#                 logger.warning(f"Critical columns contain NULL values: {null_counts}")
#                 return False
            
#             # Check for anomalies in aggregated values
#             stats = df.select(
#                 F.min("day.mintemp_c").alias("min_temp"),
#                 F.max("day.maxtemp_c").alias("max_temp"),
#                 F.avg("day.avghumidity").alias("avg_humidity"),
#                 F.max("anomaly_score").alias("anomaly_score"),
#                 F.max(when(col("is_anomaly") == True, 1).otherwise(0)).cast("boolean").alias("is_anomaly")
#             ).collect()[0]
            
#             logger.info(f"Data stats: min_temp={stats['min_temp']}, max_temp={stats['max_temp']}, avg_humidity={stats['avg_humidity']}")
            
#             # Return True if data passes quality checks
#             quality_check = all([
#                 stats['min_temp'] is not None,
#                 stats['max_temp'] is not None,
#                 stats['min_temp'] <= stats['max_temp'],
#                 stats['avg_humidity'] is not None and 0 <= stats['avg_humidity'] <= 100
#             ])
            
#             if not quality_check:
#                 logger.warning("Data quality check failed for aggregated values")
#                 return False
                
#         return True  # For raw table or if all checks pass
#     except Exception as e:
#         logger.error(f"Error in validate_data_quality: {str(e)}")
#         logger.error(traceback.format_exc())
#         return False
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
                "timestamp": df.filter(df["timestamp"].isNull()).count()
            }
            logger.info(f"Null counts in critical columns: {null_counts}")
            
            if any(count > 0 for count in null_counts.values()):
                logger.warning(f"Critical columns contain NULL values: {null_counts}")
                return False
            
            # Check for reasonable temperature ranges
            stats = df.select(
                F.min("min_temp").alias("min_temp"),
                F.max("max_temp").alias("max_temp"),
                F.avg("avg_humidity").alias("avg_humidity")
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

# def write_batch_to_postgres(batch_df, table, batch_id):
#     """Write batch data to PostgreSQL with error handling"""
#     try:
#         logger.info(f"Writing batch {batch_id} to {table}")
        
#         # Check if DataFrame is empty
#         if batch_df.isEmpty():
#             logger.warning(f"Batch {batch_id} is empty, skipping write to {table}")
#             return False
            
#         # Validate data quality
#         if not validate_data_quality(batch_df, table):
#             logger.warning(f"Data quality validation failed for batch {batch_id}, skipping write to {table}")
#             return False
            
#         # Prepare data for raw table
#         if table == POSTGRES_TABLE_RAW:
#             batch_df = batch_df.withColumn("data", to_json(struct("*"))).select("data")
        
#         # Log schema for debugging
#         logger.info(f"Schema for table {table}: {batch_df.schema.simpleString()}")
        
#         # Write to PostgreSQL
#         (batch_df.write
#              .format("jdbc")
#              .option("url", POSTGRES_URL)
#              .option("dbtable", table)
#              .option("user", POSTGRES_USER)
#              .option("password", POSTGRES_PASSWORD)
#              .option("driver", POSTGRES_DRIVER)
#              .mode("append")
#              .save())
        
#         logger.info(f"Successfully wrote batch {batch_id} to {table}")
#         return True
#     except Exception as e:
#         logger.error(f"Failed to write batch {batch_id} to {table}: {str(e)}")
#         logger.error(traceback.format_exc())
#         return False
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
            flagged = detect_anomalies(imputed)
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
        raw_schema = define_raw_schema()
        
        # Create streaming DataFrame from Kafka
        df_kafka_raw = (spark.readStream
                        .format("kafka")
                        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                        .option("subscribe", KAFKA_TOPIC_RAW)
                        .option("startingOffsets", "earliest")  # Changed from "latest" to "earliest"
                        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)  # Control batch size
                        .load())
                        
        # Parse JSON data
        # df_parsed = df_kafka_raw.select(from_json(col("value").cast("string"), raw_schema).alias("data")).select("data.*")
        outer_schema = define_outer_schema()

        df_parsed = df_kafka_raw.select(
            from_json(col("value").cast("string"), outer_schema).alias("parsed")
        ).select("parsed.data.*")
        df_parsed.writeStream \
            .format("console") \
            .option("truncate", False) \
            .outputMode("append") \
            .start()

        df_flat = df_parsed.select(
            "reading_id",
            "device_id",
            col("reading.valid").alias("valid"),
            col("reading.uv_index").alias("uv_index"),
            col("reading.rain_gauge").alias("rain_gauge"),
            col("reading.wind_speed").alias("wind_speed"),
            col("reading.air_humidity").alias("air_humidity"),
            col("reading.peak_wind_gust").alias("peak_wind_gust"),
            col("reading.air_temperature").alias("air_temperature"),
            col("reading.light_intensity").alias("light_intensity"),
            col("reading.rain_accumulation").alias("rain_accumulation"),
            col("reading.barometric_pressure").alias("barometric_pressure"),
            col("reading.wind_direction_sensor").alias("wind_direction_sensor"),
            col("created_at").alias("processing_timestamp")
        )


        
        # Use foreachBatch for micro-batch processing with proper checkpointing
        query = (df_parsed
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
