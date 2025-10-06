"""
Speed layer processing for real-time stream data.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

def create_spark_streaming_session():
    """Create and return a Spark session with streaming support."""
    return SparkSession.builder \
        .appName("SpeedLayerProcessing") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

def define_schema():
    """Define the schema for incoming stream data."""
    return StructType([
        StructField("timestamp", StringType()),
        StructField("sensor_id", StringType()),
        StructField("value", DoubleType()),
        StructField("status", StringType())
    ])

def process_stream(spark, kafka_bootstrap_servers, kafka_topic, checkpoint_dir):
    """Process the streaming data from Kafka."""
    # Read from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON data
    schema = define_schema()
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Process stream with windowing (5-minute windows)
    windowed_counts = parsed_df \
        .withColumn("processing_time", current_timestamp()) \
        .withWatermark("processing_time", "10 minutes") \
        .groupBy(
            window("processing_time", "5 minutes"),
            "sensor_id"
        ) \
        .agg({
            "value": "avg",
            "sensor_id": "count"
        }) \
        .withColumnRenamed("avg(value)", "avg_value") \
        .withColumnRenamed("count(sensor_id)", "message_count")
    
    # Output to console (in production, you'd write to a database or another Kafka topic)
    query = windowed_counts \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("checkpointLocation", checkpoint_dir) \
        .start()
    
    return query

if __name__ == "__main__":
    # Configuration
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "stream_events")
    checkpoint_dir = os.getenv("CHECKPOINT_DIR", "/tmp/checkpoints/speed_layer")
    
    # Initialize Spark
    spark = create_spark_streaming_session()
    
    # Start processing
    print("Starting stream processing...")
    query = process_stream(spark, kafka_bootstrap_servers, kafka_topic, checkpoint_dir)
    
    # Keep the application running
    query.awaitTermination()
