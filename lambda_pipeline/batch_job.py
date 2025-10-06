"""
Batch processing job for the data pipeline.
"""
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, avg, count

def create_spark_session():
    """Create and return a Spark session with Hive support."""
    return SparkSession.builder \
        .appName("BatchProcessing") \
        .enableHiveSupport() \
        .getOrCreate()

def process_batch_data(spark, input_path, output_path):
    """Process batch data and save results."""
    # Read input data
    df = spark.read.parquet(input_path)
    
    # Perform batch processing (example: daily aggregations)
    result = df \
        .withColumn("date", to_date(col("timestamp"))) \
        .groupBy("date", "category") \
        .agg(
            avg("value1").alias("avg_value1"),
            avg("value2").alias("avg_value2"),
            count("*").alias("record_count")
        )
    
    # Save results
    result.write.mode("overwrite").parquet(output_path)
    print(f"Batch processing completed. Results saved to {output_path}")
    return result

if __name__ == "__main__":
    # Configure paths
    input_dir = os.getenv("BATCH_INPUT_DIR", "data/batch_input")
    output_dir = os.getenv("BATCH_OUTPUT_DIR", "serving/batch")
    
    # Process data
    spark = create_spark_session()
    process_batch_data(spark, input_dir, output_dir)
    spark.stop()
