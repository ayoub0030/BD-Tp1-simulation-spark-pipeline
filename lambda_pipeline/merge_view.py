"""
Merge batch and speed layer results to create a unified view.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp

def create_spark_session():
    """Create and return a Spark session."""
    return SparkSession.builder \
        .appName("MergeView") \
        .enableHiveSupport() \
        .getOrCreate()

def merge_views(spark, batch_path, speed_path, output_path):
    """Merge batch and speed layer results."""
    # Read batch and speed data
    batch_df = spark.read.parquet(batch_path)
    speed_df = spark.read.parquet(speed_path)  # Assuming speed layer writes to parquet
    
    # Add processing type for lineage
    batch_df = batch_df.withColumn("processing_type", lit("batch"))
    speed_df = speed_df.withColumn("processing_type", lit("speed"))
    
    # Union the datasets (in a real scenario, you might do more sophisticated merging)
    merged_df = batch_df.unionByName(speed_df, allowMissingColumns=True)
    
    # Add metadata
    merged_df = merged_df.withColumn("processed_at", current_timestamp())
    
    # Save the merged view
    merged_df.write.mode("overwrite").parquet(output_path)
    print(f"Merged view saved to {output_path}")
    
    return merged_df

if __name__ == "__main__":
    # Configure paths
    batch_path = os.getenv("BATCH_OUTPUT_DIR", "serving/batch")
    speed_path = os.getenv("SPEED_OUTPUT_DIR", "serving/speed")
    output_path = os.getenv("MERGED_OUTPUT_DIR", "serving/view/merged")
    
    # Process and merge
    spark = create_spark_session()
    merge_views(spark, batch_path, speed_path, output_path)
    spark.stop()
