from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time

import os

def process_data(file_path, data_format):
    print(f"Process {data_format}...")
    start_time = time.time()
    if not os.path.exists(file_path):
        print(f"Error: File does not exist at {file_path}")
    else:
        try:
            if data_format == "CSV":
                schema = "station_name STRING, measurement DOUBLE"
                data = spark.read.option("header", "false").schema(schema).csv(file_path)
            else:
                data = spark.read.parquet(file_path)

            result = data.groupBy("station_name").agg(
                F.min("measurement").alias("min_measurement"),
                F.mean("measurement").alias("mean_measurement"),
                F.max("measurement").alias("max_measurement")
            )

            # Collect and show results
            result.show()

            end_time = time.time()
            print(f"Time taken: {end_time - start_time:.2f} seconds")

        except Exception as e:
            print(f"Error processing data: {str(e)}")

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Discogs Processing") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# process_data("/opt/bitnami/spark/data/1brc/measurements.csv", "CSV")

process_data("/opt/bitnami/spark/data/1brc/measurements.parquet", "Parquet")

# Stop the Spark session
spark.stop()
