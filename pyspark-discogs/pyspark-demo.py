import time
from pyspark.sql import SparkSession
import os

def process_data(file_path, data_format):
    print(f"Process {data_format}...")
    start_time = time.time()
    if not os.path.exists(file_path):
        print(f"Error: File does not exist at {file_path}")
    else:
        try:
            if data_format == "CSV":
                data = spark.read.csv(file_path)
            else:
                data = spark.read.parquet(file_path)

            result = data.select('artist_name').distinct()

            # Show the result
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

process_data("/opt/bitnami/spark/data/discogs/discogs.csv", "CSV")

process_data("/opt/bitnami/spark/data/discogs/discogs.parquet", "Parquet")

# Stop the Spark session
spark.stop()
