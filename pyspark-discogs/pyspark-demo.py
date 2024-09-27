import time
from pyspark.sql import SparkSession
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Discogs Processing") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

start_time1 = time.time()

# File path
file_path = '/opt/bitnami/spark/data/discogs/discogs.csv'
file_path = '/opt/bitnami/spark/data/discogs/discogs.parquet'

# Check if file exists
if not os.path.exists(file_path):
    print(f"Error: File does not exist at {file_path}")
else:
    try:
        # Read the CSV file
        # data = spark.read.csv(file_path, header=True, inferSchema=True)
        data = spark.read.parquet(file_path)

        # Perform the groupby operation
        result = data.select('artist_name').distinct()

        # Show the result
        result.show()

        end_time1 = time.time()
        print(f"Time taken: {end_time1 - start_time1:.2f} seconds")

    except Exception as e:
        print(f"Error processing data: {str(e)}")

# Stop the Spark session
spark.stop()