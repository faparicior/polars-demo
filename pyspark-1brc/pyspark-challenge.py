from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time

# Initialize Spark session
spark = (
    SparkSession.builder
    .appName("1brc")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

print("Process Parquet...")
start_time1 = time.time()

# Read Parquet file
df = spark.read.parquet("/opt/bitnami/spark/data/1brc/measurements.parquet")

# Perform grouping and aggregation
result_df = df.groupBy("station_name").agg(
    F.min("measurement").alias("min_measurement"),
    F.mean("measurement").alias("mean_measurement"),
    F.max("measurement").alias("max_measurement")
)

# Collect and show results
result_df.show()

end_time1 = time.time()
print(f"Time taken: {end_time1 - start_time1:.2f} seconds")

print("Process CSV...")
start_time1 = time.time()

# Read CSV file
df = spark.read.csv("/opt/bitnami/spark/data/1brc/measurements.csv", sep=";", header=False, schema="station_name STRING, measurement DOUBLE")

# Perform grouping and aggregation
result_df = df.groupBy("station_name").agg(
    F.min("measurement").alias("min_measurement"),
    F.mean("measurement").alias("mean_measurement"),
    F.max("measurement").alias("max_measurement")
)

# Collect and show results
result_df.show()

end_time1 = time.time()
print(f"Time taken: {end_time1 - start_time1:.2f} seconds")

# Stop Spark session
spark.stop()
