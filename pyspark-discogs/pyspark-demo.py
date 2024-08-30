import time
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Discogs Processing") \
    .getOrCreate()

start_time1 = time.time()

# Read the CSV file
data = spark.read.csv('/tmp/data/discogs/discogs.csv', header=True, inferSchema=True)

# Perform the groupby operation
result = data.groupBy('artist_name').count()

# Show the result
result.show()

end_time1 = time.time()
print(f"Time taken: {end_time1 - start_time1:.2f} seconds")

# Stop the Spark session
spark.stop()