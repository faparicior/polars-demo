import time
import pandas as pd
import psutil

def process_data(file_path, data_format):
    print(f"Process {data_format}...")
    start_time = time.time()
    if data_format == "CSV":
        data = pd.read_csv('/tmp/data/discogs/discogs.csv', low_memory=True, chunksize=10000)
    else:
        data = pd.read_parquet('/tmp/data/discogs/discogs.parquet',)

    result = pd.concat([df.groupby('artist_name').size() for df in data])
    print(result)

    end_time = time.time()
    print(f"Time taken: {end_time - start_time:.2f} seconds")

# Process CSV
process_data("/tmp/data/discogs/discogs.csv", "CSV")

# Process Parquet
process_data("/tmp/data/discogs/discogs.parquet", "Parquet")
