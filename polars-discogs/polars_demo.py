import time
import polars as pl

def process_data(file_path, data_format):
    print(f"Process {data_format}...")
    start_time = time.time()
    if data_format == "CSV":
        data = pl.scan_csv(file_path, low_memory=True, rechunk=False)
    else:
        data = pl.scan_parquet(file_path, low_memory=True, parallel='row_groups')

    result = data.group_by('artist_name').len().select('artist_name')
    print(result.collect(streaming=True))

    end_time = time.time()
    print(f"Time taken: {end_time - start_time:.2f} seconds")

# Process CSV
process_data("/tmp/data/discogs/discogs.csv", "CSV")

# Process Parquet
process_data("/tmp/data/discogs/discogs.parquet", "Parquet")
