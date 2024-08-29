import time
import polars as pl

print("Process CSV...")

start_time1 = time.time()
data = pl.scan_csv('/tmp/data/discogs.csv', low_memory=True, rechunk=False)
result = data.group_by('artist_name').len().select('artist_name')
print(result.collect(streaming=True))

end_time1 = time.time()
print(f"Time taken: {end_time1 - start_time1:.2f} seconds")

print("Process Parquet...")

start_time1 = time.time()
data = pl.scan_parquet('/tmp/data/discogs.parquet')
result = data.group_by('artist_name').len().select('artist_name')
print(result.collect(streaming=True))

end_time1 = time.time()
print(f"Time taken: {end_time1 - start_time1:.2f} seconds")
