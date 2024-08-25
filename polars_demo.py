import time
import polars as pl

start_time1 = time.time()

# data = pl.scan_csv('/tmp/data/discogs.csv')
data = pl.scan_csv('./data/discogs.csv')
result = data.group_by('artist_name').len()

# print(result.collect(streaming=True))
print(result.collect())

end_time1 = time.time()
print(f"Time taken: {end_time1 - start_time1:.2f} seconds")
