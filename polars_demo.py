import time
import polars as pl
import psutil

start_time1 = time.time()

data = pl.scan_csv('/tmp/data/discogs.csv', low_memory=True, rechunk=False)
# data = pl.scan_csv('./data/discogs.csv')
result = data.group_by('artist_name').len().select('artist_name')

print(result.collect(streaming=True))
#
# for batch in result.collect(streaming=True):
#     print(batch)

# print(result.collect())

end_time1 = time.time()
print(f"Time taken: {end_time1 - start_time1:.2f} seconds")

# Getting % usage of virtual_memory ( 3rd field)
print('RAM memory % used:', psutil.virtual_memory()[2])
# Getting usage of virtual_memory in GB ( 4th field)
print('RAM Used (GB):', psutil.virtual_memory()[3]/1000000000)