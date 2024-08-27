import time
import pandas as pd
import psutil

start_time1 = time.time()

data = pd.read_csv('/tmp/data/discogs.csv', low_memory=True, chunksize=10000)
result = pd.concat([df.groupby('artist_name').size() for df in data])

print(result)

end_time1 = time.time()
print(f"Time taken: {end_time1 - start_time1:.2f} seconds")

# Getting % usage of virtual_memory ( 3rd field)
print('RAM memory % used:', psutil.virtual_memory()[2])
# Getting usage of virtual_memory in GB ( 4th field)
print('RAM Used (GB):', psutil.virtual_memory()[3]/1000000000)
