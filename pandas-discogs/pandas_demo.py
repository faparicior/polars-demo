import time
import pandas as pd
import psutil
from collections import Counter


def process_csv_in_chunks(file_path, chunk_size=10000):
    artist_counter = Counter()

    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        artist_counts = chunk['artist_name'].value_counts()
        artist_counter.update(artist_counts.to_dict())

    return pd.DataFrame.from_dict(artist_counter, orient='index', columns=['count']).reset_index().rename(
        columns={'index': 'artist_name'}).sort_values('count', ascending=False)


start_time = time.time()

# Process the CSV file
result = process_csv_in_chunks('/tmp/data/discogs/discogs.csv')

print(result)

end_time = time.time()
print(f"Time taken: {end_time - start_time:.2f} seconds")
