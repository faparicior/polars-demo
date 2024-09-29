import time
import polars as pl

def process_csv_in_chunks(file_path, chunk_size=10000):
    # Use scan_csv for lazy evaluation
    data = pl.scan_csv(file_path, low_memory=True, rechunk=False)

    # Collect the result in a streaming fashion
    result = data.collect(streaming=True)

    # Process the data in chunks
    artist_counter = {}
    for chunk in result.iter_slices(chunk_size):
        chunk_df = chunk.to_pandas()
        artist_counts = chunk_df['artist_name'].value_counts()
        for artist, count in artist_counts.items():
            if artist in artist_counter:
                artist_counter[artist] += count
            else:
                artist_counter[artist] = count

    return pl.DataFrame({
        'artist_name': list(artist_counter.keys()),
        'count': list(artist_counter.values())
    }).sort('count', reverse=True)

start_time = time.time()

# Process the CSV file
result = process_csv_in_chunks('/tmp/data/discogs/discogs.csv')

print(result)

end_time = time.time()
print(f"Time taken: {end_time - start_time:.2f} seconds")