import time
import polars as pl

def process_csv_in_chunks(file_path, batch_size=10000):
    # Initialize a dictionary to count artist occurrences
    unique_artists = set()

    # Read the CSV file in batches
    batched_reader = pl.read_csv_batched(file_path, low_memory=True, batch_size=batch_size)

    batches = batched_reader.next_batches(2)
    # Iterate over the batches
    while batches:
        for batch in batches:
            batch_unique_artists = batch.select("artist_name").unique().to_series().to_list()
            unique_artists.update(batch_unique_artists)
        batches = batched_reader.next_batches(2)

    return pl.DataFrame({
        'artist_name': list(unique_artists)
    })

start_time = time.time()

# Process the CSV file
result = process_csv_in_chunks('/tmp/data/discogs/discogs.csv')

print(result)

end_time = time.time()
print(f"Time taken: {end_time - start_time:.2f} seconds")