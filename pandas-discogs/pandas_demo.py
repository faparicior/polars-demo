import time
import pandas as pd

def process_csv(file_path):
    df = pd.read_csv(file_path, low_memory=True)

    df.groupby('artist_name')
    print(df)

start_time = time.time()

# Process the CSV file
result = process_csv('/tmp/data/discogs/discogs.csv')

print(result)

end_time = time.time()
print(f"Time taken: {end_time - start_time:.2f} seconds")
