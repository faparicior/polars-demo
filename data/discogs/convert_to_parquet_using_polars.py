import polars as pl

def process_csv_to_parquet(csv_filename, parquet_filename):
    """
    Reads a CSV file in chunks and writes the data to a single Parquet file.

    Args:
        csv_filename (str): Path to the CSV file.
        parquet_filename (str): Path to the output Parquet file.
    """

    lf = pl.scan_csv(csv_filename, separator=",")
    lf.sink_parquet(parquet_filename)

csv_file = "/tmp/data/discogs/discogs.csv"
parquet_file = "/tmp/data/discogs/discogs.parquet"
process_csv_to_parquet(csv_file, parquet_file)
