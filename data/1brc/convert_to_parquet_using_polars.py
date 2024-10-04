import polars as pl

def process_csv_to_parquet(csv_filename, parquet_filename):
    """
    Reads a CSV file in chunks and writes the data to a single Parquet file.

    Args:
        csv_filename (str): Path to the CSV file.
        parquet_filename (str): Path to the output Parquet file.
    """

    lf = pl.scan_csv(csv_filename, separator=";", has_header=False,
                             with_column_names=lambda cols: ["station_name", "measurement"])
    lf.sink_parquet(parquet_filename)

csv_file = "/tmp/data/1brc/measurements.csv"
parquet_file = "/tmp/data/1brc/measurements.parquet"
process_csv_to_parquet(csv_file, parquet_file)
