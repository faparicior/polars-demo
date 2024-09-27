# Code credits: https://github.com/ifnesi/1brc#submitting
import time

import polars as pl

def process_data(file_path, data_format):
    print(f"Process {data_format}...")
    start_time = time.time()
    if data_format == "CSV":
        df = pl.scan_csv(file_path, low_memory=True, separator=";", has_header=False, with_column_names=lambda cols: ["station_name", "measurement"])
    else:
        df = pl.scan_parquet(file_path, low_memory=True, parallel='row_groups')

    df = df.group_by("station_name").agg(
        pl.min("measurement").alias("min_measurement"),
        pl.mean("measurement").alias("mean_measurement"),
        pl.max("measurement").alias("max_measurement")
    )

    df.show_graph(optimized=True, output_path="/tmp/data/1brc/graph.png")

    df.collect(streaming=True)
    df = (df.group_by("station_name").agg(
        pl.min("measurement").alias("min_measurement"),
        pl.mean("measurement").alias("mean_measurement"),
        pl.max("measurement").alias("max_measurement")
    ).collect(streaming=True))

    print(df)
    end_time = time.time()
    print(f"Time taken: {end_time - start_time:.2f} seconds")

# Process CSV
process_data("/tmp/data/1brc/measurements.csv", "CSV")

# Process Parquet
process_data("/tmp/data/1brc/measurements.parquet", "Parquet")
