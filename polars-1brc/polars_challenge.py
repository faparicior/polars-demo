# Code credits: https://github.com/ifnesi/1brc#submitting
import time

import polars as pl

print("Process CSV...")
start_time1 = time.time()
df = (
    pl.scan_parquet("/tmp/data/1brc/measurements.parquet", low_memory=True, parallel='row_groups')
        .group_by("station_name")
        .agg(
            pl.min("measurement").alias("min_measurement"),
            pl.mean("measurement").alias("mean_measurement"),
            pl.max("measurement").alias("max_measurement")
        )
        .collect(streaming=True)
)
print(df)

end_time1 = time.time()
print(f"Time taken: {end_time1 - start_time1:.2f} seconds")

print("Process Parquet...")
start_time1 = time.time()
df = (
    pl.scan_csv("/tmp/data/1brc/measurements.csv", separator=";", has_header=False, with_column_names=lambda cols: ["station_name", "measurement"])
        .group_by("station_name")
        .agg(
            pl.min("measurement").alias("min_measurement"),
            pl.mean("measurement").alias("mean_measurement"),
            pl.max("measurement").alias("max_measurement")
        )
        .collect(streaming=True)
)
print(df)

end_time1 = time.time()
print(f"Time taken: {end_time1 - start_time1:.2f} seconds")
