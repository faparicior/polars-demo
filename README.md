# Polars performance test

## Data sources in CSV format

- Discogs: Place the data from [this source](https://www.kaggle.com/datasets/ofurkancoban/discogs-releases-dataset?select=discogs.csv) to the `data/discogs` folder.
- 1BRC: Generate the data source executing 'create_measurements.sh' command in the `data/1brc` folder.

### Data sources in Parquet format

- Discogs: Execute the command convert_to_parquest_using_polars.sh in the `data/discogs` folder.
Place the data in the `data` folder.

- 1BRC: Execute the command convert_to_parquest_using_polars.sh in the `data/1brc` folder.
Place the data in the `data` folder.

## Performance test

Each folder test one datasource with one technology.

### Testing Polars
Polars has two bash scripts to run the tests:

- 6Gb RAM, 8 threads: high_resources.sh
- 300Mb RAM, 2 threads: low_resources.sh

### Using Pyspark

Pyspark establishes the baseline for the performance tests.
To run the tests, execute the following commands in the correct folder:

```bash
pyspark-up.sh
pyspark-exec.sh
```
