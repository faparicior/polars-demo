#!/usr/bin/env bash

docker compose -f ../../docker-compose.yml run --rm python-brc -c "source /opt/venv/bin/activate && exec python /tmp/data/discogs/convert_to_parquet_using_polars.py"
