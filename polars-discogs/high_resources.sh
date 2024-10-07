#!/usr/bin/env bash

docker compose -f ../docker-compose.yml run --rm --remove-orphans python-brc -c "source /opt/venv/bin/activate && exec python /tmp/polars-discogs/polars_demo.py"
