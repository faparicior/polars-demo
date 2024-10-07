#!/usr/bin/env bash

docker compose -f ../docker-compose.yml run --rm python-brc -c "source /opt/venv/bin/activate && exec python /tmp/polars-1brc/polars_challenge.py"
