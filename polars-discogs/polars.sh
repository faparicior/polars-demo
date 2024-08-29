#!/usr/bin/env bash

docker compose -f ../docker-compose.yml run --rm python-brc -c "source /opt/venv/bin/activate && exec python /tmp/polars/polars_demo.py"
