#!/usr/bin/env bash

docker compose -f ../../docker-compose.yml run --rm python-brc -c "source /opt/venv/bin/activate && exec python /tmp/data/1brc/create_measurements.py 1_000_000_000"
