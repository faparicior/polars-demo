#!/usr/bin/env bash

docker compose run --rm python-brc -c "source /opt/venv/bin/activate && exec python /tmp/polars_demo.py"
