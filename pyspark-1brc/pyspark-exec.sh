#!/usr/bin/env bash

docker cp -L pyspark-challenge.py pandas-polars-test-spark-master-1:/opt/bitnami/spark/pyspark-challenge.py

docker compose -f ../docker-compose-spark.yml exec spark-master spark-submit --master spark://172.21.0.2:7077 pyspark-challenge.py
