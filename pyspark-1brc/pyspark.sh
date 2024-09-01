#!/usr/bin/env bash

docker cp -L pyspark-challenge.py pandas-polars-test-spark-master-1:/opt/bitnami/spark/pyspark-challenge.py

docker logs pandas-polars-test-spark-master-1
