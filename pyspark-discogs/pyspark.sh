#!/usr/bin/env bash

docker cp -L pyspark-demo.py pandas-polars-test-spark-master-1:/opt/bitnami/spark/pyspark-demo.py

docker logs pandas-polars-test-spark-master-1
