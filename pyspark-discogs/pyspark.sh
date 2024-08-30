#!/usr/bin/env bash

docker cp -L pyspark_challenge.py 1brc-spark-master-1:/opt/bitnami/spark/pyspark_challenge.py

docker logs 1brc-spark-master-1
