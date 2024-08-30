#!/usr/bin/env bash

docker compose -f ../docker-compose-spark.yml exec spark-master spark-submit --master spark://172.21.0.2:7077 pyspark-demo.py
