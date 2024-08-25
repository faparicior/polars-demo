#!/bin/sh

cp ../requirements.txt requirements.txt
#docker build --no-cache -t python-brc .
docker build -t python-brc .
