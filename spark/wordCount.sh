#!/usr/bin/env bash
set -v

echo $1

# Stop
docker stop pytap

# Remove previuos container 
docker container rm pytap

docker build . -f Dockerfile-WordCount --build-arg TXT=$1 --tag alawys:spark
docker run -e SPARK_ACTION=wordcount -e TAP_CODE="$1" -v alavolume:/alavolume  -p 4040:4040 --name pytap -it alawys:spark 
