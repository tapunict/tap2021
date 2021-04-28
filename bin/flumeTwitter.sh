#!/usr/bin/env bash

#Build
docker build ../flume/ --tag tap:flume

# Run
docker run --network tap --ip 10.0.100.10  -it -e FLUME_CONF_FILE=twitter.conf -e MAX_BYTES_TO_LOG=10000 tap:flume