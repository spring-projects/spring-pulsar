#!/bin/bash

docker run -it -p 6650:6650 -p 8080:8080 \
  --mount source=pulsardata-arm64,target=/pulsar/data \
  --mount source=pulsarconf-arm64,target=/pulsar/conf \
  kezhenxu94/pulsar:latest \
  bin/pulsar standalone
