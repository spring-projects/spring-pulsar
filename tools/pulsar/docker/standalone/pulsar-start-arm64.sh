#!/bin/bash

docker run -it -p 6650:6650 -p 8080:8080 \
  --mount source=pulsardata,target=/pulsar/data \
  --mount source=pulsarconf,target=/pulsar/conf \
  kezhenxu94/pulsar:latest \
  bin/pulsar standalone
