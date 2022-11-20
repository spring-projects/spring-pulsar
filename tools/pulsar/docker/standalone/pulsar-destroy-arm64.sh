#!/bin/bash

echo "Finding and removing Pulsar container..."
docker ps -a | grep "kezhenxu94/pulsar" | awk '{print $1}' | xargs docker rm -f
echo "Removing Pulsar volumes..."
docker volume rm pulsardata-arm64 pulsarconf-arm64
echo "All traces of Pulsar standalone removed"
